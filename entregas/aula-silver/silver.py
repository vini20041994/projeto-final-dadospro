"""Job Silver: bronze (Parquet) -> silver (eventos limpos, itens e rejeitados).

    docker compose run --rm app python pipeline/silver.py

O que este job faz (requisitos R5 a R10 do enunciado):
- deduplica por event_id (R5): no reenvio de lote o mesmo evento chega em dois arquivos; fica a
  cópia ingerida primeiro;
- extrai os params para colunas tipadas (R6), inclusive element_id;
- valida com as seis regras do enunciado (R7): o que falha vai para silver/eventos_rejeitados com
  motivo_rejeicao, um motivo por linha (o primeiro que o evento quebra, na ordem da tabela do R7);
- sessioniza por user_pseudo_id, abrindo sessão nova depois de mais de 30 minutos sem evento (R8),
  sem olhar o session_hint;
- costura a identidade (R9): todo evento de um user_pseudo_id recebe o user_id que esse navegador
  teve em algum evento válido, inclusive os eventos de antes do login;
- explode items em silver/itens, uma linha por item por evento (R10).

Diferente da Bronze, a Silver é recalculada inteira a partir da Bronze a cada execução: rodar duas
vezes dá o mesmo resultado. Os eventos são gravados com overwrite dinâmico (troca só as partições
event_name=… presentes, sem apagar itens/ e eventos_rejeitados/, que são vizinhas na raiz).

As partições year/month/day da Silver são a data do evento em UTC (5.4, item 10), e não o dia de
coleta da Bronze: assim "as compras de 3 de setembro" são as que aconteceram em 3 de setembro,
mesmo que a sessão tenha atravessado a meia-noite e o lote tenha sido coletado no dia seguinte.
"""

from pyspark.sql import Window
from pyspark.sql import functions as F

from comum import caminho, criar_sessao, exigir_camada, glob_eventos, gravar_eventos, ler_eventos, titulo

# Os eventos do contrato (docs/CONTRATO_EVENTO.md, "Os 20 eventos"; video_*, add/remove_from_cart e
# sign_up/login aparecem agrupados lá, por isso a lista tem mais de 20 nomes).
CATALOGO = [
    "page_view", "scroll", "click", "site_search", "video_start", "video_progress", "video_complete",
    "file_download", "view_item_list", "select_item", "view_item", "add_to_cart", "remove_from_cart",
    "view_cart", "begin_checkout", "add_shipping_info", "add_payment_info", "purchase", "refund",
    "generate_lead", "sign_up", "login",
]
MARCOS_SCROLL = [25, 50, 75, 90]
MARCOS_VIDEO = [0, 10, 25, 50, 75, 100]
PAUSA_DA_SESSAO_US = 30 * 60 * 1_000_000          # 30 minutos, em microssegundos (unix_micros)
FORMATO_TS = "yyyy-MM-dd'T'HH:mm:ss.SSSX"         # 2026-10-03T14:22:05.123Z, conferido nos dados

# Chave de params -> tipo da coluna na Silver (R6). Tudo chega como texto do map da Bronze e é
# convertido com try_cast: no modo ANSI do Spark 4 um cast impossível pararia o job; com try_cast o
# valor vira null e a validação decide o que fazer com ele.
PARAMS = {
    "page_title": "string",
    "percent_scrolled": "int",
    "link_url": "string",
    "link_domain": "string",
    "link_text": "string",
    "link_id": "string",
    "outbound": "boolean",
    "search_term": "string",
    "search_results": "int",
    "search_area": "string",
    "video_title": "string",
    "video_provider": "string",
    "video_duration": "double",
    "video_current_time": "double",
    "video_percent": "int",
    "file_name": "string",
    "file_extension": "string",
    "item_list_id": "string",
    "item_list_name": "string",
    "transaction_id": "string",
    "value": "double",
    "tax": "double",
    "shipping": "double",
    "currency": "string",
    "shipping_tier": "string",
    "payment_type": "string",
    "coupon": "string",
    "reason": "string",
    "lead_source": "string",
    "method": "string",
    "element_id": "string",
}

# Os motivos de rejeição do R7, na ordem em que são testados.
FORA_DO_CATALOGO = "evento fora do catálogo"
TIMESTAMP_RUIM = "timestamp ausente ou futuro"
TRANSACAO_SEM_ID = "transação sem id"
VALOR_INVALIDO = "valor monetário inválido"
MARCO_FORA_DO_PADRAO = "marco fora do padrão"
REEMBOLSO_SEM_COMPRA = "reembolso sem compra"


def motivo_rejeicao() -> F.Column:
    """O primeiro motivo que o evento quebra, ou null se ele passa nas cinco primeiras regras.

    A sexta (reembolso sem compra) depende das compras válidas e é aplicada depois."""
    nome = F.col("event_name")
    # event_ts que não segue o formato também vira null no try_to_timestamp: sem um instante
    # confiável o evento não pode ser sessionizado, então conta como timestamp ausente.
    ts_ruim = F.col("event_ts_tipado").isNull() | (F.col("event_ts_tipado") > F.col("ingestao_ts"))
    sem_id = nome.isin("purchase", "refund") & F.col("transaction_id").isNull()
    # "evento que tem value": a chave existe em params. Um value que não é número também é inválido.
    tem_valor = F.col("params")["value"].isNotNull()
    valor_ruim = tem_valor & (F.col("value").isNull() | (F.col("value") < 0)
                              | ~F.coalesce(F.col("currency") == "BRL", F.lit(False)))
    marco_ruim = (
        ((nome == "scroll") & ~F.coalesce(F.col("percent_scrolled").isin(MARCOS_SCROLL), F.lit(False)))
        | (nome.startswith("video_") & ~F.coalesce(F.col("video_percent").isin(MARCOS_VIDEO), F.lit(False)))
    )
    return (F.when(~F.coalesce(nome.isin(CATALOGO), F.lit(False)), F.lit(FORA_DO_CATALOGO))
             .when(ts_ruim, F.lit(TIMESTAMP_RUIM))
             .when(sem_id, F.lit(TRANSACAO_SEM_ID))
             .when(valor_ruim, F.lit(VALOR_INVALIDO))
             .when(marco_ruim, F.lit(MARCO_FORA_DO_PADRAO)))


def pagina(location: F.Column) -> F.Column:
    """Caminho de page_location sem domínio, '#' e '?'; a raiz vira '/' (5.4, item 12)."""
    caminho_url = F.regexp_extract(location, r"^[A-Za-z][A-Za-z0-9+.-]*://[^/?#]*([^?#]*)", 1)
    return F.when(location.isNull(), F.lit(None)).when(caminho_url == "", F.lit("/")).otherwise(caminho_url)


def main() -> None:
    spark = criar_sessao("silver")
    exigir_camada(spark, glob_eventos("bronze"), "python pipeline/bronze.py")

    bronze = ler_eventos(spark, "bronze")
    colunas_bronze = bronze.columns

    # R5: uma linha por event_id. Entre as cópias, fica a que entrou primeiro na Bronze.
    primeira_copia = Window.partitionBy("event_id").orderBy("ingestao_ts", "arquivo_origem")
    unicos = (bronze.withColumn("_copia", F.row_number().over(primeira_copia))
              .filter(F.col("_copia") == 1).drop("_copia"))

    # R6: params tipados ao lado das colunas da Bronze (que os rejeitados guardam como chegaram).
    tipados = unicos.select(
        "*",
        F.try_to_timestamp("event_ts", F.lit(FORMATO_TS)).alias("event_ts_tipado"),
        *[F.col("params")[chave].try_cast(tipo).alias(chave) for chave, tipo in PARAMS.items()],
    )

    # R7: as cinco primeiras regras olham só o próprio evento; a sexta precisa das compras válidas.
    avaliados = tipados.withColumn("motivo_rejeicao", motivo_rejeicao())
    compras_validas = (avaliados
                       .filter((F.col("event_name") == "purchase") & F.col("motivo_rejeicao").isNull())
                       .select(F.col("transaction_id").alias("_compra")).distinct())
    avaliados = (avaliados
                 .join(F.broadcast(compras_validas), F.col("transaction_id") == F.col("_compra"), "left")
                 .withColumn("motivo_rejeicao",
                             F.when(F.col("motivo_rejeicao").isNull() & (F.col("event_name") == "refund")
                                    & F.col("_compra").isNull(), F.lit(REEMBOLSO_SEM_COMPRA))
                              .otherwise(F.col("motivo_rejeicao")))
                 .drop("_compra")
                 .cache())

    rejeitados = avaliados.filter(F.col("motivo_rejeicao").isNotNull()).select(*colunas_bronze, "motivo_rejeicao")

    # R8: ordena os eventos de cada navegador e corta onde a pausa passa de 30 minutos.
    validos = avaliados.filter(F.col("motivo_rejeicao").isNull())
    linha_do_tempo = Window.partitionBy("user_pseudo_id").orderBy("event_ts_tipado", "event_id")
    ate_aqui = linha_do_tempo.rowsBetween(Window.unboundedPreceding, Window.currentRow)
    navegador_inteiro = linha_do_tempo.rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
    pausa = F.unix_micros("event_ts_tipado") - F.unix_micros(F.lag("event_ts_tipado").over(linha_do_tempo))
    validos = (validos
               .withColumn("_nova_sessao", F.when(pausa.isNull() | (pausa > PAUSA_DA_SESSAO_US), 1).otherwise(0))
               .withColumn("session_number", F.sum("_nova_sessao").over(ate_aqui))
               .withColumn("session_id", F.concat_ws("-s", "user_pseudo_id", F.col("session_number").cast("string")))
               # R9: o user_id que o navegador teve em algum momento vale para todos os eventos dele.
               .withColumn("user_id_resolvido", F.first("user_id", ignorenulls=True).over(navegador_inteiro)))

    silver = validos.select(
        "event_id",
        "event_name",
        F.col("event_ts_tipado").alias("event_ts"),
        "user_pseudo_id",
        F.col("user_id_resolvido").alias("user_id"),
        F.col("user_id").alias("user_id_original"),
        "session_id",
        "session_number",
        F.col("page.location").alias("page_location"),
        F.col("page.referrer").alias("page_referrer"),
        pagina(F.col("page.location")).alias("pagina"),
        F.col("traffic.source").alias("traffic_source"),
        F.col("traffic.medium").alias("traffic_medium"),
        F.col("traffic.campaign").alias("traffic_campaign"),
        F.col("device.category").alias("device_category"),
        F.col("device.os").alias("device_os"),
        F.col("device.browser").alias("device_browser"),
        *PARAMS,
        "items",
        "arquivo_origem",
        "ingestao_ts",
        F.date_format("event_ts_tipado", "yyyy").alias("year"),
        F.date_format("event_ts_tipado", "MM").alias("month"),
        F.date_format("event_ts_tipado", "dd").alias("day"),
    ).cache()

    # R10: uma linha por item por evento, com a posição do item na lista.
    itens = (silver
             .select("event_id", "event_name", "event_ts", "session_id", "user_pseudo_id", "user_id",
                     "transaction_id", F.posexplode("items").alias("posicao", "item"), "year", "month", "day")
             .select("event_id", "event_name", "event_ts", "session_id", "user_pseudo_id", "user_id",
                     "transaction_id", "posicao", "item.*", "year", "month", "day"))

    gravar_eventos(silver, "silver", "overwrite")
    itens.write.mode("overwrite").partitionBy("year", "month", "day").parquet(caminho("silver/itens"))
    rejeitados.write.mode("overwrite").parquet(caminho("silver/eventos_rejeitados"))

    linhas_bronze = bronze.count()
    total_unicos = avaliados.count()
    total_validos = silver.count()
    titulo("Silver")
    print(f"linhas na bronze:      {linhas_bronze}")
    print(f"duplicatas removidas:  {linhas_bronze - total_unicos}")
    print(f"eventos válidos:       {total_validos}")
    print(f"eventos rejeitados:    {total_unicos - total_validos}")
    print(f"sessões:               {silver.select('session_id').distinct().count()}")
    print(f"visitantes:            {silver.select('user_pseudo_id').distinct().count()}")
    print(f"eventos com user_id:   {silver.filter(F.col('user_id_original').isNotNull()).count()} antes da costura, "
          f"{silver.filter(F.col('user_id').isNotNull()).count()} depois")
    print(f"linhas de itens:       {spark.read.parquet(caminho('silver/itens')).count()}")
    print("\nrejeitados por motivo:")
    rejeitados.groupBy("motivo_rejeicao").count().orderBy(F.desc("count")).show(truncate=False)

    spark.stop()


if __name__ == "__main__":
    main()
