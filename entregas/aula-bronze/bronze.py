"""Job Bronze: landing (JSON Lines) -> bronze (Parquet).

    docker compose run --rm app python pipeline/bronze.py

O que este job faz (requisitos R1, R2 e R4 do enunciado):
- lê a landing com schema explícito, sem inferência;
- não limpa nada: duplicatas, eventos fora do catálogo e datas estranhas entram como chegaram
  (quem decide o que é inválido é a Silver);
- acrescenta as colunas de proveniência: arquivo_origem e ingestao_ts;
- grava em append, particionado por event_name/year/month/day (o dia de coleta, do caminho da landing);
- é append-only e idempotente: só ingere arquivos da landing que ainda não estão na Bronze,
  então rodar duas vezes não duplica nada.
"""

from datetime import datetime, timezone

from pyspark.sql import functions as F
from pyspark.sql.types import (ArrayType, DoubleType, LongType, MapType, StringType, StructField,
                               StructType)

from comum import camada_existe, criar_sessao, exigir_camada, glob_eventos, gravar_eventos, ler_eventos, titulo

# Schema explícito da landing (R4), conferido com docs/CONTRATO_EVENTO.md e com os dados reais.
# - event_ts fica como texto: converter aqui transformaria uma data mal escrita em null sem deixar
#   rastro. A conversão (e a rejeição) é trabalho da Silver.
# - params muda de evento para evento, então vira um map de texto: nenhum valor se perde e a Silver
#   extrai cada chave com o tipo certo (R6).
# - items traz index, item_list_id e item_list_name nos eventos view_item_list e select_item,
#   campos que o contrato não cita, mas que existem nos dados.
# - _corrupt_record guarda a linha original quando ela não bate com o schema (JSON quebrado ou tipo
#   errado), em vez de a linha virar só nulos em silêncio.
# - year, month e day não estão aqui: vêm do caminho da landing, como texto.
ITEM = StructType([
    StructField("item_id", StringType()),
    StructField("item_name", StringType()),
    StructField("item_category", StringType()),
    StructField("price", DoubleType()),
    StructField("quantity", LongType()),
    StructField("index", LongType()),
    StructField("item_list_id", StringType()),
    StructField("item_list_name", StringType()),
])

SCHEMA = StructType([
    StructField("event_id", StringType()),
    StructField("event_name", StringType()),
    StructField("event_ts", StringType()),
    StructField("user_pseudo_id", StringType()),
    StructField("user_id", StringType()),
    StructField("session_hint", StringType()),
    StructField("page", StructType([
        StructField("location", StringType()),
        StructField("referrer", StringType()),
    ])),
    StructField("traffic", StructType([
        StructField("source", StringType()),
        StructField("medium", StringType()),
        StructField("campaign", StringType()),
    ])),
    StructField("device", StructType([
        StructField("category", StringType()),
        StructField("os", StringType()),
        StructField("browser", StringType()),
    ])),
    StructField("params", MapType(StringType(), StringType())),
    StructField("items", ArrayType(ITEM)),
    StructField("_corrupt_record", StringType()),
])

COLUNAS = [f.name for f in SCHEMA.fields] + ["arquivo_origem", "ingestao_ts", "year", "month", "day"]


def main() -> None:
    spark = criar_sessao("bronze")
    exigir_camada(spark, glob_eventos("landing"), "python gerador/gerar_eventos.py")

    # Um instante só para a execução inteira, calculado no Python: todas as linhas desta
    # ingestão recebem o mesmo carimbo, mesmo que o Spark avalie o plano mais de uma vez.
    agora = datetime.now(timezone.utc).replace(tzinfo=None)

    landing = (ler_eventos(spark, "landing", "json", schema=SCHEMA)
               .withColumn("arquivo_origem", F.col("_metadata.file_path"))
               .withColumn("ingestao_ts", F.lit(agora).cast("timestamp")))

    # Append-only sem duplicar: fica só o que veio de arquivos que a Bronze ainda não tem.
    # O caminho completo identifica o arquivo (o mesmo nome coletor-s42-001.jsonl se repete
    # em várias pastas event_name=/dia).
    if camada_existe(spark, glob_eventos("bronze")):
        ja_ingeridos = ler_eventos(spark, "bronze").select("arquivo_origem").distinct()
        novos = landing.join(F.broadcast(ja_ingeridos), "arquivo_origem", "left_anti")
    else:
        novos = landing

    novos = novos.select(*COLUNAS).cache()
    linhas = novos.count()

    titulo("Bronze")
    if linhas == 0:
        print("Nenhum arquivo novo na landing: a Bronze já está em dia. Nada foi gravado.")
        spark.stop()
        return

    gravar_eventos(novos, "bronze", "append")

    arquivos = novos.select("arquivo_origem").distinct().count()
    corrompidas = novos.filter(F.col("_corrupt_record").isNotNull()).count()
    print(f"ingestão em:        {agora.isoformat()}Z")
    print(f"arquivos novos:     {arquivos}")
    print(f"linhas gravadas:    {linhas}")
    print(f"linhas corrompidas: {corrompidas}")
    print("\nlinhas por event_name:")
    novos.groupBy("event_name").count().orderBy(F.desc("count")).show(50, truncate=False)

    spark.stop()


if __name__ == "__main__":
    main()
