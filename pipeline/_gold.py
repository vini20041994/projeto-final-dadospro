"""Peças compartilhadas pelos jobs Gold. Não é um job (o nome começa com "_": o explorador ignora).

    from _gold import ler_silver, sessoes_base, gravar_gold, canal, ...

Fica ao lado dos jobs, como o comum.py: os jobs rodam como `python pipeline/<job>.py`, então o
Python encontra este arquivo na mesma pasta.

Definições de 5.4 do enunciado que mais de uma tabela usa ficam aqui, escritas uma vez só:
- canal da sessão = "<source> / <medium>" do primeiro evento da sessão;
- plano da sessão = item_id do primeiro select_item da sessão, "(nenhum)" se não houver;
- datas em UTC (a sessão Spark do comum.py já usa spark.sql.session.timeZone=UTC).
"""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from comum import caminho, exigir_camada, glob_eventos, ler_eventos

SEM_PLANO = "(nenhum)"
TODOS = "(todos)"           # linha de total nas tabelas com cubo (plano × canal etc.)

ETAPAS_FUNIL = ["view_item_list", "select_item", "add_to_cart", "view_cart",
                "begin_checkout", "add_payment_info", "purchase"]

# Eventos de que a Gold quer saber "a sessão teve?" (coluna tem_<evento> em sessoes_base).
EVENTOS_FLAG = ETAPAS_FUNIL + ["view_item", "generate_lead", "video_start", "site_search", "file_download", "click"]


def ler_silver(spark: SparkSession) -> DataFrame:
    """Eventos válidos da Silver. Para o job com mensagem clara se a Silver não existe (R2)."""
    exigir_camada(spark, glob_eventos("silver"), "python pipeline/silver.py")
    return ler_eventos(spark, "silver")


def canal(origem: str = "traffic_source", meio: str = "traffic_medium") -> F.Column:
    return F.concat_ws(" / ", F.col(origem), F.col(meio))


def ordem_do_evento() -> F.Column:
    """Desempata eventos no mesmo instante pelo event_id, como a sessionização da Silver."""
    return F.struct("event_ts", "event_id")


def sessoes_base(eventos: DataFrame) -> DataFrame:
    """Uma linha por sessão, com tudo o que as Golds de funil, engajamento e ML precisam.

    Cada coluna agrega os eventos da sessão em uma única passada pelos dados (sem joins).
    `min_by` pega o valor do primeiro evento pela ordem (event_ts, event_id); o `when` dentro
    do critério faz o `min_by` olhar só os eventos que interessam (critério nulo é ignorado).
    """
    ordem = ordem_do_evento()
    nome = F.col("event_name")
    marcas = [F.max((nome == e).cast("int")).alias(f"tem_{e}") for e in EVENTOS_FLAG]
    return eventos.groupBy("session_id").agg(
        F.first("user_pseudo_id").alias("user_pseudo_id"),
        F.max("user_id").alias("user_id"),   # a costura (R9) deixa um user_id só por navegador
        F.min_by(canal(), ordem).alias("canal"),
        F.min_by("device_category", ordem).alias("dispositivo"),
        F.coalesce(
            F.min_by(F.get("items", F.lit(0))["item_id"], F.when(nome == "select_item", ordem)),
            F.lit(SEM_PLANO)).alias("plano"),
        F.min("event_ts").alias("inicio"),
        F.max("event_ts").alias("fim"),
        F.count("*").alias("eventos"),
        F.max("percent_scrolled").alias("scroll_max"),
        F.max("video_percent").alias("video_percent_max"),
        # marcas de funil de 5.4 (item 4): conta quem teve o evento em qualquer momento
        *marcas,
        # funil_lead: só a landing ("/") conta
        F.max(((nome == "page_view") & (F.col("pagina") == "/")).cast("int")).alias("tem_page_view_landing"),
        F.max(((nome == "scroll") & (F.col("pagina") == "/") & (F.col("percent_scrolled") >= 50)).cast("int"))
         .alias("tem_scroll_50_landing"),
        F.max(F.when((nome == "scroll") & (F.col("pagina") == "/"), F.col("percent_scrolled")))
         .alias("scroll_max_landing"),
        F.max((F.col("event_name") == "purchase").cast("int")).alias("comprou"),
    )


def gravar_gold(df: DataFrame, nome: str) -> None:
    """Grava gold/<nome> em Parquet, recalculando a tabela inteira (overwrite)."""
    df.write.mode("overwrite").parquet(caminho(f"gold/{nome}"))


def mostrar(spark: SparkSession, nome: str, linhas: int = 20) -> None:
    """Lê a tabela de volta do bucket e mostra: confere que o que foi gravado é o que se espera."""
    tabela = spark.read.parquet(caminho(f"gold/{nome}"))
    print(f"\n>> gold/{nome}: {tabela.count()} linhas")
    tabela.show(linhas, truncate=False)


def rotular_totais(df: DataFrame, dimensoes: list[str]) -> DataFrame:
    """Depois de um cube/agg com F.grouping, troca NULL de subtotal por '(todos)'.

    Pressupõe que o agg incluiu F.grouping(<dim>).alias("_g_<dim>") para cada dimensão. Sem isso,
    não dá para distinguir 'subtotal' de 'valor realmente nulo'.
    """
    for d in dimensoes:
        df = df.withColumn(d, F.when(F.col(f"_g_{d}") == 1, F.lit(TODOS)).otherwise(F.col(d))).drop(f"_g_{d}")
    return df


def agregar_cubo(base: DataFrame, dimensoes: list[str], *agregacoes: F.Column) -> DataFrame:
    """base.cube(*dimensoes).agg(*agregacoes) com os subtotais rotulados como '(todos)'."""
    marcas = [F.grouping(d).alias(f"_g_{d}") for d in dimensoes]
    return rotular_totais(base.cube(*dimensoes).agg(*agregacoes, *marcas), dimensoes)
