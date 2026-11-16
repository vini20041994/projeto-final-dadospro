"""Job Gold do mapa de calor: silver -> gold/mapa_calor_cliques e gold/mapa_calor_rolagem.

    docker compose run --rm app python pipeline/gold_mapa_calor.py

Nomes de coluna EXATOS (R11), porque o site os lê:
- mapa_calor_cliques: pagina, element_id, cliques, sessoes, sessoes_na_pagina, taxa_clique, intensidade.
  Uma linha por página × element_id, só com eventos que têm element_id e page_location.
    cliques           = eventos
    sessoes           = sessões distintas que clicaram
    sessoes_na_pagina = sessões com page_view na página
    taxa_clique       = sessoes ÷ sessoes_na_pagina (nulo se o divisor for 0), 4 casas
    intensidade       = cliques ÷ o maior cliques da mesma página (0 a 1), 4 casas
- mapa_calor_rolagem: pagina, marco, sessoes_que_chegaram, pct_sessoes. Uma linha por marco
  (0, 25, 50, 75, 90) da landing (pagina = "/"): entre as sessões com page_view na landing, as que têm
  scroll na landing com percent_scrolled >= marco (marco 0 = todas). pct_sessoes = chegaram ÷ as do marco 0.
"""

from pyspark.sql import Window
from pyspark.sql import functions as F

from _gold import gravar_gold, ler_silver, mostrar
from comum import criar_sessao, titulo

MARCOS = [0, 25, 50, 75, 90]
LANDING = "/"


def main() -> None:
    spark = criar_sessao("gold_mapa_calor")
    eventos = ler_silver(spark)

    # sessões que viram cada página: o divisor da taxa de clique e a base da rolagem
    na_pagina = (eventos.filter(F.col("event_name") == "page_view").filter(F.col("pagina").isNotNull())
                 .groupBy("pagina").agg(F.countDistinct("session_id").alias("sessoes_na_pagina")))

    cliques = (eventos.filter(F.col("element_id").isNotNull() & F.col("page_location").isNotNull())
               .groupBy("pagina", "element_id")
               .agg(F.count("*").alias("cliques"), F.countDistinct("session_id").alias("sessoes"))
               .join(na_pagina, "pagina", "left")
               .withColumn("taxa_clique", F.round(F.try_divide(F.col("sessoes"), F.col("sessoes_na_pagina")), 4))
               .withColumn("intensidade", F.round(F.col("cliques") / F.max("cliques").over(Window.partitionBy("pagina")), 4))
               .select("pagina", "element_id", "cliques", "sessoes",
                       F.coalesce("sessoes_na_pagina", F.lit(0)).alias("sessoes_na_pagina"),
                       "taxa_clique", "intensidade")
               .orderBy("pagina", F.desc("cliques")))
    gravar_gold(cliques, "mapa_calor_cliques")

    # rolagem: só as sessões com page_view na landing; o marco 0 é o total delas
    viram = eventos.filter((F.col("event_name") == "page_view") & (F.col("pagina") == LANDING)).select("session_id").distinct()
    maximo = (eventos.filter((F.col("event_name") == "scroll") & (F.col("pagina") == LANDING))
              .groupBy("session_id").agg(F.max("percent_scrolled").alias("scroll_max")))
    base = viram.join(maximo, "session_id", "left").cache()
    total = base.count()
    marcos = spark.createDataFrame([(m,) for m in MARCOS], "marco int")
    rolagem = (marcos.crossJoin(base.select("scroll_max"))
               .groupBy("marco")
               .agg(F.sum(F.when(F.col("marco") == 0, 1)
                           .when(F.coalesce(F.col("scroll_max"), F.lit(-1)) >= F.col("marco"), 1)
                           .otherwise(0)).alias("sessoes_que_chegaram"))
               .withColumn("pagina", F.lit(LANDING))
               .withColumn("pct_sessoes", F.round(F.try_divide(F.col("sessoes_que_chegaram"), F.lit(total)), 4))
               .select("pagina", "marco", "sessoes_que_chegaram", "pct_sessoes").orderBy("marco"))
    gravar_gold(rolagem, "mapa_calor_rolagem")

    titulo("Gold: mapa de calor")
    mostrar(spark, "mapa_calor_cliques", 40)
    mostrar(spark, "mapa_calor_rolagem")
    spark.stop()


if __name__ == "__main__":
    main()
