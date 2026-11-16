"""Job Gold de engajamento: silver -> sessoes, retencao_video, buscas_faq e saidas_downloads.

    docker compose run --rm app python pipeline/gold_engajamento.py

Responde à pergunta 2 do enunciado: o vídeo é visto até o fim? O que o visitante procura no FAQ e não acha?

- sessoes: uma linha por sessão (duração, eventos, scroll máximo, maior video_percent, fez lead,
  comprou, canal, dispositivo).
- retencao_video: das sessões com video_start, quantas atingiram 10, 25, 50, 75 e 100%.
- buscas_faq: por termo, total de buscas e buscas sem resultado (search_results = 0).
- saidas_downloads: cliques de saída por domínio e downloads por arquivo, numa tabela só (coluna tipo).
"""

from pyspark.sql import functions as F

from _gold import gravar_gold, ler_silver, mostrar, sessoes_base
from comum import criar_sessao, titulo

MARCOS_RETENCAO = [10, 25, 50, 75, 100]


def main() -> None:
    spark = criar_sessao("gold_engajamento")
    eventos = ler_silver(spark)
    base = sessoes_base(eventos).cache()

    sessoes = base.select(
        "session_id", "user_pseudo_id", "user_id", "canal", "dispositivo", "inicio", "fim",
        ((F.unix_micros("fim") - F.unix_micros("inicio")) / 1_000_000).alias("duracao_seg"),
        "eventos", "scroll_max",
        F.col("video_percent_max").alias("maior_video_percent"),
        (F.col("tem_generate_lead") == 1).alias("fez_lead"),
        (F.col("comprou") == 1).alias("comprou"),
    )
    gravar_gold(sessoes, "sessoes")

    # retencao_video: o maior video_percent da sessão diz até onde ela viu (>= marco). Só entram as
    # sessões que deram play (video_start), que são o denominador de todos os marcos.
    com_video = base.filter(F.col("tem_video_start") == 1)
    total_video = com_video.count()
    retencao = spark.createDataFrame([(m,) for m in MARCOS_RETENCAO], "marco int").crossJoin(
        com_video.select("video_percent_max")
    ).groupBy("marco").agg(
        F.lit(total_video).alias("sessoes_com_video_start"),
        F.sum((F.coalesce(F.col("video_percent_max"), F.lit(0)) >= F.col("marco")).cast("int")).alias("sessoes_que_atingiram"),
    ).withColumn("pct_sessoes", F.round(F.try_divide(F.col("sessoes_que_atingiram"), F.col("sessoes_com_video_start")), 4)
                 ).orderBy("marco")
    gravar_gold(retencao, "retencao_video")

    buscas = (eventos.filter(F.col("event_name") == "site_search")
              .groupBy(F.col("search_term").alias("termo"))
              .agg(F.count("*").alias("total_buscas"),
                   F.sum((F.col("search_results") == 0).cast("int")).alias("buscas_sem_resultado"),
                   F.countDistinct("session_id").alias("sessoes"))
              .withColumn("pct_sem_resultado", F.round(F.try_divide(F.col("buscas_sem_resultado"), F.col("total_buscas")), 4))
              .orderBy(F.desc("buscas_sem_resultado"), F.desc("total_buscas")))
    gravar_gold(buscas, "buscas_faq")

    saidas = (eventos.filter((F.col("event_name") == "click") & (F.col("outbound") == True))  # noqa: E712
              .select(F.lit("saida").alias("tipo"), F.col("link_domain").alias("destino"), "session_id"))
    downloads = (eventos.filter(F.col("event_name") == "file_download")
                 .select(F.lit("download").alias("tipo"), F.col("file_name").alias("destino"), "session_id"))
    saidas_downloads = (saidas.unionByName(downloads).groupBy("tipo", "destino")
                        .agg(F.count("*").alias("eventos"), F.countDistinct("session_id").alias("sessoes"))
                        .orderBy("tipo", F.desc("eventos")))
    gravar_gold(saidas_downloads, "saidas_downloads")

    titulo("Gold: engajamento")
    mostrar(spark, "sessoes", 5)
    mostrar(spark, "retencao_video")
    mostrar(spark, "buscas_faq")
    mostrar(spark, "saidas_downloads")
    spark.stop()


if __name__ == "__main__":
    main()
