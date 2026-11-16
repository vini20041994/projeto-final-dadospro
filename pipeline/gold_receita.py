"""Job Gold de receita e aquisição: silver -> receita_diaria, taxa_reembolso, adesao_order_bump e lead_para_compra.

    docker compose run --rm app python pipeline/gold_receita.py

Responde à pergunta 3 do enunciado: quanto entrou de fato, descontados os reembolsos, e qual canal traz
cliente que não desiste?

Definições (5.4 do enunciado):
- receita bruta = soma de value dos purchase válidos; reembolsada = soma de value dos refund válidos,
  atribuída à compra pelo transaction_id (entra na data, no plano e no canal da COMPRA, não do reembolso);
  líquida = bruta - reembolsada; ticket médio = bruta ÷ pedidos;
- plano da compra = o item de item_category 'plano'; canal da compra = canal da sessão da compra;
- datas em UTC (a SparkSession do comum.py usa UTC).

Tabelas:
- receita_diaria: uma linha por data da compra × plano × canal (sem totais, para poder somar sem repetir).
- taxa_reembolso: por plano × canal, com linhas de total ('(todos)').
- adesao_order_bump: percentual de compras com BUMP-MENTORIA, total e por plano.
- lead_para_compra: por pessoa, o primeiro generate_lead e a primeira compra estritamente depois dele.
"""

from pyspark.sql import Window
from pyspark.sql import functions as F

from _gold import agregar_cubo, gravar_gold, ler_silver, mostrar, sessoes_base
from comum import criar_sessao, titulo


def main() -> None:
    spark = criar_sessao("gold_receita")
    eventos = ler_silver(spark)
    canais = sessoes_base(eventos).select("session_id", "canal")

    reembolsos = (eventos.filter(F.col("event_name") == "refund")
                  .groupBy("transaction_id").agg(F.sum("value").alias("valor_reembolsado")))
    plano = F.element_at(F.filter("items", lambda item: item["item_category"] == "plano"), 1)["item_id"]
    tem_bump = F.exists("items", lambda item: item["item_id"] == "BUMP-MENTORIA")
    compras = (eventos.filter(F.col("event_name") == "purchase")
               .select("transaction_id", "session_id", "user_pseudo_id", "user_id", "event_id",
                       F.col("event_ts").alias("ts_compra"), F.col("value").alias("valor"),
                       plano.alias("plano"), tem_bump.alias("com_bump"))
               .join(canais, "session_id", "left")
               .join(reembolsos, "transaction_id", "left")
               .withColumn("valor_reembolsado", F.coalesce("valor_reembolsado", F.lit(0.0)))
               .withColumn("data_compra", F.to_date("ts_compra"))
               .cache())

    receita = (compras.groupBy("data_compra", "plano", "canal")
               .agg(F.round(F.sum("valor"), 2).alias("receita_bruta"),
                    F.round(F.sum("valor_reembolsado"), 2).alias("receita_reembolsada"),
                    F.count("*").alias("pedidos"))
               .withColumn("receita_liquida", F.round(F.col("receita_bruta") - F.col("receita_reembolsada"), 2))
               .withColumn("ticket_medio", F.round(F.try_divide(F.col("receita_bruta"), F.col("pedidos")), 2))
               .select("data_compra", "plano", "canal", "receita_bruta", "receita_reembolsada",
                       "receita_liquida", "pedidos", "ticket_medio")
               .orderBy("data_compra", "plano", "canal"))
    gravar_gold(receita, "receita_diaria")

    reembolso = (agregar_cubo(compras.withColumn("reembolsado", (F.col("valor_reembolsado") > 0).cast("int")),
                              ["plano", "canal"],
                              F.count("*").alias("pedidos"),
                              F.sum("reembolsado").alias("pedidos_com_reembolso"),
                              F.round(F.sum("valor"), 2).alias("receita_bruta"),
                              F.round(F.sum("valor_reembolsado"), 2).alias("receita_reembolsada"))
                 .withColumn("taxa_reembolso", F.round(F.try_divide(F.col("pedidos_com_reembolso"), F.col("pedidos")), 4))
                 .select("plano", "canal", "pedidos", "pedidos_com_reembolso", "taxa_reembolso",
                         "receita_bruta", "receita_reembolsada")
                 .orderBy("plano", "canal"))
    gravar_gold(reembolso, "taxa_reembolso")

    bump = (agregar_cubo(compras.withColumn("bump", F.col("com_bump").cast("int")), ["plano"],
                         F.count("*").alias("compras"), F.sum("bump").alias("compras_com_bump"))
            .withColumn("pct_com_bump", F.round(F.try_divide(F.col("compras_com_bump"), F.col("compras")), 4))
            .select("plano", "compras", "compras_com_bump", "pct_com_bump").orderBy("plano"))
    gravar_gold(bump, "adesao_order_bump")

    # lead -> compra. A unidade é a pessoa: user_id (já costurado na Silver) ou, sem ele, user_pseudo_id.
    pessoa = F.coalesce("user_id", "user_pseudo_id")
    primeiro = Window.partitionBy("pessoa").orderBy("event_ts", "event_id")
    leads = (eventos.filter(F.col("event_name") == "generate_lead")
             .select(pessoa.alias("pessoa"), "event_ts", "event_id", "session_id")
             .withColumn("_n", F.row_number().over(primeiro)).filter("_n = 1")
             .select("pessoa", F.col("event_ts").alias("ts_lead"), "session_id")
             .join(canais.withColumnRenamed("canal", "canal_lead"), "session_id", "left"))
    primeira_compra_depois = (
        leads.select("pessoa", "ts_lead")
        .join(compras.select(pessoa.alias("pessoa"), "ts_compra"), "pessoa")
        .filter(F.col("ts_compra") > F.col("ts_lead"))                  # estritamente depois do lead
        .groupBy("pessoa").agg(F.min("ts_compra").alias("ts_primeira_compra")))
    por_pessoa = (leads.join(primeira_compra_depois, "pessoa", "left")
                  .withColumn("dias", (F.unix_micros("ts_primeira_compra") - F.unix_micros("ts_lead"))
                              / 1_000_000 / 86400))
    lead_compra = (agregar_cubo(por_pessoa.withColumn("comprou", F.col("ts_primeira_compra").isNotNull().cast("int")),
                                ["canal_lead"],
                                F.count("*").alias("leads"),
                                F.sum("comprou").alias("leads_que_compraram"),
                                F.percentile_approx("dias", 0.5).alias("mediana_dias_ate_compra"))
                   .withColumn("taxa_lead_para_compra",
                               F.round(F.try_divide(F.col("leads_que_compraram"), F.col("leads")), 4))
                   .select("canal_lead", "leads", "leads_que_compraram", "taxa_lead_para_compra",
                           "mediana_dias_ate_compra").orderBy("canal_lead"))
    gravar_gold(lead_compra, "lead_para_compra")

    titulo("Gold: receita e aquisição")
    mostrar(spark, "receita_diaria", 10)
    mostrar(spark, "taxa_reembolso", 30)
    mostrar(spark, "adesao_order_bump")
    mostrar(spark, "lead_para_compra")
    spark.stop()


if __name__ == "__main__":
    main()
