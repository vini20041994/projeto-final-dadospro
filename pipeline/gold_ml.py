"""Job Gold de ML: silver -> propensao_sessao, metricas_modelo e segmentos_visitantes.

    docker compose run --rm app python pipeline/gold_ml.py

Responde à pergunta 4 do enunciado: quais sessões estão perto de comprar, e que perfis de visitante existem?

Propensão (5.3 e 5.4, item 6):
- uma linha por sessão (todas as da Silver); rótulo = a sessão tem um purchase;
- SEM VAZAMENTO: as features só olham eventos da LANDING ("/") que aconteceram ANTES da primeira etapa
  de carrinho/checkout da sessão (add_to_cart, view_cart, begin_checkout, add_shipping_info,
  add_payment_info, purchase). Nada do checkout entra, nem o próprio purchase;
- separação treino/teste por VISITANTE (hash do user_pseudo_id), para que sessões do mesmo navegador não
  caiam nos dois lados e o teste seja mesmo "dado que o modelo não viu";
- modelo: regressão logística. O limiar de decisão é o que maximiza o F1 no TREINO (compradores são
  raros; 0,5 daria quase só "não compra"). AUC, F1, precisão e recall são medidos só no TESTE e
  referem-se à classe positiva (sessão com compra).

Segmentos: K-Means por visitante (user_pseudo_id) sobre o comportamento padronizado; o K (3 a 6) é o de
maior silhueta.
"""

from pyspark.ml import Pipeline
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import BinaryClassificationEvaluator, ClusteringEvaluator
from pyspark.ml.feature import OneHotEncoder, StandardScaler, StringIndexer, VectorAssembler
from pyspark.ml.functions import vector_to_array
from pyspark.sql import functions as F

from _gold import SEM_PLANO, gravar_gold, ler_silver, mostrar
from comum import criar_sessao, titulo

# Eventos que marcam a entrada no carrinho/checkout: a partir do primeiro deles, nada vira feature.
CORTE = ["add_to_cart", "view_cart", "begin_checkout", "add_shipping_info", "add_payment_info", "purchase"]
FEATURES_NUMERICAS = [
    "scroll_max", "video_iniciou", "video_percent_max", "viu_planos", "selecionou_plano", "abriu_detalhe",
    "buscas", "downloads", "cliques", "eventos_antes_do_carrinho",
]
FEATURES_CATEGORICAS = ["canal", "dispositivo", "plano"]
PROPORCAO_TESTE = 0.2        # ~20% dos visitantes, escolhidos por hash (não por sorteio: é reproduzível)
SEMENTE = 42


def montar_sessoes(eventos):
    """Uma linha por sessão: rótulo e features só do comportamento na landing antes do carrinho."""
    nome = F.col("event_name")
    ordem = F.struct("event_ts", "event_id")
    corte = (eventos.groupBy("session_id")
             .agg(F.min(F.when(nome.isin(CORTE), F.col("event_ts"))).alias("ts_corte"),
                  F.max((nome == "purchase").cast("int")).alias("rotulo"),
                  F.min_by(F.concat_ws(" / ", "traffic_source", "traffic_medium"), ordem).alias("canal"),
                  F.min_by("device_category", ordem).alias("dispositivo"),
                  F.first("user_pseudo_id").alias("user_pseudo_id")))
    antes = (eventos.join(corte.select("session_id", "ts_corte"), "session_id")
             .filter((F.col("pagina") == "/") & (F.col("ts_corte").isNull() | (F.col("event_ts") < F.col("ts_corte")))))
    # o ts_corte nulo = a sessão nunca chegou ao carrinho: todo o comportamento na landing vale
    comportamento = antes.groupBy("session_id").agg(
        F.coalesce(F.max("percent_scrolled"), F.lit(0)).alias("scroll_max"),
        F.max((nome == "video_start").cast("int")).alias("video_iniciou"),
        F.coalesce(F.max("video_percent"), F.lit(0)).alias("video_percent_max"),
        F.max((nome == "view_item_list").cast("int")).alias("viu_planos"),
        F.max((nome == "select_item").cast("int")).alias("selecionou_plano"),
        F.max((nome == "view_item").cast("int")).alias("abriu_detalhe"),
        F.sum((nome == "site_search").cast("int")).alias("buscas"),
        F.sum((nome == "file_download").cast("int")).alias("downloads"),
        F.sum((nome == "click").cast("int")).alias("cliques"),
        F.count("*").alias("eventos_antes_do_carrinho"),
        F.coalesce(F.min_by(F.get("items", F.lit(0))["item_id"], F.when(nome == "select_item", ordem)),
                   F.lit(SEM_PLANO)).alias("plano"),
    )
    sessoes = corte.join(comportamento, "session_id", "left")
    zeros = {c: 0 for c in FEATURES_NUMERICAS}
    sessoes = sessoes.fillna(zeros).fillna({"plano": SEM_PLANO})
    # treino/teste por visitante: pmod(hash) cai em 0..99 e os menores de 20 vão para o teste
    percentil = F.pmod(F.hash("user_pseudo_id", F.lit(SEMENTE)), F.lit(100))
    return sessoes.withColumn("conjunto", F.when(percentil < int(PROPORCAO_TESTE * 100), "teste").otherwise("treino"))


def propensao(spark, eventos):
    sessoes = montar_sessoes(eventos).cache()
    treino, teste = sessoes.filter("conjunto = 'treino'"), sessoes.filter("conjunto = 'teste'")

    indexadores = [StringIndexer(inputCol=c, outputCol=f"{c}_idx", handleInvalid="keep") for c in FEATURES_CATEGORICAS]
    codificador = OneHotEncoder(inputCols=[f"{c}_idx" for c in FEATURES_CATEGORICAS],
                                outputCols=[f"{c}_vec" for c in FEATURES_CATEGORICAS], handleInvalid="keep")
    montador = VectorAssembler(inputCols=FEATURES_NUMERICAS + [f"{c}_vec" for c in FEATURES_CATEGORICAS],
                               outputCol="vetor", handleInvalid="keep")
    escala = StandardScaler(inputCol="vetor", outputCol="features", withMean=False, withStd=True)
    logistica = LogisticRegression(featuresCol="features", labelCol="rotulo", maxIter=100, regParam=0.01)
    modelo = Pipeline(stages=[*indexadores, codificador, montador, escala, logistica]).fit(treino)

    # limiar que maximiza o F1 da classe positiva no treino
    resumo = modelo.stages[-1].summary
    melhor = resumo.fMeasureByThreshold.orderBy(F.desc("F-Measure")).first()
    limiar = float(melhor["threshold"])

    previsto = modelo.transform(sessoes).withColumn("prob", vector_to_array("probability")[1])
    previsto = previsto.withColumn("previsto_compra", (F.col("prob") >= limiar).cast("int"))

    propensao_df = previsto.select(
        "session_id", "user_pseudo_id", "canal", "dispositivo", "plano", "conjunto",
        F.col("rotulo").alias("comprou"),
        F.round("prob", 4).alias("probabilidade_compra"), "previsto_compra",
    )

    # métricas só no teste
    t = previsto.filter("conjunto = 'teste'").cache()
    auc = BinaryClassificationEvaluator(labelCol="rotulo", rawPredictionCol="rawPrediction",
                                        metricName="areaUnderROC").evaluate(t)
    c = t.agg(
        F.sum(((F.col("previsto_compra") == 1) & (F.col("rotulo") == 1)).cast("int")).alias("tp"),
        F.sum(((F.col("previsto_compra") == 1) & (F.col("rotulo") == 0)).cast("int")).alias("fp"),
        F.sum(((F.col("previsto_compra") == 0) & (F.col("rotulo") == 1)).cast("int")).alias("fn"),
        F.sum("rotulo").alias("positivos"), F.count("*").alias("n"),
    ).first()
    tp, fp, fn = c["tp"], c["fp"], c["fn"]
    precisao = tp / (tp + fp) if (tp + fp) else 0.0
    recall = tp / (tp + fn) if (tp + fn) else 0.0
    f1 = 2 * precisao * recall / (precisao + recall) if (precisao + recall) else 0.0

    linhas = [
        ("metrica", "auc", float(auc), "teste"),
        ("metrica", "f1", f1, "classe positiva (compra), no limiar"),
        ("metrica", "precisao", precisao, "classe positiva (compra), no limiar"),
        ("metrica", "recall", recall, "classe positiva (compra), no limiar"),
        ("metrica", "limiar", limiar, "maximiza o F1 no treino"),
        ("metrica", "sessoes_treino", float(treino.count()), "treino"),
        ("metrica", "sessoes_teste", float(c["n"]), "teste"),
        ("metrica", "compras_no_teste", float(c["positivos"]), "teste"),
        ("metrica", "verdadeiros_positivos", float(tp), "teste"),
        ("metrica", "falsos_positivos", float(fp), "teste"),
        ("metrica", "falsos_negativos", float(fn), "teste"),
    ] + [("feature", f, None, "numérica") for f in FEATURES_NUMERICAS] \
      + [("feature", f, None, "categórica (one-hot)") for f in FEATURES_CATEGORICAS]
    metricas = spark.createDataFrame(linhas, "tipo string, nome string, valor double, detalhe string")
    return propensao_df, metricas


def segmentos(spark, eventos):
    """K-Means por visitante. O perfil de cada grupo é a média das colunas do comportamento."""
    nome = F.col("event_name")
    visitantes = eventos.groupBy("user_pseudo_id").agg(
        F.countDistinct("session_id").alias("sessoes"),
        F.count("*").alias("eventos"),
        F.coalesce(F.max("percent_scrolled"), F.lit(0)).alias("scroll_max"),
        F.coalesce(F.max("video_percent"), F.lit(0)).alias("video_percent_max"),
        F.sum((nome == "site_search").cast("int")).alias("buscas"),
        F.max((nome == "generate_lead").cast("int")).alias("fez_lead"),
        F.max((nome == "add_to_cart").cast("int")).alias("pos_no_carrinho"),
        F.max((nome == "purchase").cast("int")).alias("comprou"),
    ).cache()
    colunas = [c for c in visitantes.columns if c != "user_pseudo_id"]
    vetor = VectorAssembler(inputCols=colunas, outputCol="bruto").transform(visitantes)
    escalado = StandardScaler(inputCol="bruto", outputCol="features", withMean=True, withStd=True).fit(vetor).transform(vetor)

    melhor = None
    for k in range(3, 7):
        m = KMeans(k=k, seed=SEMENTE, featuresCol="features").fit(escalado)
        silhueta = ClusteringEvaluator(featuresCol="features").evaluate(m.transform(escalado))
        print(f"  K={k}: silhueta {silhueta:.4f}")
        if melhor is None or silhueta > melhor[0]:
            melhor = (silhueta, k, m)
    silhueta, k, m = melhor
    print(f"  K escolhido: {k} (silhueta {silhueta:.4f})")
    atribuido = m.transform(escalado).withColumn("grupo", F.col("prediction") + 1).cache()

    medias = [F.round(F.avg(c), 4).alias(c) for c in colunas]
    perfil = (atribuido.groupBy("grupo").agg(F.count("*").alias("visitantes"), *medias)
              .withColumn("tipo", F.lit("perfil")).withColumn("user_pseudo_id", F.lit(None).cast("string")))
    por_visitante = (atribuido.select("user_pseudo_id", "grupo", *colunas)
                     .withColumn("tipo", F.lit("visitante")).withColumn("visitantes", F.lit(None).cast("long")))
    cols = ["tipo", "grupo", "user_pseudo_id", "visitantes", *colunas]
    return por_visitante.select(*cols).unionByName(perfil.select(*cols)).orderBy("tipo", "grupo", "user_pseudo_id")


def main() -> None:
    spark = criar_sessao("gold_ml")
    eventos = ler_silver(spark).cache()

    propensao_df, metricas = propensao(spark, eventos)
    gravar_gold(propensao_df, "propensao_sessao")
    gravar_gold(metricas, "metricas_modelo")
    titulo("Gold: ML - segmentos (silhueta por K)")
    gravar_gold(segmentos(spark, eventos), "segmentos_visitantes")

    titulo("Gold: ML")
    mostrar(spark, "metricas_modelo", 30)
    mostrar(spark, "propensao_sessao", 5)
    perfis = spark.read.parquet("s3a://datalake-tracking/gold/segmentos_visitantes").filter("tipo = 'perfil'").orderBy("grupo")
    print("\n>> perfil médio de cada grupo")
    perfis.drop("tipo", "user_pseudo_id").show(truncate=False)
    spark.stop()


if __name__ == "__main__":
    main()
