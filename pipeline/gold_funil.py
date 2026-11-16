"""Job Gold do funil: silver -> gold/funil_compra e gold/funil_lead.

    docker compose run --rm app python pipeline/gold_funil.py

Responde à pergunta 1 do enunciado: em que etapa a jornada de compra perde mais gente, por plano e por canal?

- funil_compra: sessões que atingiram cada etapa de view_item_list -> select_item -> add_to_cart ->
  view_cart -> begin_checkout -> add_payment_info -> purchase, com a conversão sobre a etapa anterior
  e sobre o topo. Uma linha por plano × canal, mais as linhas de total (plano ou canal = '(todos)').
  Uma sessão conta numa etapa se teve aquele evento em qualquer momento, sem exigir as anteriores (5.4,
  item 4). view_item não entra na sequência: vira a coluna taxa_view_item (sessões com view_item ÷
  sessões com select_item).
- funil_lead: page_view da landing -> scroll >= 50% -> generate_lead, por canal (e total).
"""

from pyspark.sql import functions as F

from _gold import ETAPAS_FUNIL, agregar_cubo, gravar_gold, ler_silver, mostrar, sessoes_base
from comum import criar_sessao, titulo


def taxa(numerador: str, denominador: str) -> F.Column:
    """numerador ÷ denominador a 4 casas; nulo se o denominador for zero (try_divide, modo ANSI)."""
    return F.round(F.try_divide(F.col(numerador), F.col(denominador)), 4)


def main() -> None:
    spark = criar_sessao("gold_funil")
    sessoes = sessoes_base(ler_silver(spark)).cache()

    # funil_compra: cubo plano × canal. O cubo gera as linhas por plano, por canal e o total geral.
    contagens = [F.sum(f"tem_{etapa}").alias(f"sessoes_{etapa}") for etapa in ETAPAS_FUNIL]
    funil = agregar_cubo(sessoes, ["plano", "canal"], *contagens,
                         F.sum("tem_view_item").alias("sessoes_view_item"))
    topo = f"sessoes_{ETAPAS_FUNIL[0]}"
    for anterior, etapa in zip(ETAPAS_FUNIL, ETAPAS_FUNIL[1:]):
        funil = (funil
                 .withColumn(f"conv_anterior_{etapa}", taxa(f"sessoes_{etapa}", f"sessoes_{anterior}"))
                 .withColumn(f"conv_topo_{etapa}", taxa(f"sessoes_{etapa}", topo)))
    funil = funil.withColumn("taxa_view_item", taxa("sessoes_view_item", "sessoes_select_item"))
    colunas = (["plano", "canal"] + [f"sessoes_{e}" for e in ETAPAS_FUNIL]
               + [c for e in ETAPAS_FUNIL[1:] for c in (f"conv_anterior_{e}", f"conv_topo_{e}")]
               + ["sessoes_view_item", "taxa_view_item"])
    gravar_gold(funil.select(*colunas), "funil_compra")

    # funil_lead: só a landing ("/") conta; o cubo dá uma linha por canal e o total.
    lead = agregar_cubo(
        sessoes, ["canal"],
        F.sum("tem_page_view_landing").alias("sessoes_page_view_landing"),
        F.sum("tem_scroll_50_landing").alias("sessoes_scroll_50"),
        F.sum("tem_generate_lead").alias("sessoes_lead"),
    )
    lead = (lead
            .withColumn("conv_scroll_50", taxa("sessoes_scroll_50", "sessoes_page_view_landing"))
            .withColumn("conv_lead_sobre_scroll", taxa("sessoes_lead", "sessoes_scroll_50"))
            .withColumn("conv_lead_sobre_page_view", taxa("sessoes_lead", "sessoes_page_view_landing")))
    gravar_gold(lead, "funil_lead")

    titulo("Gold: funil")
    mostrar(spark, "funil_compra", 60)
    mostrar(spark, "funil_lead")
    spark.stop()


if __name__ == "__main__":
    main()
