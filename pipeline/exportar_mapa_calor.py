"""Job de exportação do mapa de calor (R11): gold -> site/mapa_calor.js.

    docker compose run --rm app python pipeline/exportar_mapa_calor.py

Lê as duas tabelas Gold pelo Spark (R1) e só no fim grava um arquivo comum, que o site carrega do disco.
As tabelas têm poucas dezenas de linhas, então o collect() é seguro aqui (nunca faça isso com a Silver).
`site/mapa_calor.js` é um resultado gerado: não entra no git.
"""

import json
from datetime import datetime, timezone

from comum import arquivo_do_projeto, caminho, criar_sessao, exigir_camada, titulo

COLUNAS_CLIQUES = ["pagina", "element_id", "cliques", "sessoes", "sessoes_na_pagina", "taxa_clique", "intensidade"]
COLUNAS_ROLAGEM = ["pagina", "marco", "sessoes_que_chegaram", "pct_sessoes"]


def main() -> None:
    spark = criar_sessao("exportar_mapa_calor")
    for tabela in ("mapa_calor_cliques", "mapa_calor_rolagem"):
        exigir_camada(spark, f"gold/{tabela}", "python pipeline/gold_mapa_calor.py")

    cliques = [l.asDict() for l in spark.read.parquet(caminho("gold/mapa_calor_cliques"))
               .select(*COLUNAS_CLIQUES).orderBy("pagina", "element_id").collect()]
    rolagem = [l.asDict() for l in spark.read.parquet(caminho("gold/mapa_calor_rolagem"))
               .select(*COLUNAS_ROLAGEM).orderBy("pagina", "marco").collect()]
    dados = {"gerado_em": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
             "cliques": cliques, "rolagem": rolagem}

    destino = arquivo_do_projeto("site/mapa_calor.js")
    texto = json.dumps(dados, ensure_ascii=False, default=float)
    temporario = destino.with_name(destino.name + ".tmp")           # grava inteiro e troca: o site nunca lê pela metade
    temporario.write_text("window.MAPA_CALOR = " + texto + ";\n", encoding="utf-8")
    temporario.replace(destino)

    titulo("Mapa de calor exportado")
    print(f"{destino}: {len(cliques)} linhas de cliques, {len(rolagem)} marcos de rolagem")
    spark.stop()


if __name__ == "__main__":
    main()
