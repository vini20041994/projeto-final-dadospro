"""Roda os oito jobs do pipeline em sequência (R3): Bronze, Silver, cinco Golds e a exportação do mapa de calor.

    docker compose run --rm app python pipeline/rodar_tudo.py

Reconstrói o datalake inteiro a partir da landing que já existe. Este script NÃO gera dados: se a landing
estiver vazia, o job Bronze para e diz para rodar o gerador antes.

Cada job roda num processo próprio (como `python pipeline/<job>.py`), então cada um continua executável
sozinho. Se um job falhar, a sequência para ali, mostra qual foi e sai com o mesmo código de erro dele.
"""

import subprocess
import sys
import time
from pathlib import Path

PIPELINE = Path(__file__).resolve().parent
JOBS = [
    "bronze",
    "silver",
    "gold_funil",
    "gold_engajamento",
    "gold_receita",
    "gold_ml",
    "gold_mapa_calor",
    "exportar_mapa_calor",
]


def main() -> None:
    inicio = time.time()
    for n, job in enumerate(JOBS, start=1):
        print(f"\n[{n}/{len(JOBS)}] {job}", flush=True)
        t0 = time.time()
        codigo = subprocess.run([sys.executable, str(PIPELINE / f"{job}.py")]).returncode
        if codigo != 0:
            raise SystemExit(f"\nO job '{job}' falhou (código {codigo}). A sequência parou aqui.")
        print(f"[{n}/{len(JOBS)}] {job} ok em {time.time() - t0:.0f}s", flush=True)
    print(f"\nPipeline completo em {time.time() - inicio:.0f}s.")


if __name__ == "__main__":
    main()
