"""
Silver RAIS TI — traduz e filtra pra TI o que já está em
Gianpedro/bronze-rais, publica em Gianpedro/silver-rais-ti.

Dois modos, um script:
    python silver-rais-ti.py --modo verificar [--ano-inicio N] [--ano-fim N]
    python silver-rais-ti.py --modo processar --tabela X --ano-inicio Y --ano-fim Y
"""
import argparse
import json
import os
import sys
import time
from pathlib import Path

_AQUI = Path(__file__).resolve().parent
sys.path.insert(0, str(_AQUI))
sys.path.insert(0, str(_AQUI.parent))

from silver_rais.construir_silver import construir  # noqa: E402
from silver_rais import mapeamento as mp  # noqa: E402
from _utils import nuvem, telegram  # noqa: E402


def _argumentos():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--modo", choices=("verificar", "processar"), required=True)
    p.add_argument("--tabela", nargs="+", default=list(mp.TABELAS_RAIS))
    p.add_argument("--ano-inicio", type=int, default=0)
    p.add_argument("--ano-fim", type=int, default=9999)
    return p.parse_args()


def modo_verificar(args) -> int:
    ano_inicio = args.ano_inicio or (time.localtime().tm_year - 1)
    ano_fim = args.ano_fim if args.ano_fim != 9999 else time.localtime().tm_year

    pares = []
    for tabela in mp.TABELAS_RAIS:
        bronze = nuvem.anos_no_repo(nuvem.repo_bronze(tabela), tabela, "ano")
        silver = nuvem.anos_no_repo(nuvem.repo_silver_ti(tabela), tabela, "ano_particao")
        faltam = sorted(a for a in bronze - silver if ano_inicio <= a <= ano_fim)
        print(f"   {tabela}: {len(bronze)} ano(s) na bronze, {len(silver)} já na silver, "
              f"{len(faltam)} pendente(s)")
        pares += [{"tabela": tabela, "ano": a} for a in faltam]

    saida = {"tem_novidade": bool(pares), "anos": pares}
    print(json.dumps(saida, ensure_ascii=False, indent=2))

    github_output = os.getenv("GITHUB_OUTPUT")
    if github_output:
        with open(github_output, "a", encoding="utf-8") as f:
            f.write(f"tem_novidade={str(saida['tem_novidade']).lower()}\n")
            f.write(f"anos={json.dumps(saida['anos'])}\n")

    if saida["tem_novidade"]:
        telegram.notificar("🔎 *Silver RAIS TI*: tem ano pendente de tradução, processando.")
    else:
        telegram.notificar("🔎 *Silver RAIS TI*: verificado, já em dia.")
    return 0


def modo_processar(args) -> int:
    con = nuvem.conectar_duckdb()
    sucesso = 0
    for tabela in args.tabela:
        if construir(con, tabela, args.ano_inicio, args.ano_fim):
            sucesso += 1

    icone = "🏁" if sucesso == len(args.tabela) else "⚠️"
    telegram.notificar(f"{icone} *Silver RAIS TI*: {sucesso}/{len(args.tabela)} tabela(s) "
                       f"({', '.join(args.tabela)} {args.ano_inicio})")
    return 0 if sucesso == len(args.tabela) else 2


def main() -> int:
    args = _argumentos()
    if args.modo == "verificar":
        return modo_verificar(args)
    return modo_processar(args)


if __name__ == "__main__":
    sys.exit(main())
