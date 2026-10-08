"""
Varre o FTP do MTE inteiro, compara com o que já está publicado no Hugging
Face e devolve a lista de (tabela, ano) que têm arquivo novo para processar.

POR QUE CATÁLOGO INTEIRO, SEMPRE
----------------------------------
A checagem é só comparação de NOME de arquivo (metadado), não download de
dado — por isso olhar os 20+ anos custa quase o mesmo que olhar só os 2
últimos. E olhar só os últimos anos deixaria passar correções tardias: o
MTE publica `caged_for`/`caged_exc`/`caged_ajustes` (declarações fora do
prazo) para competências antigas o tempo todo, não só para o período
corrente. O processamento (caro) é que fica restrito aos anos com novidade
real — a checagem (barata) cobre tudo sempre.

SAÍDA
-----
Imprime um JSON de volta pro workflow via `$GITHUB_OUTPUT`:
    {"tem_novidade": true, "anos": [{"tabela": "caged_mov", "ano": 2026}, ...]}

Uso:
    python -m extracao_ftp.verificar_novidades
    python -m extracao_ftp.verificar_novidades --dataset novo_caged rais
"""
import argparse
import json
import os
import sys

from extracao_ftp import nuvem
from extracao_ftp.catalogo import DATASETS, descobrir
from extracao_ftp.estado import EstadoLake
from extracao_ftp.ftp_utils import ClienteFTP


def _argumentos():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--dataset", nargs="+", choices=DATASETS, default=list(DATASETS))
    p.add_argument("--ano-inicio", type=int, default=1985)
    p.add_argument("--ano-fim", type=int, default=2030)  # mesmo padrão de run_extracao.py
    return p.parse_args()


def main() -> int:
    if not nuvem.MODO_NUVEM:
        print("⚠️  MODO_NUVEM não está ligado — este script é pensado pro CI "
              "(verifica contra o Hugging Face, não contra o MinIO local).")

    args = _argumentos()
    cliente = ClienteFTP()
    estado = EstadoLake()

    print(f"🔎 Varrendo o FTP ({', '.join(args.dataset)}, "
          f"{args.ano_inicio}-{args.ano_fim})...")
    itens = descobrir(cliente, args.dataset, args.ano_inicio, args.ano_fim)
    print(f"   {len(itens)} arquivo(s) no catálogo do FTP")

    faltantes = [item for item in itens if not estado.ja_existe(item)]
    print(f"   {len(faltantes)} arquivo(s) ainda não publicado(s)")

    pares = sorted({(item.tabela, item.ano) for item in faltantes})
    saida = {
        "tem_novidade": bool(pares),
        "anos": [{"tabela": t, "ano": a} for t, a in pares],
    }

    print(json.dumps(saida, ensure_ascii=False, indent=2))

    github_output = os.getenv("GITHUB_OUTPUT")
    if github_output:
        with open(github_output, "a", encoding="utf-8") as f:
            f.write(f"tem_novidade={str(saida['tem_novidade']).lower()}\n")
            f.write(f"anos={json.dumps(saida['anos'])}\n")

    return 0


if __name__ == "__main__":
    sys.exit(main())
