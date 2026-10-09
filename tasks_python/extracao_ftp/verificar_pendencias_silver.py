"""
Compara a bronze com a silver-ti já publicada e devolve os (tabela, ano)
que têm dado na bronze mas ainda não foram traduzidos.

POR QUE SÓ LISTAGEM, SEM LER DADO
-----------------------------------
Diferente de `verificar_novidades.py` (bronze vs FTP, que precisa ler
`caminho_fonte` por causa do _parteNN), aqui a decisão é por ANO inteiro: a
silver em modo nuvem sempre reprocessa o ano inteiro quando é acionada (ver
construir_silver.py), nunca por arquivo. Então "o ano está publicado na
silver" já é a resposta certa — não precisa olhar dentro do dado, só listar
os dois repositórios (duas chamadas de API, não dezenas) e comparar os
anos que aparecem no caminho (`ano=`/`ano_particao=`).

Uso:
    python -m extracao_ftp.verificar_pendencias_silver --tabela caged_mov caged_old
"""
import argparse
import json
import os
import re
import sys

from extracao_ftp import nuvem

TABELAS_CAGED = ("caged_mov", "caged_for", "caged_exc", "caged_old", "caged_ajustes")
TABELAS_RAIS = ("rais_vinc", "rais_estab")


def _anos_no_repo(repo: str, tabela: str, padrao: str) -> set[int]:
    try:
        arquivos = nuvem.arquivos_remotos(repo)
    except Exception as e:
        print(f"⚠️  não consegui listar {repo}: {e}")
        return set()
    anos = set()
    for caminho in arquivos:
        if not caminho.startswith(f"{tabela}/"):
            continue
        m = re.search(padrao, caminho)
        if m:
            anos.add(int(m.group(1)))
    return anos


def pendencias(tabela: str, ano_inicio: int, ano_fim: int) -> list[int]:
    bronze = _anos_no_repo(nuvem.repo_bronze(tabela), tabela, r"ano=(\d{4})")
    silver = _anos_no_repo(nuvem.repo_silver_ti(tabela), tabela, r"ano_particao=(\d{4})")
    faltam = sorted(a for a in bronze - silver if ano_inicio <= a <= ano_fim)
    print(f"   {tabela}: {len(bronze)} ano(s) na bronze, {len(silver)} já na silver, "
          f"{len(faltam)} pendente(s) no recorte pedido")
    return faltam


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--tabela", nargs="+", required=True)
    p.add_argument("--ano-inicio", type=int, default=0)
    p.add_argument("--ano-fim", type=int, default=9999)
    args = p.parse_args()

    pares = []
    for tabela in args.tabela:
        for ano in pendencias(tabela, args.ano_inicio, args.ano_fim):
            pares.append({"tabela": tabela, "ano": ano})

    saida = {"tem_novidade": bool(pares), "anos": pares}
    print(json.dumps(saida, ensure_ascii=False, indent=2))

    github_output = os.getenv("GITHUB_OUTPUT")
    if github_output:
        with open(github_output, "a", encoding="utf-8") as f:
            f.write(f"tem_novidade={str(saida['tem_novidade']).lower()}\n")
            f.write(f"anos={json.dumps(saida['anos'])}\n")

    return 0


if __name__ == "__main__":
    sys.exit(main())
