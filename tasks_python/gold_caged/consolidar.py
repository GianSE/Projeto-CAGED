"""
Consolida a silver particionada em um parquet POR TABELA E POR ANO, para
publicar na web.

POR QUE UM ARQUIVO POR ANO, E NÃO UM POR TABELA
------------------------------------------------
A silver está em ~350 arquivos (um por competência) — ótimo para o lake
local, mas não serve para servir por HTTPS: sobre HTTP puro não existe
listagem de diretório, então `read_parquet('https://.../**/*.parquet')` não
tem como descobrir os arquivos sozinho.

Um arquivo por tabela (a versão anterior deste módulo) resolvia isso, mas
amarrava publicar um ano novo a reescrever e reenviar a série inteira — e
amarrava corrigir UM ano com problema (como o buraco de 2022 na RAIS, achado
na auditoria) a reconstruir tudo de novo. Um arquivo por ano resolve as duas
pontas: o leitor sabe o nome de cada arquivo sem listar diretório (a lista
de anos vem da API do Hub, ver `dashboard/dados.py`), e reprocessar um ano
só toca o arquivo daquele ano.

ORDENAÇÃO IMPORTA
-----------------
Dentro de cada ano, grava ordenado por competência: assim cada row group
cobre uma faixa estreita de datas, e o DuckDB descarta blocos inteiros ao
filtrar por período sem baixá-los.

Uso:
    python -m gold_caged.consolidar
    python -m gold_caged.consolidar --tabela caged_mov caged_old
    python -m gold_caged.consolidar --ano-inicio 2022 --ano-fim 2022
    # gera publicacao/detalhado/<tabela>_<ano>.parquet
"""
import argparse
import sys
from pathlib import Path

from extracao_ftp.config_extracao import (
    BUCKET_SILVER_TI,
    PARQUET_COMPRESSION,
    PARQUET_COMPRESSION_LEVEL,
    conectar_duckdb,
)

DIR_SAIDA = Path(__file__).resolve().parents[2] / "publicacao" / "detalhado"

# Coluna de tempo de cada tabela, usada para ordenar dentro do ano (ver docstring).
COLUNA_TEMPO = {
    "caged_mov": "competenciamov_data",
    "caged_for": "competenciamov_data",
    "caged_exc": "competenciamov_data",
    "caged_old": "competencia_declarada_data",
    "caged_ajustes": "competencia_movimentacao_data",
}

# Row group menor que o padrão (~122k): com range request, o cliente baixa o
# row group inteiro para ler qualquer linha dele. Blocos menores = menos
# tráfego desperdiçado por consulta.
TAMANHO_ROW_GROUP = 50_000


def anos_na_silver(con, tabela: str) -> list[int]:
    origem = f"s3://{BUCKET_SILVER_TI}/{tabela}/**/*.parquet"
    try:
        return [r[0] for r in con.execute(
            f"SELECT DISTINCT ano_particao FROM read_parquet('{origem}', union_by_name=true) "
            f"ORDER BY 1"
        ).fetchall()]
    except Exception:
        return []


def consolidar_ano(con, tabela: str, ano: int) -> bool:
    origem = f"s3://{BUCKET_SILVER_TI}/{tabela}/ano_particao={ano}/**/*.parquet"
    destino = DIR_SAIDA / f"{tabela}_{ano}.parquet"

    try:
        # `union_by_name` porque a série NÃO tem as mesmas colunas do começo ao
        # fim, e isso é do dado, não defeito: o CAGED antigo trazia bairros de
        # São Paulo, Rio e Fortaleza até 2007, e ganhou `ind_trab_intermitente` e
        # `ind_trab_parcial` em 2018, com a reforma trabalhista. Um `SELECT *` sem
        # isso casa coluna por POSIÇÃO e falha no primeiro arquivo de schema
        # diferente. O arquivo do ano carrega a união, com nulo nas colunas que
        # aquele ano não tinha — que é a informação correta.
        colunas = [r[0] for r in con.execute(
            f"DESCRIBE SELECT * FROM read_parquet('{origem}', union_by_name=true) LIMIT 0"
        ).fetchall()]
    except Exception:
        print(f"   ⏭️  {tabela} {ano}: sem dados na silver, pulando")
        return False

    coluna_tempo = COLUNA_TEMPO.get(tabela)
    ordem = f"ORDER BY {coluna_tempo}" if coluna_tempo in colunas else ""

    try:
        con.execute(f"""
            COPY (SELECT * FROM read_parquet('{origem}', union_by_name=true) {ordem})
            TO '{destino.as_posix()}' (
                FORMAT PARQUET,
                COMPRESSION '{PARQUET_COMPRESSION.upper()}',
                COMPRESSION_LEVEL {PARQUET_COMPRESSION_LEVEL},
                ROW_GROUP_SIZE {TAMANHO_ROW_GROUP}
            );
        """)
    except Exception as e:
        print(f"   ❌ {tabela} {ano}: {str(e)[:200]}")
        return False

    linhas = con.execute(
        f"SELECT count(*) FROM read_parquet('{destino.as_posix()}')"
    ).fetchone()[0]
    tam = destino.stat().st_size
    print(f"   ✅ {tabela:<15} {ano}  {linhas:>9,} linhas  {tam / 1e6:>6.1f} MB")
    return True


def consolidar(con, tabela: str, ano_inicio: int = 0, ano_fim: int = 9999) -> int:
    anos = [a for a in anos_na_silver(con, tabela) if ano_inicio <= a <= ano_fim]
    if not anos:
        print(f"   ⏭️  {tabela}: sem anos na silver no recorte pedido")
        return 0
    return sum(consolidar_ano(con, tabela, ano) for ano in anos)


def _argumentos() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--tabela", nargs="+", choices=list(COLUNA_TEMPO),
                   default=list(COLUNA_TEMPO))
    p.add_argument("--ano-inicio", type=int, default=0)
    p.add_argument("--ano-fim", type=int, default=9999)
    return p.parse_args()


def main() -> int:
    args = _argumentos()
    DIR_SAIDA.mkdir(parents=True, exist_ok=True)
    con = conectar_duckdb()

    print(f"📦 Consolidando a silver em {DIR_SAIDA} (por ano)\n")
    ok = sum(consolidar(con, t, args.ano_inicio, args.ano_fim) for t in args.tabela)

    total = sum(f.stat().st_size for f in DIR_SAIDA.glob("*.parquet"))
    print(f"\n🏁 {ok} arquivo(s) de ano gerado(s) · {total / 1e6:.1f} MB no total em {DIR_SAIDA}")
    print("\nPróximo passo — publicar no Hugging Face:")
    print("   set HF_TOKEN=hf_xxxxx")
    print("   python -m gold_caged.publicar_hf --repo SEU_USUARIO/caged-tecnologia")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
