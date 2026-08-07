"""
Consolida a silver particionada em um parquet por tabela, para publicar na web.

POR QUE CONSOLIDAR
------------------
A silver está em ~350 arquivos (um por competência). Isso é ótimo para o lake
local, mas não serve para servir por HTTPS: sobre HTTP puro não existe
listagem de diretório, então `read_parquet('https://.../**/*.parquet')` não
tem como descobrir os arquivos. Um arquivo por tabela resolve — e não custa
performance, porque o DuckDB continua pulando blocos via estatísticas de row
group.

ORDENAÇÃO IMPORTA
-----------------
Grava ordenado por competência: assim cada row group cobre uma faixa estreita
de datas, e o DuckDB descarta blocos inteiros ao filtrar por período sem
baixá-los. Sem ordenar, as datas ficariam espalhadas por todos os blocos e
qualquer filtro leria o arquivo inteiro — justamente o que se quer evitar ao
servir por range request.

Uso:
    python -m gold_caged.consolidar
    # gera publicacao/detalhado/*.parquet para subir ao Supabase Storage
"""
import sys
from pathlib import Path

from extracao_ftp.config_extracao import (
    BUCKET_SILVER,
    PARQUET_COMPRESSION,
    PARQUET_COMPRESSION_LEVEL,
    conectar_duckdb,
)

DIR_SAIDA = Path(__file__).resolve().parents[2] / "publicacao" / "detalhado"

# Coluna de tempo de cada tabela, usada para ordenar (ver docstring).
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


def consolidar(con, tabela: str) -> bool:
    origem = f"s3://{BUCKET_SILVER}/{tabela}/**/*.parquet"
    destino = DIR_SAIDA / f"{tabela}.parquet"

    try:
        colunas = [r[0] for r in con.execute(
            f"DESCRIBE SELECT * FROM read_parquet('{origem}') LIMIT 0"
        ).fetchall()]
    except Exception:
        print(f"   ⏭️  {tabela}: sem dados na silver, pulando")
        return False

    coluna_tempo = COLUNA_TEMPO.get(tabela)
    ordem = f"ORDER BY {coluna_tempo}" if coluna_tempo in colunas else ""
    if not ordem:
        print(f"   ⚠️  {tabela}: sem coluna de tempo conhecida — gravando sem ordenação")

    try:
        con.execute(f"""
            COPY (SELECT * FROM read_parquet('{origem}') {ordem})
            TO '{destino.as_posix()}' (
                FORMAT PARQUET,
                COMPRESSION '{PARQUET_COMPRESSION.upper()}',
                COMPRESSION_LEVEL {PARQUET_COMPRESSION_LEVEL},
                ROW_GROUP_SIZE {TAMANHO_ROW_GROUP}
            );
        """)
    except Exception as e:
        print(f"   ❌ {tabela}: {str(e)[:200]}")
        return False

    linhas = con.execute(
        f"SELECT count(*) FROM read_parquet('{destino.as_posix()}')"
    ).fetchone()[0]
    tam = destino.stat().st_size
    print(f"   ✅ {tabela:<15} {linhas:>10,} linhas  {tam / 1e6:>7.1f} MB")
    return True


def main() -> int:
    DIR_SAIDA.mkdir(parents=True, exist_ok=True)
    con = conectar_duckdb()

    print(f"📦 Consolidando a silver em {DIR_SAIDA}\n")
    tabelas = sys.argv[1:] or list(COLUNA_TEMPO)
    ok = sum(consolidar(con, t) for t in tabelas)

    total = sum(f.stat().st_size for f in DIR_SAIDA.glob("*.parquet"))
    print(f"\n🏁 {ok} tabela(s) · {total / 1e6:.1f} MB no total")
    print("\nPróximo passo — publicar no Hugging Face:")
    print("   set HF_TOKEN=hf_xxxxx")
    print("   python -m gold_caged.publicar_hf --repo SEU_USUARIO/caged-tecnologia")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
