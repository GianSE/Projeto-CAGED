"""
Gera UM parquet com as duas gerações do CAGED harmonizadas.

POR QUE UNIFICAR
----------------
Publicar `caged_mov.parquet` e `caged_old.parquet` separados obriga quem
consome a descobrir sozinho que `saldomovimentacao` e `saldo_mov` são a mesma
coisa, que a competência muda de nome, e que o setor está numa taxonomia em
uma base e em outra na outra. Um arquivo só, com nomes estáveis, torna a
série 2007–2026 utilizável direto.

O SETOR TAMBÉM É UNIFICADO
--------------------------
O CAGED antigo não traz a seção CNAE pronta — traz o subsetor IBGE, que é
outra taxonomia. Mas traz `cnae_20_subclas` com os MESMOS códigos CNAE 2.0 do
Novo CAGED, e a seção é determinada pela divisão (dois primeiros dígitos).
Derivando a seção (ver cnae_secao.py), a análise setorial deixa de ficar
presa ao período recente e passa a cobrir os 20 anos.

A derivação foi validada contra a seção oficial do Novo CAGED: bate em 100%
dos casos (as únicas diferenças eram de grafia do próprio dicionário do MTE,
agora replicada).

Uso:
    python -m gold_caged.unificar
"""
import sys
from pathlib import Path

from extracao_ftp.config_extracao import (
    BUCKET_SILVER,
    PARQUET_COMPRESSION,
    PARQUET_COMPRESSION_LEVEL,
    conectar_duckdb,
)
from gold_caged.cnae_secao import sql_secao

DIR_SAIDA = Path(__file__).resolve().parents[2] / "publicacao" / "detalhado"
DESTINO = DIR_SAIDA / "caged_ti.parquet"

TAMANHO_ROW_GROUP = 50_000


def sql_unificado() -> str:
    mov = f"s3://{BUCKET_SILVER}/caged_mov/**/*.parquet"
    old = f"s3://{BUCKET_SILVER}/caged_old/**/*.parquet"

    return f"""
        SELECT
            competenciamov_data                AS competencia,
            ano_particao                       AS ano,
            mes_particao                       AS mes,
            uf_descricao                       AS uf,
            municipio_descricao                AS municipio,
            secao_descricao                    AS setor,
            subclasse                          AS cnae_subclasse,
            subclasse_descricao                AS cnae_subclasse_descricao,
            cbo2002ocupacao                    AS cbo,
            cbo2002ocupacao_descricao          AS ocupacao,
            sexo_descricao                     AS sexo,
            racacor_descricao                  AS raca_cor,
            graudeinstrucao_descricao          AS escolaridade,
            idade,
            saldomovimentacao                  AS saldo,
            salario                            AS salario,
            tipomovimentacao_descricao         AS tipo_movimentacao,
            'Novo CAGED'                       AS geracao
        FROM read_parquet('{mov}')

        UNION ALL BY NAME

        SELECT
            competencia_declarada_data         AS competencia,
            ano_particao                       AS ano,
            mes_particao                       AS mes,
            uf_descricao                       AS uf,
            municipio_descricao                AS municipio,
            {sql_secao("cnae_20_subclas")}     AS setor,
            cnae_20_subclas                    AS cnae_subclasse,
            cnae_20_subclas_descricao          AS cnae_subclasse_descricao,
            cbo_2002_ocupacao                  AS cbo,
            cbo_2002_ocupacao_descricao        AS ocupacao,
            sexo_descricao                     AS sexo,
            raca_cor_descricao                 AS raca_cor,
            grau_instrucao_descricao           AS escolaridade,
            idade,
            saldo_mov                          AS saldo,
            salario_mensal                     AS salario,
            tipo_mov_desagregado_descricao     AS tipo_movimentacao,
            'CAGED antigo'                     AS geracao
        FROM read_parquet('{old}')
    """


def main() -> int:
    DIR_SAIDA.mkdir(parents=True, exist_ok=True)
    con = conectar_duckdb()

    print("📦 Unificando as duas gerações do CAGED\n")
    con.execute(f"""
        COPY (SELECT * FROM ({sql_unificado()}) ORDER BY competencia)
        TO '{DESTINO.as_posix()}' (
            FORMAT PARQUET,
            COMPRESSION '{PARQUET_COMPRESSION.upper()}',
            COMPRESSION_LEVEL {PARQUET_COMPRESSION_LEVEL},
            ROW_GROUP_SIZE {TAMANHO_ROW_GROUP}
        );
    """)

    linhas, ini, fim, setores = con.execute(f"""
        SELECT count(*), min(ano), max(ano), count(DISTINCT setor)
        FROM read_parquet('{DESTINO.as_posix()}')
    """).fetchone()

    print(f"   ✅ {linhas:,} linhas · {ini}–{fim} · {setores} setores")
    print(f"   📁 {DESTINO.stat().st_size / 1e6:.1f} MB -> {DESTINO}")

    # Conferência que importa: a seção derivada precisa existir nos dois lados,
    # senão a série setorial teria um buraco justamente no período antigo.
    print("\n   Cobertura de setor por geração:")
    for ger, tot, com in con.execute(f"""
        SELECT geracao, count(*), count(setor)
        FROM read_parquet('{DESTINO.as_posix()}') GROUP BY 1 ORDER BY 1
    """).fetchall():
        print(f"      {ger:<14} {com / tot * 100:5.1f}% com setor")

    return 0


if __name__ == "__main__":
    sys.exit(main())
