"""
Constrói a camada gold: agregados do CAGED prontos para o dashboard.

POR QUE ESTA CAMADA EXISTE
--------------------------
A silver tem ~725 milhões de linhas. Um dashboard que consulte a silver a
cada mudança de filtro varreria centenas de milhões de linhas no S3 por
clique — inutilizável. A gold pré-agrega nas dimensões que o dashboard
oferece, derrubando o volume para alguns milhares de linhas por tabela: o
dashboard passa a responder instantaneamente e a agregação pesada roda uma
vez só, aqui.

O QUE É O SALDO
---------------
No Novo CAGED cada linha é UMA movimentação, e `saldomovimentacao` vale
+1 para admissão e -1 para desligamento. Então:
    admissões     = count(*) filtrado por saldo = 1
    desligamentos = count(*) filtrado por saldo = -1
    saldo         = sum(saldomovimentacao)
Essa é a métrica-título do CAGED: saldo positivo = geração líquida de
empregos formais no período.

O salário médio é calculado SÓ sobre admissões — é o salário de contratação.
Misturar desligamentos distorceria (o salário no desligamento reflete o
histórico do vínculo, não o mercado atual). Zeros também são descartados:
aparecem em registros sem informação salarial e puxariam a média para baixo.

ESCOPO
------
Novo CAGED (2020+). O CAGED antigo (caged_old, 2007-2019) usa outra
taxonomia setorial (IBGE subsetor em vez de seção CNAE), então unir os dois
numa série de setor daria uma comparação falsa. As demais dimensões (UF,
município, sexo, raça/cor, escolaridade, CBO) são compatíveis e podem ser
unidas depois, num conjunto separado de tabelas históricas.

Uso:
    python -m gold_caged.construir_gold
    python -m gold_caged.construir_gold --tabela saldo_mensal saldo_uf
"""
import argparse
import sys
import time

from extracao_ftp.config_extracao import (
    BUCKET_GOLD,
    BUCKET_SILVER_TI,
    PARQUET_COMPRESSION,
    PARQUET_COMPRESSION_LEVEL,
    conectar_duckdb,
)
from gold_caged import escopo_tecnologia as esc

FONTE = f"s3://{BUCKET_SILVER_TI}/caged_mov/**/*.parquet"

# Cada linha da silver é rotulada nas duas lentes de tecnologia (setor por
# CNAE, ocupação por CBO — ver escopo_tecnologia.py). Rotular em vez de
# filtrar preserva o mercado geral como linha de base na mesma tabela: sem
# baseline, "TI cresceu 8%" não diz nada.
BASE_ROTULADA = f"""
    SELECT *, {esc.sql_classificacao()}
    FROM read_parquet('{FONTE}')
"""

# Admissão sem salário informado entra como 0 e afundaria a média.
SALARIO_ADMISSAO = "avg(CASE WHEN saldomovimentacao = 1 AND salario > 0 THEN salario END)"

METRICAS = f"""
        count(*) FILTER (WHERE saldomovimentacao = 1)  AS admissoes,
        count(*) FILTER (WHERE saldomovimentacao = -1) AS desligamentos,
        sum(saldomovimentacao)                          AS saldo,
        round({SALARIO_ADMISSAO}, 2)                    AS salario_medio_admissao,
        round(avg(CASE WHEN saldomovimentacao = 1 THEN idade END), 1) AS idade_media_admissao
"""

# Cada agregado escolhe as dimensões que o dashboard realmente filtra/exibe.
# Manter poucas dimensões por tabela é o que mantém a gold pequena: cruzar
# tudo numa tabela só multiplicaria a cardinalidade sem ninguém consultar.
# `setor_ti` e `ocupacao_ti` entram como DIMENSÃO em quase todo agregado: o
# dashboard escolhe a lente (ou o mercado todo) filtrando, sem reprocessar.
AGREGADOS = {
    "saldo_mensal": f"""
        SELECT competenciamov_data AS competencia, setor_ti, ocupacao_ti, {METRICAS}
        FROM ({BASE_ROTULADA})
        WHERE competenciamov_data IS NOT NULL
        GROUP BY 1, 2, 3 ORDER BY 1
    """,
    "saldo_uf": f"""
        SELECT competenciamov_data AS competencia,
               uf_descricao AS uf, regiao_descricao AS regiao,
               setor_ti, ocupacao_ti, {METRICAS}
        FROM ({BASE_ROTULADA})
        WHERE competenciamov_data IS NOT NULL AND uf_descricao IS NOT NULL
        GROUP BY 1, 2, 3, 4, 5 ORDER BY 1, 2
    """,
    "saldo_setor": f"""
        SELECT competenciamov_data AS competencia,
               secao_descricao AS setor, ocupacao_ti, {METRICAS}
        FROM ({BASE_ROTULADA})
        WHERE competenciamov_data IS NOT NULL AND secao_descricao IS NOT NULL
        GROUP BY 1, 2, 3 ORDER BY 1, 2
    """,
    "perfil_demografico": f"""
        SELECT competenciamov_data AS competencia,
               sexo_descricao AS sexo,
               racacor_descricao AS raca_cor,
               graudeinstrucao_descricao AS escolaridade,
               setor_ti, ocupacao_ti, {METRICAS}
        FROM ({BASE_ROTULADA})
        WHERE competenciamov_data IS NOT NULL
        GROUP BY 1, 2, 3, 4, 5, 6 ORDER BY 1
    """,
    "ocupacoes": f"""
        SELECT ano_particao AS ano,
               cbo2002ocupacao_descricao AS ocupacao,
               setor_ti, ocupacao_ti, {METRICAS}
        FROM ({BASE_ROTULADA})
        WHERE cbo2002ocupacao_descricao IS NOT NULL
        GROUP BY 1, 2, 3, 4
        -- Corta a cauda longa: ocupações com pouquíssimo movimento no ano
        -- não aparecem em ranking nenhum e triplicariam a tabela.
        HAVING count(*) >= 200
        ORDER BY 1, 5 DESC
    """,
    "saldo_municipio": f"""
        SELECT ano_particao AS ano,
               uf_descricao AS uf, municipio_descricao AS municipio,
               ocupacao_ti, {METRICAS}
        FROM ({BASE_ROTULADA})
        WHERE municipio_descricao IS NOT NULL
        GROUP BY 1, 2, 3, 4 ORDER BY 1, 5 DESC
    """,
    # O achado central: quanto do trabalho de TI acontece FORA do setor de TI.
    # Cruzar as duas lentes numa tabela própria deixa a comparação explícita.
    "ti_setor_vs_ocupacao": f"""
        SELECT ano_particao AS ano, setor_ti, ocupacao_ti,
               secao_descricao AS setor_empresa, {METRICAS}
        FROM ({BASE_ROTULADA})
        WHERE ocupacao_ti OR setor_ti
        GROUP BY 1, 2, 3, 4 ORDER BY 1, 5 DESC
    """,
}


def construir(con, nome: str) -> bool:
    destino = f"s3://{BUCKET_GOLD}/{nome}.parquet"
    print(f"\n🔨 {nome}")
    inicio = time.time()
    try:
        con.execute(f"""
            COPY ({AGREGADOS[nome]}) TO '{destino}' (
                FORMAT PARQUET,
                COMPRESSION '{PARQUET_COMPRESSION.upper()}',
                COMPRESSION_LEVEL {PARQUET_COMPRESSION_LEVEL}
            );
        """)
        linhas = con.execute(f"SELECT count(*) FROM read_parquet('{destino}')").fetchone()[0]
        print(f"   ✅ {linhas:,} linhas em {time.time() - inicio:.0f}s -> {destino}")
        return True
    except Exception as e:
        print(f"   ❌ falhou: {str(e)[:300]}")
        return False


def main() -> int:
    p = argparse.ArgumentParser(description="Constrói a camada gold do CAGED.")
    p.add_argument("--tabela", nargs="+", choices=list(AGREGADOS), default=list(AGREGADOS))
    args = p.parse_args()

    con = conectar_duckdb()
    print("=" * 70)
    print("  GOLD — agregados do CAGED para o dashboard")
    print("=" * 70)

    ok = sum(construir(con, nome) for nome in args.tabela)
    print(f"\n🏁 {ok}/{len(args.tabela)} agregado(s) construído(s).")
    return 0 if ok == len(args.tabela) else 2


if __name__ == "__main__":
    sys.exit(main())
