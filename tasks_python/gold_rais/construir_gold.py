"""
Constrói a camada gold da RAIS: agregados de estoque prontos para o dashboard.

POR QUE A RAIS AO LADO DO CAGED
-------------------------------
As duas bases respondem perguntas diferentes, e é a diferença que interessa:

    CAGED  -> FLUXO. Quantos empregos foram criados neste mês.
    RAIS   -> ESTOQUE. Quantos empregos existem em 31 de dezembro.

Um saldo positivo no CAGED não diz se o mercado é grande, e um estoque grande
na RAIS não diz se está crescendo. Juntas: "o setor tem 1,27 milhão de
vínculos de TI e gerou X mil no ano" — que é a leitura que um trabalho sobre
mercado de trabalho precisa fazer.

A RAIS ainda permite três coisas que o CAGED não tem:

  1. REMUNERAÇÃO EM SALÁRIOS MÍNIMOS (`vl_remun_media_sm`). O salário nominal
     do CAGED não é comparável entre 2007 e 2025 sem deflacionar — e escolher
     deflator é decisão metodológica discutível. O múltiplo do mínimo já vem
     pronto na fonte e atravessa a série inteira.
  2. TEMPO DE EMPREGO. Permanência média no vínculo, que é a leitura de
     rotatividade. O CAGED registra a movimentação, não a duração.
  3. ESTABELECIMENTOS. Quantas empresas de TI existem e de que porte — o
     CAGED não tem cadastro de empresa.

O QUE É O ESTOQUE
-----------------
`vinculo_ativo_3112 = '1'` marca o vínculo ativo em 31/12. A RAIS traz também
os vínculos que existiram e terminaram durante o ano, e somar os dois inflaria
o estoque. Por isso as médias (remuneração, tempo, idade) são calculadas SÓ
sobre os ativos: incluir quem saiu misturaria o mercado do fim do ano com o de
qualquer mês anterior.

A contagem dos que NÃO estão ativos vira métrica própria (`desligados_no_ano`)
em vez de ser descartada — é o que permite ler rotatividade contra o estoque.

SOBRE A UF
----------
A RAIS não tem coluna de UF. O código do município é IBGE de 6 dígitos e a
descrição já vem prefixada com a sigla ('Df-Brasilia', 'Mt-Sinop'), então a UF
sai do prefixo. É mais direto que carregar uma tabela de-para de município só
para recuperar informação que já está no texto.

Uso:
    python -m gold_rais.construir_gold
    python -m gold_rais.construir_gold --tabela rais_estoque_anual
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

FONTE_VINC = f"s3://{BUCKET_SILVER_TI}/rais_vinc/**/*.parquet"
FONTE_ESTAB = f"s3://{BUCKET_SILVER_TI}/rais_estab/**/*.parquet"

COL_CNAE = "cnae_20_subclasse"
COL_CBO = "cbo_ocupacao_2002"

# Rotula cada vínculo nas duas lentes (setor por CNAE, ocupação por CBO). Igual
# ao CAGED: rotular em vez de filtrar mantém o mercado geral como linha de base
# na mesma tabela — sem baseline, "TI paga 2,3x mais" não diz nada.
BASE_VINC = f"""
    SELECT *, {esc.sql_classificacao(COL_CNAE, COL_CBO)}
    FROM read_parquet('{FONTE_VINC}', hive_partitioning=true)
"""

# O estabelecimento não tem ocupação — uma empresa não exerce CBO —, então só
# a lente de setor se aplica.
BASE_ESTAB = f"""
    SELECT *, {esc.sql_classificacao(COL_CNAE, None)}
    FROM read_parquet('{FONTE_ESTAB}', hive_partitioning=true)
"""

ATIVO = "vinculo_ativo_3112 = '1'"
UF = "upper(split_part(municipio_descricao, '-', 1))"

# Teto de plausibilidade da remuneração, em salários mínimos.
#
# POR QUE ISTO PRECISA EXISTIR
# ----------------------------
# A partir de 2023 a fonte traz registros com a conversão para salário mínimo
# INVERTIDA — multiplicou em vez de dividir. Um exemplo real de 2023:
#
#     vl_remun_media_sm = 147.817,11   vl_remun_media_nom = R$ 112,43
#
# A razão entre os dois é 1.315, que é o salário mínimo daquele ano. São 222
# registros em 2023, 205 em 2024 e 94 em 2025 — 0,018% dos vínculos. E eles
# sozinhos respondem por 35% de toda a massa salarial do ano.
#
# O efeito na média é devastador e silencioso: a remuneração média salta de
# 6,17 para 8,84 SM entre 2022 e 2023, o que se leria como uma valorização de
# 43% do trabalho em TI. Não houve valorização nenhuma — a MEDIANA fica em
# 3,60, praticamente igual à de 2022 (3,63).
#
# O teto é generoso de propósito: em 2022, com o layout antigo e sem o defeito,
# o maior valor da série inteira é 149,90 SM. Cortar em 500 preserva qualquer
# salário legítimo com folga de mais de 3x e remove só o que é aritmeticamente
# impossível.
#
# A conferência que expõe isso é dividir o nominal pelo SM: o "mínimo
# implícito" bate com o salário mínimo real até 2022 (R$ 1.000 em 2019, R$
# 1.102 em 2021) e desaba para R$ 843 em 2023.
LIMITE_REMUN_SM = 500

REMUN_VALIDA = f"vl_remun_media_sm > 0 AND vl_remun_media_sm <= {LIMITE_REMUN_SM}"


def _so_ativo(expr: str, positivo: bool = False) -> str:
    """Média calculada apenas sobre o estoque de 31/12, ignorando zeros."""
    guarda = f" AND {expr} > 0" if positivo else ""
    return f"CASE WHEN {ATIVO}{guarda} THEN {expr} END"


def _remuneracao(expr: str) -> str:
    """
    Remuneração do estoque, descartando o registro implausível.

    O descarte vale para as DUAS colunas de remuneração, não só para a que
    está corrompida: se a conversão de um registro está invertida, o registro
    inteiro é suspeito, e mantê-lo na média nominal seria escolher confiar em
    metade de um dado que já se sabe errado.
    """
    return f"CASE WHEN {ATIVO} AND {REMUN_VALIDA} THEN {expr} END"


# A mediana entra ao lado da média de propósito: distribuição salarial é
# assimétrica, e a distância entre as duas É o achado. Média puxada por poucos
# salários altos esconde o que a maioria ganha.
METRICAS = f"""
        count(*)                                   AS vinculos_no_ano,
        count(*) FILTER (WHERE {ATIVO})            AS estoque_3112,
        count(*) FILTER (WHERE NOT {ATIVO})        AS desligados_no_ano,
        round(avg({_remuneracao('vl_remun_media_sm')}), 2)     AS remuneracao_sm,
        round(median({_remuneracao('vl_remun_media_sm')}), 2)  AS remuneracao_sm_mediana,
        round(avg({_remuneracao('vl_remun_media_nom')}), 2)    AS remuneracao_nominal,
        -- Quantos registros o teto de plausibilidade descartou. Fica NA
        -- TABELA, não só no comentário: quem consultar a gold precisa poder
        -- ver que houve descarte e conferir que ele é desprezível no estoque
        -- (0,018%) ainda que enorme na massa salarial (35%).
        count(*) FILTER (WHERE {ATIVO} AND vl_remun_media_sm > {LIMITE_REMUN_SM})
                                                   AS remun_implausivel,
        round(avg({_so_ativo('tempo_emprego')}), 1)            AS tempo_emprego_meses,
        round(avg({_so_ativo('idade')}), 1)                    AS idade_media,
        round(avg({_so_ativo('qtd_hora_contr', True)}), 1)     AS horas_semanais
"""

AGREGADOS = {
    # A série-título: estoque de TI ano a ano, nas duas lentes.
    "rais_estoque_anual": f"""
        SELECT ano_particao AS ano, setor_ti, ocupacao_ti, {METRICAS}
        FROM ({BASE_VINC})
        GROUP BY 1, 2, 3 ORDER BY 1
    """,
    "rais_estoque_uf": f"""
        SELECT ano_particao AS ano, {UF} AS uf, setor_ti, ocupacao_ti, {METRICAS}
        FROM ({BASE_VINC})
        WHERE municipio_descricao IS NOT NULL
        GROUP BY 1, 2, 3, 4 ORDER BY 1, 2
    """,
    # Áreas de atuação (ver escopo_tecnologia.AREAS_TI): desenvolvimento,
    # infraestrutura, dados, suporte. Só faz sentido sobre quem TEM ocupação de
    # TI — a recepcionista da software house não pertence a nenhuma área.
    "rais_estoque_area": f"""
        SELECT ano_particao AS ano,
               {esc.sql_area_ti(COL_CBO)} AS area_ti,
               setor_ti, {METRICAS}
        FROM ({BASE_VINC})
        WHERE ocupacao_ti
        GROUP BY 1, 2, 3 ORDER BY 1, 2
    """,
    # Sexo, raça e escolaridade juntos permitem ler o hiato salarial DENTRO de
    # tecnologia, controlando por escolaridade — a comparação que separa
    # "ganham menos porque estudaram menos" de "ganham menos no mesmo nível".
    "rais_estoque_perfil": f"""
        SELECT ano_particao AS ano,
               sexo_trabalhador_descricao AS sexo,
               raca_cor_descricao AS raca_cor,
               escolaridade_apos_2005_descricao AS escolaridade,
               setor_ti, ocupacao_ti, {METRICAS}
        FROM ({BASE_VINC})
        GROUP BY 1, 2, 3, 4, 5, 6 ORDER BY 1
    """,
    "rais_remuneracao_ocupacao": f"""
        SELECT ano_particao AS ano,
               cbo_ocupacao_2002_descricao AS ocupacao,
               setor_ti, {METRICAS}
        FROM ({BASE_VINC})
        WHERE cbo_ocupacao_2002_descricao IS NOT NULL
        GROUP BY 1, 2, 3
        HAVING count(*) FILTER (WHERE vinculo_ativo_3112 = '1') >= 200
        ORDER BY 1, 5 DESC
    """,
    "rais_estoque_municipio": f"""
        SELECT ano_particao AS ano, {UF} AS uf,
               municipio_descricao AS municipio, ocupacao_ti, {METRICAS}
        FROM ({BASE_VINC})
        WHERE municipio_descricao IS NOT NULL
        GROUP BY 1, 2, 3, 4 ORDER BY 1, 5 DESC
    """,
    # O achado central, agora sobre o estoque: quanto do trabalho de TI
    # acontece FORA do setor de TI — e quanto ele paga em cada lado.
    "rais_setor_vs_ocupacao": f"""
        SELECT ano_particao AS ano, setor_ti, ocupacao_ti,
               cnae_20_subclasse_descricao AS setor_empresa, {METRICAS}
        FROM ({BASE_VINC})
        WHERE ocupacao_ti OR setor_ti
        GROUP BY 1, 2, 3, 4 ORDER BY 1, 5 DESC
    """,
    # Estabelecimentos: o que o CAGED não tem. Quantas empresas de TI existem,
    # de que porte, e quantos vínculos elas concentram.
    "rais_estabelecimentos": f"""
        SELECT ano_particao AS ano, {UF} AS uf,
               tamanho_estabelecimento_descricao AS porte,
               natureza_juridica_descricao AS natureza_juridica,
               setor_ti,
               count(*)                           AS estabelecimentos,
               sum(qtd_vinculos_ativos)           AS vinculos_ativos,
               sum(qtd_vinculos_clt)              AS vinculos_clt,
               sum(qtd_vinculos_estatutarios)     AS vinculos_estatutarios,
               round(avg(qtd_vinculos_ativos), 1) AS media_vinculos_por_estab
        FROM ({BASE_ESTAB})
        WHERE municipio_descricao IS NOT NULL
        GROUP BY 1, 2, 3, 4, 5 ORDER BY 1, 6 DESC
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
    p = argparse.ArgumentParser(description="Constrói a camada gold da RAIS.")
    p.add_argument("--tabela", nargs="+", choices=list(AGREGADOS), default=list(AGREGADOS))
    p.add_argument("--threads", type=int, default=4,
                   help="A RAIS de TI tem 23,8 milhões de vínculos e as médias "
                        "com FILTER seguram muita memória por thread.")
    args = p.parse_args()

    con = conectar_duckdb()
    con.execute(f"SET threads={args.threads}")
    print("=" * 70)
    print("  GOLD — estoque de emprego em tecnologia (RAIS)")
    print("=" * 70)

    ok = sum(construir(con, nome) for nome in args.tabela)
    print(f"\n🏁 {ok}/{len(args.tabela)} agregado(s) construído(s).")
    return 0 if ok == len(args.tabela) else 2


if __name__ == "__main__":
    sys.exit(main())
