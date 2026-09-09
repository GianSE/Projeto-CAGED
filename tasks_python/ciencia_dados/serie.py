"""
A série mensal do CAGED de tecnologia, montada uma vez e reaproveitada.

POR QUE UM MÓDULO SÓ PARA ISTO
------------------------------
Três coisas diferentes precisam da MESMA série: a previsão, o nowcast do
estoque e qualquer diagnóstico de quebra estrutural. Se cada uma montasse a
sua, bastaria uma divergir num detalhe — o filtro de data, a união das duas
gerações — para os resultados deixarem de conversar entre si, sem ninguém
perceber.

A EMENDA DE 2020
----------------
A série cruza as duas gerações do CAGED, e elas datam a movimentação de
formas diferentes:

    caged_old (2007-2019)  -> competência DECLARADA
    caged_mov (2020-2026)  -> competência da MOVIMENTAÇÃO

Não é a mesma definição. Declaração atrasada aparece no mês da declaração na
primeira e no mês do fato na segunda. Na prática o efeito é pequeno para o
agregado mensal — o grosso é declarado em dia —, mas é uma emenda, e qualquer
modelo sobre esta série precisa ser testado quanto a quebra em 2020 antes de
se acreditar nele. `diagnostico()` faz esse teste.
"""
import pandas as pd

from extracao_ftp.config_extracao import BUCKET_SILVER_TI, conectar_duckdb

INICIO_NOVO_CAGED = pd.Timestamp("2020-01-01")

SQL = f"""
WITH uniao AS (
    SELECT competenciamov_data AS competencia,
           saldomovimentacao   AS saldo
    FROM read_parquet('s3://{BUCKET_SILVER_TI}/caged_mov/**/*.parquet',
                      hive_partitioning=true)
    UNION ALL
    SELECT competencia_declarada_data, saldo_mov
    FROM read_parquet('s3://{BUCKET_SILVER_TI}/caged_old/**/*.parquet',
                      hive_partitioning=true)
)
SELECT date_trunc('month', competencia)          AS mes,
       sum(saldo)                                 AS saldo,
       count(*) FILTER (WHERE saldo = 1)          AS admissoes,
       count(*) FILTER (WHERE saldo = -1)         AS desligamentos
FROM uniao
WHERE competencia IS NOT NULL
GROUP BY 1 ORDER BY 1
"""


def carregar(con=None) -> pd.DataFrame:
    """Série mensal completa, indexada por mês, com frequência declarada."""
    con = con or conectar_duckdb()
    con.execute("SET enable_progress_bar=false")
    df = con.execute(SQL).df()
    df["mes"] = pd.to_datetime(df["mes"])
    df = df.set_index("mes").sort_index()
    # `asfreq` deixa a frequência EXPLÍCITA para o statsmodels. Sem isso ele
    # avisa que vai supor a frequência e as previsões saem sem índice de data,
    # o que embaralha a leitura do resultado.
    return df.asfreq("MS")


def diagnostico(df: pd.DataFrame) -> dict:
    """
    Números que dizem se dá para modelar esta série — e se a emenda incomoda.

    Vale mais que um gráfico: quebra estrutural invisível no gráfico produz
    modelo que ajusta bem o passado e erra o futuro inteiro.
    """
    from statsmodels.tsa.stattools import adfuller, kpss

    saldo = df["saldo"].dropna()
    antes = saldo[saldo.index < INICIO_NOVO_CAGED]
    depois = saldo[saldo.index >= INICIO_NOVO_CAGED]

    # ADF e KPSS têm hipóteses nulas OPOSTAS. Rodar os dois evita a conclusão
    # apressada: só há estacionariedade quando ADF rejeita E KPSS não rejeita.
    adf_p = adfuller(saldo, autolag="AIC")[1]
    import warnings
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        kpss_p = kpss(saldo, regression="c", nlags="auto")[1]

    sazonal = saldo.groupby(saldo.index.month).mean()
    return {
        "n": len(saldo),
        "inicio": saldo.index.min(),
        "fim": saldo.index.max(),
        "media": saldo.mean(),
        "desvio": saldo.std(),
        "adf_p": adf_p,
        "kpss_p": kpss_p,
        "media_antes_2020": antes.mean(),
        "media_depois_2020": depois.mean(),
        "desvio_antes_2020": antes.std(),
        "desvio_depois_2020": depois.std(),
        "mes_pior": int(sazonal.idxmin()),
        "mes_melhor": int(sazonal.idxmax()),
        "sazonal": sazonal,
    }
