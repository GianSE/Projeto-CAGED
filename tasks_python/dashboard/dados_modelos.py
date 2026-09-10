"""
Leitura das tabelas de modelo e de território na gold.

TUDO AQUI É LEITURA
-------------------
Nenhum modelo roda no dashboard. Eles são materializados por
`ciencia_dados.materializar` e por `gold_unificado.mapa`, e o que chega aqui é
resultado pronto: previsão com intervalo, tábua de sobrevivência, decomposição
do hiato, grupos de municípios, agregados territoriais.

O motivo está no docstring de `materializar`: os quatro modelos custam cerca de
quinze minutos somados, e um dashboard que os recalculasse seria inutilizável.

FALHA SILENCIOSA É PIOR QUE PÁGINA VAZIA
----------------------------------------
Cada leitor devolve DataFrame vazio quando a tabela não existe, e a aba mostra
o comando que falta rodar. A alternativa — deixar a exceção subir — derrubaria
a página inteira porque um modelo não foi materializado; a outra alternativa,
esconder o erro, deixaria um gráfico em branco sem explicação.
"""
import os

import pandas as pd
import streamlit as st

from dashboard.dados import _consultar, conectar  # noqa: F401
from extracao_ftp.config_extracao import BUCKET_GOLD

# Mesma variável própria de `dados_rais`: os agregados não estão no dataset do
# CAGED, e reaproveitar DADOS_URL_BASE daria 404 disfarçado de tabela ausente.
URL_BASE = os.getenv("DADOS_URL_BASE_RAIS", "").rstrip("/")


def caminho(nome: str) -> str:
    return (f"{URL_BASE}/{nome}.parquet" if URL_BASE
            else f"s3://{BUCKET_GOLD}/{nome}.parquet")


@st.cache_data(ttl=1800, show_spinner=False)
def ler(nome: str) -> pd.DataFrame:
    """Uma tabela da gold, ou DataFrame vazio se ela ainda não existe."""
    try:
        return conectar().execute(
            f"SELECT * FROM read_parquet('{caminho(nome)}')").df()
    except Exception:
        return pd.DataFrame()


@st.cache_resource
def tem(nome: str) -> bool:
    try:
        conectar().execute(
            f"SELECT 1 FROM read_parquet('{caminho(nome)}') LIMIT 1").fetchone()
        return True
    except Exception:
        return False


# ---------------------------------------------------------------- território
def mapa_uf(ano: int | None = None) -> pd.DataFrame:
    df = ler("mapa_uf")
    if df.empty or ano is None:
        return df
    return df[df["ano"] == ano]


def mapa_municipio(ano: int | None = None) -> pd.DataFrame:
    df = ler("mapa_municipio")
    if df.empty or ano is None:
        return df
    return df[df["ano"] == ano]


def municipios_com_coordenada(ano: int, minimo: int = 30) -> pd.DataFrame:
    """
    Municípios do ano pedido já cruzados com centroide e população.

    O cruzamento acontece aqui, e não na gold, porque o mapa é a única coisa
    que precisa de coordenada — carregá-la em toda tabela territorial
    engordaria agregados que ninguém plota.
    """
    mapa, geo = ler("mapa_municipio"), ler("geo_municipios")
    if mapa.empty or geo.empty:
        return pd.DataFrame()
    df = mapa[(mapa["ano"] == ano) & (mapa["estoque"] >= minimo)]
    return df.merge(geo[["cod6", "latitude", "longitude", "populacao"]],
                    left_on="cod_municipio", right_on="cod6", how="inner")


def anos_do_mapa() -> list[int]:
    df = ler("mapa_uf")
    return [] if df.empty else sorted(int(a) for a in df["ano"].unique())


def clusters() -> pd.DataFrame:
    return ler("municipios_cluster")


# -------------------------------------------------------------------- modelos
def previsao() -> pd.DataFrame:
    return ler("previsao_saldo")


def previsao_placar() -> pd.DataFrame:
    return ler("previsao_placar")


def serie_mensal() -> pd.DataFrame:
    df = ler("serie_mensal")
    if not df.empty:
        df["mes"] = pd.to_datetime(df["mes"])
    return df


def nowcast_retrospectiva() -> pd.DataFrame:
    return ler("nowcast_retrospectiva")


def nowcast_ano_retido() -> pd.DataFrame:
    return ler("nowcast_ano_retido")


def nowcast_projecao() -> pd.DataFrame:
    return ler("nowcast_projecao")


def nowcast_pares() -> pd.DataFrame:
    return ler("nowcast_pares")


def sobrevivencia_tabua(recorte: str = "todos") -> pd.DataFrame:
    df = ler("sobrevivencia_tabua")
    return df if df.empty else df[df["recorte"] == recorte]


def sobrevivencia_risco() -> pd.DataFrame:
    return ler("sobrevivencia_risco")


def sobrevivencia_multivariado() -> pd.DataFrame:
    return ler("sobrevivencia_multivariado")


def hiato(comparacao: str | None = None) -> pd.DataFrame:
    df = ler("hiato_serie")
    if df.empty or comparacao is None:
        return df
    return df[df["comparacao"] == comparacao]


def comparacoes_do_hiato() -> list[str]:
    df = ler("hiato_serie")
    return [] if df.empty else sorted(df["comparacao"].unique())
