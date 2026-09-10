"""
O mapa: onde o emprego de TI está (RAIS) e para onde ele se move (CAGED).

POR QUE DUAS CAMADAS E NÃO UMA
------------------------------
Um mapa de estoque pinta São Paulo de escuro e o resto de claro, todo ano,
para sempre. É verdadeiro e é inútil: mede população.

Aqui há duas leituras sobrepostas, e a graça está em compará-las:

    cor da UF    -> ESTOQUE (RAIS). O tamanho do mercado.
    bolha        -> FLUXO (CAGED). Para onde ele está se movendo.

Um estado escuro com bolha vermelha está grande e encolhendo. Um estado claro
com bolha azul grande está pequeno e fervendo. Nenhuma das duas bases sozinha
mostra isso, e é a comparação que responde "onde o mercado está indo".

O QUE O MAPA NÃO MOSTRA
-----------------------
Onde as pessoas trabalham. A RAIS localiza pelo ESTABELECIMENTO, e empresa de
TI escolhe onde se registrar por causa do ISS — Barueri aparece com 200
vínculos de TI por mil habitantes. O aviso fica na própria página, não só aqui
no código: um mapa é convincente demais para carregar essa ressalva escondida.
"""
import json
from pathlib import Path

import pandas as pd
import plotly.graph_objects as go
import streamlit as st

from dashboard import dados_modelos as dm
from dashboard import tema
from dashboard.tema import fmt_compacto, fmt_num

GEOJSON = Path(__file__).resolve().parent / "geo" / "br_uf.json"


@st.cache_resource
def _geojson() -> dict | None:
    try:
        return json.loads(GEOJSON.read_text(encoding="utf-8"))
    except Exception:
        return None


def _leitura(texto: str):
    if texto:
        st.markdown(f'<div class="leitura">{texto}</div>', unsafe_allow_html=True)


def _coropleto(df: pd.DataFrame, coluna: str, titulo: str, escala: str):
    geo = _geojson()
    if geo is None:
        st.warning("Geometria dos estados não encontrada (`dashboard/geo/br_uf.json`).")
        return None

    fig = go.Figure(go.Choropleth(
        geojson=geo,
        locations=df["uf"],
        featureidkey="properties.sigla",
        z=df[coluna],
        colorscale=escala,
        marker_line_color=tema.SUPERFICIE,
        marker_line_width=0.6,
        colorbar=dict(title=dict(text=titulo, side="right"), thickness=12,
                      len=0.75, outlinewidth=0),
        hovertemplate="<b>%{location}</b><br>" + titulo + ": %{z:,.0f}<extra></extra>",
    ))
    fig.update_geos(fitbounds="locations", visible=False,
                    bgcolor="rgba(0,0,0,0)")
    fig.update_layout(margin=dict(l=0, r=0, t=10, b=0), height=560,
                      paper_bgcolor="rgba(0,0,0,0)",
                      font=dict(color=tema.TEXTO))
    return fig


def _bolhas(df: pd.DataFrame, ano: int):
    """
    Municípios como bolhas: tamanho é estoque, cor é o sinal do saldo.

    Azul/vermelho em vez de verde/vermelho pelo mesmo motivo das barras do
    resto do dashboard — daltônicos não distinguem verde de vermelho, e aqui o
    sinal é a informação principal.
    """
    if df.empty:
        return None
    d = df.copy()
    d["cor"] = [tema.POSITIVO if s >= 0 else tema.NEGATIVO for s in d["saldo"]]
    # Raiz do estoque no tamanho: área proporcional ao valor, não o raio —
    # senão São Paulo cobriria o Sudeste inteiro.
    d["tamanho"] = (d["estoque"] ** 0.5)
    maior = d["tamanho"].max() or 1

    fig = go.Figure(go.Scattergeo(
        lon=d["longitude"], lat=d["latitude"],
        text=d["municipio"],
        customdata=d[["estoque", "saldo", "remuneracao_sm_mediana"]],
        marker=dict(
            size=d["tamanho"] / maior * 42 + 3,
            color=d["cor"], opacity=0.65,
            line=dict(width=0.4, color=tema.SUPERFICIE),
        ),
        hovertemplate=("<b>%{text}</b><br>estoque: %{customdata[0]:,.0f}"
                       "<br>saldo no ano: %{customdata[1]:+,.0f}"
                       "<br>mediana: %{customdata[2]:.2f} SM<extra></extra>"),
    ))
    fig.update_geos(scope="south america", fitbounds="locations", visible=True,
                    showcountries=True, countrycolor=tema.GRID,
                    showland=True, landcolor=tema.SUPERFICIE,
                    lakecolor="rgba(0,0,0,0)", bgcolor="rgba(0,0,0,0)")
    fig.update_layout(margin=dict(l=0, r=0, t=10, b=0), height=560,
                      paper_bgcolor="rgba(0,0,0,0)", font=dict(color=tema.TEXTO))
    return fig


def render():
    anos = dm.anos_do_mapa()
    if not anos:
        st.info("Os agregados territoriais ainda não foram construídos "
                "(`python -m gold_unificado.mapa`).")
        return

    # A RAIS termina antes do CAGED: o último ano do mapa tem estoque zerado se
    # a RAIS ainda não cobriu. Escolher o último ano COM estoque evita abrir a
    # página num mapa todo branco.
    todos = dm.mapa_uf()
    com_estoque = sorted(todos[todos["estoque"] > 0]["ano"].unique())
    padrao = int(com_estoque[-1]) if com_estoque else anos[-1]

    col1, col2 = st.columns([3, 1])
    with col2:
        ano = st.select_slider("Ano", options=anos, value=padrao)
        camada = st.radio("Colorir por", ["Estoque (RAIS)", "Saldo (CAGED)",
                                          "Remuneração (RAIS)"])
        minimo = st.number_input("Estoque mínimo do município", 10, 5000, 50, 10)
    with col1:
        st.subheader(f"O mercado de TI no Brasil — {ano}")

    uf = dm.mapa_uf(ano)
    if uf.empty:
        st.warning(f"Sem dados para {ano}.")
        return

    coluna, titulo, escala = {
        "Estoque (RAIS)": ("estoque", "Vínculos ativos", "Blues"),
        "Saldo (CAGED)": ("saldo", "Saldo do ano", "RdBu"),
        "Remuneração (RAIS)": ("remuneracao_sm_mediana", "Mediana (SM)", "Purples"),
    }[camada]

    esq, dir_ = st.columns(2)
    with esq:
        st.caption(f"**Estados** · {titulo}")
        fig = _coropleto(uf.dropna(subset=[coluna]), coluna, titulo, escala)
        if fig:
            st.plotly_chart(fig, width='stretch')
    with dir_:
        st.caption("**Municípios** · tamanho = estoque, cor = sinal do saldo")
        muni = dm.municipios_com_coordenada(ano, minimo)
        fig = _bolhas(muni, ano)
        if fig:
            st.plotly_chart(fig, width='stretch')
        else:
            st.info("Sem municípios acima do mínimo neste ano.")

    total_estoque = int(uf["estoque"].sum())
    total_saldo = int(uf["saldo"].sum())
    lider = uf.nlargest(1, "estoque")
    k1, k2, k3, k4 = st.columns(4)
    k1.metric("Estoque no país", fmt_compacto(total_estoque))
    k2.metric("Saldo do ano (CAGED)", fmt_compacto(total_saldo))
    if not lider.empty:
        parte = lider["estoque"].iloc[0] / total_estoque * 100 if total_estoque else 0
        k3.metric(f"Maior estoque ({lider['uf'].iloc[0]})",
                  fmt_compacto(int(lider['estoque'].iloc[0])),
                  f"{parte:.0f}% do país")
    k4.metric("UFs com saldo positivo",
              f"{int((uf['saldo'] > 0).sum())} de {len(uf)}")

    # O achado que só a sobreposição das duas bases produz.
    juntos = uf[(uf["estoque"] > 0)].copy()
    if not juntos.empty:
        juntos["intensidade"] = juntos["saldo"] / juntos["estoque"] * 100
        subindo = juntos.nlargest(3, "intensidade")
        caindo = juntos.nsmallest(3, "intensidade")
        _leitura(
            "O mapa da esquerda mostra onde o mercado <strong>está</strong>; "
            "o da direita, para onde ele <strong>se move</strong>. "
            f"Em {ano}, o maior movimento relativo ao próprio tamanho foi em "
            f"<strong>{', '.join(subindo['uf'])}</strong>, e a maior retração "
            f"em <strong>{', '.join(caindo['uf'])}</strong>. "
            "Nenhuma das duas bases sozinha mostra isso: o estoque diz o "
            "tamanho, o fluxo diz a direção."
        )

    st.subheader("Estados por estoque e por movimento")
    ordenado = uf.sort_values("estoque", ascending=False).head(15)
    fig = go.Figure()
    fig.add_trace(go.Bar(y=ordenado["uf"], x=ordenado["estoque"], orientation="h",
                         name="Estoque (RAIS)", marker_color=tema.SERIE_1,
                         hovertemplate="%{y}<br>%{x:,.0f} vínculos<extra></extra>"))
    lay = tema.layout_base(altura=420, mostrar_legenda=False)
    lay["margin"]["l"] = 8
    fig.update_layout(**lay)
    fig.update_yaxes(autorange="reversed")
    st.plotly_chart(fig, width='stretch')

    st.warning(
        "**A localização é a do estabelecimento, não a do trabalho.** A empresa "
        "de TI escolhe onde se registrar por causa do ISS, e isso desloca o "
        "mapa: Barueri aparece com 200 vínculos de TI por mil habitantes, e "
        "Guaraciaba (MG), com 10 mil habitantes, saiu de 1 vínculo em 2022 "
        "para 1.428 em 2023. A coluna da RAIS que resolveria — `mun_trab` — "
        "vem como 'não informado' em praticamente todos os registros."
    )
