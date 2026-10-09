"""
O mapa: onde o emprego de TI está (RAIS) e para onde ele se move (CAGED).

UM MAPA SÓ, COM DUAS CAMADAS
----------------------------
Antes eram dois mapas lado a lado, cada um com metade da largura — e a métrica
escolhida mudava só o da esquerda. Ficava pequeno e, pior, enganoso: mexer no
seletor não alterava as bolhas, então parecia que os municípios não respondiam
a nada.

Agora é um mapa em largura cheia com as duas camadas sobrepostas, e as duas
seguem a métrica escolhida:

    cor do estado  -> a métrica, em tom suave (fundo)
    bolha          -> o município: tamanho pelo estoque, cor pela métrica

Sobrepor é o que permite comparar as duas leituras no mesmo lugar: um estado
claro com bolhas escuras concentra o mercado em poucas cidades; um estado
uniforme o distribui.

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

# Cada métrica com a escala que a torna legível. `divergente` marca a que tem
# zero como referência: saldo negativo e positivo precisam de cores opostas,
# e o meio da escala tem de cair exatamente no zero.
METRICAS = {
    "Estoque (RAIS)": ("estoque", "Vínculos ativos", "Blues", False),
    "Saldo (CAGED)": ("saldo", "Saldo do ano", "RdBu", True),
    "Remuneração (RAIS)": ("remuneracao_sm_mediana", "Mediana (SM)", "Purples", False),
}


@st.cache_resource
def _geojson() -> dict | None:
    try:
        return json.loads(GEOJSON.read_text(encoding="utf-8"))
    except Exception:
        return None


def _leitura(texto: str):
    if texto:
        st.markdown(f'<div class="leitura">{texto}</div>', unsafe_allow_html=True)


def _mapa(uf: pd.DataFrame, muni: pd.DataFrame, coluna: str, titulo: str,
          escala: str, divergente: bool, altura: int = 660):
    """
    Estados e municípios no mesmo mapa, ambos pela métrica escolhida.

    O estado entra com opacidade baixa: ele é o pano de fundo, e as bolhas
    precisam continuar legíveis por cima. Sem isso, num estado escuro a bolha
    some.
    """
    geo = _geojson()
    if geo is None:
        st.warning("Geometria dos estados não encontrada (`dashboard/geo/br_uf.json`).")
        return None

    fig = go.Figure()

    dados_uf = uf.dropna(subset=[coluna])
    if not dados_uf.empty:
        fig.add_trace(go.Choropleth(
            geojson=geo, locations=dados_uf["uf"], featureidkey="properties.sigla",
            z=dados_uf[coluna], colorscale=escala,
            zmid=0 if divergente else None,
            marker_line_color=tema.GRID, marker_line_width=0.8, marker_opacity=0.40,
            showscale=False, name="Estados",
            hovertemplate="<b>%{location}</b><br>" + titulo + ": %{z:,.0f}<extra></extra>"))

    dados_muni = muni.dropna(subset=[coluna, "latitude", "longitude"]) if not muni.empty else muni
    if not dados_muni.empty:
        # Raiz do estoque no tamanho: a ÁREA fica proporcional ao valor, não o
        # raio — senão São Paulo cobriria o Sudeste inteiro.
        tamanho = dados_muni["estoque"] ** 0.5
        maior = tamanho.max() or 1
        fig.add_trace(go.Scattergeo(
            lon=dados_muni["longitude"], lat=dados_muni["latitude"], mode="markers",
            name="Municípios", text=dados_muni["municipio"],
            customdata=dados_muni[["estoque", "saldo", "remuneracao_sm_mediana"]],
            marker=dict(
                size=tamanho / maior * 44 + 4,
                color=dados_muni[coluna], colorscale=escala,
                cmid=0 if divergente else None,
                opacity=0.82, line=dict(width=0.6, color=tema.SUPERFICIE),
                showscale=True,
                colorbar=dict(title=dict(text=titulo, side="right"), thickness=13,
                              len=0.7, outlinewidth=0, x=1.0)),
            hovertemplate=("<b>%{text}</b><br>estoque: %{customdata[0]:,.0f}"
                           "<br>saldo no ano: %{customdata[1]:+,.0f}"
                           "<br>mediana: %{customdata[2]:.2f} SM<extra></extra>")))

    fig.update_geos(projection_type="mercator", lataxis_range=[-34, 6.5],
                    lonaxis_range=[-74.5, -33.5], fitbounds=False, visible=True,
                    showcountries=True, countrycolor=tema.GRID,
                    showland=True, landcolor=tema.SUPERFICIE,
                    showocean=False, showlakes=False, bgcolor="rgba(0,0,0,0)")
    fig.update_layout(margin=dict(l=0, r=0, t=4, b=0), height=altura,
                      paper_bgcolor="rgba(0,0,0,0)", showlegend=False,
                      font=dict(color=tema.TEXTO, size=13))
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

    # Controles à esquerda, estreitos: o mapa fica com o resto da largura.
    controles, area = st.columns([1, 4.2], gap="medium")
    with controles:
        ano = st.select_slider("Ano", options=anos, value=padrao)
        camada = st.radio("Colorir por", list(METRICAS))
        minimo = st.number_input("Estoque mínimo do município", 10, 5000, 50, 10,
                                 help="Abaixo disso o município não vira bolha. "
                                      "Diminuir mostra o interior; aumentar limpa o mapa.")

    coluna, titulo, escala, divergente = METRICAS[camada]
    uf = dm.mapa_uf(ano)
    if uf.empty:
        st.warning(f"Sem dados para {ano}.")
        return
    muni = dm.municipios_com_coordenada(ano, minimo)

    with area:
        st.subheader(f"O mercado de TI no Brasil — {ano}")
        st.caption(f"Cor do estado e cor da bolha: **{titulo.lower()}**. "
                   f"Tamanho da bolha: vínculos ativos. "
                   f"{fmt_num(len(muni))} municípios acima do mínimo.")
        fig = _mapa(uf, muni, coluna, titulo, escala, divergente)
        if fig:
            st.plotly_chart(fig, width="stretch")
        elif muni.empty:
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
            "A cor mostra onde o mercado <strong>está</strong> quando se escolhe o "
            "estoque, e para onde ele <strong>se move</strong> quando se escolhe o "
            "saldo — trocar o seletor troca a pergunta, no mesmo mapa. "
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
    st.plotly_chart(fig, width="stretch")

    st.warning(
        "**A localização é a do estabelecimento, não a do trabalho.** A empresa "
        "de TI escolhe onde se registrar por causa do ISS, e isso desloca o "
        "mapa: Barueri aparece com 200 vínculos de TI por mil habitantes, e "
        "Guaraciaba (MG), com 10 mil habitantes, saiu de 1 vínculo em 2022 "
        "para 1.428 em 2023. A coluna da RAIS que resolveria — `mun_trab` — "
        "vem como 'não informado' em praticamente todos os registros."
    )
