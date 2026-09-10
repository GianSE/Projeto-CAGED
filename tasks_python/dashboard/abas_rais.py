"""
Abas da RAIS: estoque, remuneração e empresas.

POR QUE EM MÓDULO SEPARADO
--------------------------
`app.py` já organiza cinco abas do CAGED. Somar mais três ali passaria de 600
linhas num arquivo só, e as perguntas são de natureza diferente: o CAGED
responde quanto o mercado se MOVEU, a RAIS responde de que TAMANHO ele é.
Separar mantém cada arquivo legível e deixa explícito que são duas leituras,
não uma continuação.

Cada função renderiza uma aba inteira e não devolve nada — o efeito é o
desenho na página.
"""
import plotly.graph_objects as go
import streamlit as st

from dashboard import dados_rais as dr
from dashboard import narrativa, tema
from dashboard.tema import fmt_compacto, fmt_num


def _leitura(texto: str):
    if texto:
        st.markdown(f'<div class="leitura">{texto}</div>', unsafe_allow_html=True)


def _linha(x, y, nome, cor=None, altura=340, sufixo=""):
    fig = go.Figure(go.Scatter(
        x=x, y=y, mode="lines+markers", name=nome,
        line=dict(color=cor or tema.SERIE_1, width=2.5),
        marker=dict(size=6),
        hovertemplate=f"%{{x}}<br>{nome}: %{{y:,.2f}}{sufixo}<extra></extra>",
    ))
    fig.update_layout(**tema.layout_base(altura=altura, mostrar_legenda=False))
    return fig


def _barras(x, y, cor=None, altura=340, horizontal=False, hover=None, margem_esq=8):
    fig = go.Figure(go.Bar(
        x=x, y=y, orientation="h" if horizontal else "v",
        marker_color=cor or tema.SERIE_1,
        hovertemplate=hover or "%{x}<br>%{y:,.0f}<extra></extra>",
    ))
    lay = tema.layout_base(altura=altura, mostrar_legenda=False)
    lay["margin"]["l"] = margem_esq
    fig.update_layout(**lay)
    return fig


# ====================================================== 1. ESTOQUE
def estoque(anual, arco):
    """Quantos empregos de tecnologia EXISTEM — e há quanto tempo duram."""
    if anual is None or anual.empty:
        st.info("A gold da RAIS ainda não foi construída "
                "(`python -m gold_rais.construir_gold`).")
        return

    st.subheader("O tamanho do mercado de tecnologia")
    _leitura(narrativa.texto_estoque(arco))

    st.plotly_chart(
        _linha(anual["ano"], anual["estoque"], "Vínculos ativos em 31/12",
               altura=380, sufixo=""),
        width='stretch')

    st.caption("Fonte: RAIS · vínculos com `vinculo_ativo_3112 = SIM`. "
               "Vínculos que existiram e terminaram durante o ano ficam de fora "
               "do estoque — somá-los inflaria o número.")

    col1, col2 = st.columns(2)
    with col1:
        st.subheader("Permanência no emprego")
        st.plotly_chart(
            _linha(anual["ano"], anual["tempo_emprego_meses"],
                   "Tempo médio de emprego (meses)", cor=tema.SERIE_3, sufixo=" meses"),
            width='stretch')
        _leitura(
            "Tempo médio de vínculo é a leitura de rotatividade que o CAGED não "
            "permite: ele registra a movimentação, não a duração. Queda no tempo "
            "médio com estoque crescendo indica mercado que contrata muito e "
            "segura pouco."
        )
    with col2:
        st.subheader("Rotatividade contra o estoque")
        taxa = (anual["desligados"] / anual["estoque"].replace(0, None) * 100)
        st.plotly_chart(
            _linha(anual["ano"], taxa, "Desligados no ano / estoque (%)",
                   cor=tema.SERIE_2, sufixo="%"),
            width='stretch')
        _leitura(
            "Quantos vínculos terminaram durante o ano, em proporção ao estoque "
            "que sobreviveu até dezembro. É o giro do mercado."
        )

    # O cruzamento das duas lentes, agora em nível e não em fluxo.
    lentes = dr.lentes()
    if not lentes.empty:
        ultimo = int(lentes["ano"].max())
        atual = lentes[lentes["ano"] == ultimo]
        st.subheader(f"Onde estão os profissionais de TI ({ultimo})")
        st.plotly_chart(
            _barras(atual["estoque"], atual["categoria"], horizontal=True,
                    altura=260, margem_esq=8,
                    hover="%{y}<br>%{x:,.0f} vínculos<extra></extra>"),
            width='stretch')

        def _val(cat, col):
            linha = atual[atual["categoria"] == cat]
            return float(linha[col].iloc[0]) if not linha.empty else 0.0

        _leitura(narrativa.texto_lentes_estoque(
            fora=int(_val("Profissional de TI fora do setor de TI", "estoque")),
            dentro=int(_val("Profissional de TI em empresa de TI", "estoque")),
            sm_fora=_val("Profissional de TI fora do setor de TI", "remuneracao_sm_mediana"),
            sm_dentro=_val("Profissional de TI em empresa de TI", "remuneracao_sm_mediana"),
        ))


# ================================================= 2. REMUNERAÇÃO
def remuneracao(anual, arco, anos):
    """Quanto se paga — em salários mínimos, para atravessar 19 anos."""
    if anual is None or anual.empty:
        st.info("A gold da RAIS ainda não foi construída.")
        return

    st.subheader("Remuneração em salários mínimos")
    _leitura(narrativa.texto_remuneracao(arco))

    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=anual["ano"], y=anual["remuneracao_sm"], mode="lines+markers",
        name="Média", line=dict(color=tema.SERIE_2, width=2.5, dash="dot"),
        hovertemplate="%{x}<br>Média: %{y:.2f} SM<extra></extra>"))
    fig.add_trace(go.Scatter(
        x=anual["ano"], y=anual["remuneracao_sm_mediana"], mode="lines+markers",
        name="Mediana", line=dict(color=tema.SERIE_1, width=2.5),
        hovertemplate="%{x}<br>Mediana: %{y:.2f} SM<extra></extra>"))
    fig.update_layout(**tema.layout_base(altura=380, mostrar_legenda=True))
    st.plotly_chart(fig, width='stretch')

    st.caption(
        "Medir em múltiplos do salário mínimo torna a série comparável sem "
        "escolher deflator. A partir de 2023 a fonte traz registros com a "
        "conversão invertida (valor multiplicado pelo mínimo em vez de "
        f"dividido) — {fmt_num(arco.get('descartados', 0))} deles foram "
        "descartados por um teto de 500 salários mínimos. Sem esse corte a "
        "média saltaria de 6,17 para 8,84 SM entre 2022 e 2023, o que se leria "
        "como uma valorização de 43% que não aconteceu."
    )

    col1, col2 = st.columns(2)
    with col1:
        st.subheader("Por área de atuação")
        areas = dr.estoque_por_area()
        if not areas.empty:
            ultimo = int(areas["ano"].max())
            atual = areas[areas["ano"] == ultimo].sort_values("remuneracao_sm_mediana")
            st.plotly_chart(
                _barras(atual["remuneracao_sm_mediana"], atual["area"],
                        horizontal=True, altura=320, cor=tema.SERIE_4,
                        hover="%{y}<br>%{x:.2f} SM (mediana)<extra></extra>"),
                width='stretch')
            st.caption(f"Mediana em salários mínimos, {ultimo}.")

    with col2:
        st.subheader("Hiato salarial por escolaridade")
        ano_ref = max(anos) if anos else None
        hiato = dr.hiato_por_escolaridade(ano_ref) if ano_ref else None
        if hiato is not None and not hiato.empty:
            pivot = hiato.pivot_table(index="escolaridade", columns="sexo",
                                      values="remuneracao_sm_mediana")
            colunas = [c for c in pivot.columns]
            fig = go.Figure()
            for i, sexo in enumerate(colunas):
                fig.add_trace(go.Bar(
                    y=pivot.index, x=pivot[sexo], name=str(sexo), orientation="h",
                    marker_color=tema.CATEGORICA[i % 5],
                    hovertemplate="%{y}<br>" + str(sexo) + ": %{x:.2f} SM<extra></extra>"))
            lay = tema.layout_base(altura=320, mostrar_legenda=True)
            lay["margin"]["l"] = 8
            fig.update_layout(barmode="group", **lay)
            st.plotly_chart(fig, width='stretch')
            _leitura(
                "Comparar dentro de cada nível de formação é o que separa "
                "&ldquo;ganham menos porque estudaram menos&rdquo; de "
                "&ldquo;ganham menos com a mesma formação&rdquo;. A diferença "
                "bruta mistura as duas explicações e não sustenta nenhuma."
            )

    st.subheader("Ocupações mais bem remuneradas")
    ano_ref = max(anos) if anos else None
    ocup = dr.remuneracao_por_ocupacao(ano_ref, limite=15) if ano_ref else None
    if ocup is not None and not ocup.empty:
        ordenado = ocup.sort_values("remuneracao_sm_mediana").tail(15)
        st.plotly_chart(
            _barras(ordenado["remuneracao_sm_mediana"], ordenado["ocupacao"],
                    horizontal=True, altura=460, cor=tema.SERIE_1, margem_esq=8,
                    hover="%{y}<br>%{x:.2f} SM (mediana)<extra></extra>"),
            width='stretch')
        st.caption(f"Mediana em salários mínimos, {ano_ref}. Só ocupações com "
                   "pelo menos 200 vínculos no estoque.")


# ==================================================== 3. EMPRESAS
def empresas(anos):
    """Quantas empresas de TI existem — o cadastro que o CAGED não tem."""
    porano = dr.estabelecimentos_por_ano()
    if porano.empty:
        st.info("A gold da RAIS ainda não foi construída.")
        return

    ultimo = int(porano["ano"].max())
    atual = porano[porano["ano"] == ultimo].iloc[0]
    primeiro = porano.iloc[0]

    k1, k2, k3 = st.columns(3)
    k1.metric(f"Estabelecimentos de TI ({ultimo})",
              fmt_compacto(atual["estabelecimentos"]))
    k2.metric("Vínculos ativos nessas empresas",
              fmt_compacto(atual["vinculos_ativos"]))
    k3.metric("Média de vínculos por empresa",
              f"{atual['media_vinculos_por_estab']:.1f}")

    st.subheader("Empresas do setor de tecnologia")
    st.plotly_chart(
        _linha(porano["ano"], porano["estabelecimentos"],
               "Estabelecimentos", altura=340),
        width='stretch')
    _leitura(
        f"O setor saiu de {fmt_num(primeiro['estabelecimentos'])} estabelecimentos "
        f"em {int(primeiro['ano'])} para {fmt_num(atual['estabelecimentos'])} em "
        f"{ultimo}. Aqui a lente é só o CNAE: uma empresa não exerce ocupação, "
        f"então o recorte de estabelecimento é necessariamente setorial."
    )

    col1, col2 = st.columns(2)
    with col1:
        st.subheader("Concentração por porte")
        porte = dr.estabelecimentos_por_porte(ultimo)
        if not porte.empty:
            ordenado = porte.sort_values("vinculos_ativos").tail(12)
            st.plotly_chart(
                _barras(ordenado["vinculos_ativos"], ordenado["porte"],
                        horizontal=True, altura=380, cor=tema.SERIE_3,
                        hover="%{y}<br>%{x:,.0f} vínculos<extra></extra>"),
                width='stretch')
            st.caption(f"Vínculos ativos por faixa de tamanho do estabelecimento, {ultimo}.")

    with col2:
        st.subheader("Onde o emprego de TI está")
        muni = dr.municipios(ultimo, limite=15)
        if not muni.empty:
            ordenado = muni.sort_values("estoque").tail(15)
            st.plotly_chart(
                _barras(ordenado["estoque"], ordenado["municipio"],
                        horizontal=True, altura=380, cor=tema.SERIE_5,
                        hover="%{y}<br>%{x:,.0f} vínculos<extra></extra>"),
                width='stretch')
            st.caption(f"Municípios com maior estoque de vínculos de TI, {ultimo}.")

    uf = dr.estoque_por_uf()
    if not uf.empty:
        st.subheader("Estoque por unidade da federação")
        atual_uf = uf[uf["ano"] == ultimo].sort_values("estoque").tail(20)
        st.plotly_chart(
            _barras(atual_uf["estoque"], atual_uf["uf"], horizontal=True,
                    altura=460, cor=tema.SERIE_2,
                    hover="%{y}<br>%{x:,.0f} vínculos<extra></extra>"),
            width='stretch')
        st.caption("A RAIS não traz coluna de UF: a sigla é extraída do prefixo "
                   "da descrição do município ('Df-Brasilia').")
