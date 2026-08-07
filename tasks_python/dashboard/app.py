"""
Dashboard do mercado de trabalho em TECNOLOGIA (CAGED, 2007–2026).

Organizado em abas, cada uma respondendo uma pergunta — e cada seção traz a
leitura do dado, com os números calculados (ver narrativa.py), para que
atualizar a base atualize também a interpretação.

Rodar (a partir de tasks_python):
    ..\\.venv\\Scripts\\streamlit run dashboard/app.py
"""
import sys
from pathlib import Path

# `streamlit run dashboard/app.py` coloca no sys.path a pasta DO SCRIPT
# (dashboard/), não a raiz do projeto — diferente de `python -m`, que usa o
# diretório atual. Sem isto, `from dashboard import ...` e o `extracao_ftp`
# usado em dados.py não são encontrados, independente de onde se chame.
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import plotly.graph_objects as go  # noqa: E402
import streamlit as st  # noqa: E402

from dashboard import dados, narrativa, tema  # noqa: E402
from dashboard.tema import fmt_compacto, fmt_num, fmt_reais  # noqa: E402

st.set_page_config(page_title="Mercado de Trabalho em TI — CAGED",
                   page_icon="💻", layout="wide")

st.markdown(f"""
<style>
  .block-container {{ padding-top: 2rem; max-width: 1240px; }}
  [data-testid="stMetricValue"] {{ font-size: 1.7rem; font-family: {tema.FONTE}; }}
  [data-testid="stMetricLabel"] {{ color: {tema.TEXTO_SEC}; }}
  h1, h2, h3 {{ font-family: {tema.FONTE}; }}
  .lead {{ font-size: 1.05rem; line-height: 1.65; color: {tema.TEXTO_SEC}; margin-bottom: 6px; }}
  .leitura {{ font-size: 0.95rem; line-height: 1.65; color: {tema.TEXTO_SEC};
              border-left: 3px solid {tema.SERIE_1}; padding: 2px 0 2px 14px;
              margin: 8px 0 18px 0; }}
  .rodape {{ color: {tema.MUTED}; font-size: 12px; margin-top: 16px; line-height: 1.6; }}
  .stTabs [data-baseweb="tab"] {{ font-size: 0.95rem; }}
</style>
""", unsafe_allow_html=True)


def leitura(texto: str):
    """Bloco de interpretação — o que a seção acima quer dizer."""
    if texto:
        st.markdown(f'<div class="leitura">{texto}</div>', unsafe_allow_html=True)


def barras_saldo(x, y, altura=340, margem_esq=8, customdata=None, hover=None,
                 horizontal=False):
    """
    Barras coloridas pelo SINAL do saldo.

    Verde/vermelho seria o par intuitivo para ganho/perda, mas é justamente o
    que daltônicos não distinguem — e aqui o sinal é a informação principal.
    Azul/vermelho preserva a polaridade para todo mundo.
    """
    cores = [tema.POSITIVO if v >= 0 else tema.NEGATIVO for v in (x if horizontal else y)]
    fig = go.Figure(go.Bar(
        x=x, y=y, orientation="h" if horizontal else "v", marker_color=cores,
        customdata=customdata,
        hovertemplate=hover or "%{x}<br>Saldo: %{y:,.0f}<extra></extra>",
    ))
    lay = tema.layout_base(altura=altura, mostrar_legenda=False)
    lay["margin"]["l"] = margem_esq
    fig.update_layout(**lay)
    return fig


# ------------------------------------------------------------------ dados
if not dados.tem_dados():
    st.title("💻 Mercado de Trabalho em Tecnologia")
    st.warning("A silver ainda não tem dados.")
    st.stop()

tem_hist = dados.tem_serie_longa()
anual = dados.serie_longa_anual() if tem_hist else None
arco = narrativa.arco_historico(anual) if tem_hist and anual is not None else {}

st.title("💻 Vinte anos do mercado de trabalho em tecnologia")
st.caption("Microdados do CAGED · recorte: setor de TI (CNAE) **ou** ocupação de TI (CBO) "
           "· saldo = admissões − desligamentos")

if arco:
    st.markdown(f'<p class="lead">{narrativa.texto_abertura(arco)}</p>',
                unsafe_allow_html=True)
    k1, k2, k3, k4 = st.columns(4)
    k1.metric("Saldo acumulado", fmt_compacto(arco["saldo_total"]),
              help=f"{arco['ano_inicio']}–{arco['ano_fim']}")
    k2.metric("Admissões", fmt_compacto(arco["admissoes_total"]))
    k3.metric(f"Melhor ano ({arco['melhor_ano']})", fmt_compacto(arco["melhor_saldo"]))
    k4.metric("Anos no vermelho", str(len(arco["anos_negativos"])),
              help="Anos com saldo negativo em toda a série")

aba_hist, aba_onde, aba_quem, aba_setor, aba_dados = st.tabs([
    "📈 A trajetória",
    "🏢 Onde o trabalho acontece",
    "👥 Quem é contratado",
    "🗺️ Setores e regiões",
    "🔎 Sobre os dados",
])

# ======================================================= 1. A TRAJETÓRIA
with aba_hist:
    if not tem_hist or anual is None or anual.empty:
        st.info("A série histórica depende do arquivo unificado (`caged_ti.parquet`).")
    else:
        por_ano = anual.groupby("ano", as_index=False).agg(
            saldo=("saldo", "sum"), admissoes=("admissoes", "sum"),
            salario_medio=("salario_medio", "mean"))
        ultimo = int(por_ano["ano"].max())

        st.subheader("Saldo anual de empregos em tecnologia")
        fig = barras_saldo(por_ano["ano"], por_ano["saldo"])
        # O último ano quase sempre está incompleto (divulgação é mensal);
        # sem marcar, ele seria lido como queda real.
        fig.add_annotation(x=ultimo,
                           y=por_ano[por_ano["ano"] == ultimo]["saldo"].iloc[0],
                           text="parcial", showarrow=True, arrowhead=0, ay=-28,
                           font=dict(size=10, color=tema.MUTED))
        fig.update_layout(yaxis_title="vagas (líquido)")
        st.plotly_chart(fig, width="stretch")
        leitura(narrativa.texto_serie(arco))

        st.subheader("O salário nunca recuou")
        f2 = go.Figure(go.Scatter(
            x=por_ano["ano"], y=por_ano["salario_medio"], mode="lines+markers",
            name="Salário médio",
            line=dict(color=tema.SERIE_3, width=2), marker=dict(size=6),
            hovertemplate="R$ %{y:,.2f}<extra>Salário médio</extra>",
        ))
        lay = tema.layout_base(altura=280, mostrar_legenda=False, hover_unificado=True)
        lay["yaxis"]["title"] = "R$ na admissão"
        f2.update_layout(**lay)
        st.plotly_chart(f2, width="stretch")
        leitura(narrativa.texto_salario(arco))

        st.subheader("Fluxo bruto: contratações e desligamentos")
        mensal = dados.serie_longa_mensal()
        if not mensal.empty:
            f3 = go.Figure()
            # No modo unificado a data vem no cabeçalho do tooltip, então cada
            # série mostra só o próprio valor — repetir a data em cada linha
            # poluiria a leitura.
            f3.add_scatter(x=mensal["competencia"], y=mensal["admissoes"], mode="lines",
                           name="Admissões", line=dict(color=tema.COR_ADMISSAO, width=1.6),
                           hovertemplate="%{y:,.0f}<extra>Admissões</extra>")
            f3.add_scatter(x=mensal["competencia"], y=mensal["desligamentos"], mode="lines",
                           name="Desligamentos",
                           line=dict(color=tema.COR_DESLIGAMENTO, width=1.6),
                           hovertemplate="%{y:,.0f}<extra>Desligamentos</extra>")
            # O saldo entra como série invisível só para aparecer no tooltip:
            # é a diferença entre as duas curvas, e tê-la junto evita a
            # subtração mental a cada ponto.
            f3.add_scatter(x=mensal["competencia"], y=mensal["saldo"], mode="lines",
                           name="Saldo", line=dict(width=0), showlegend=False,
                           hovertemplate="%{y:+,.0f}<extra>Saldo</extra>")
            f3.update_layout(**tema.layout_base(altura=320, hover_unificado=True))
            st.plotly_chart(f3, width="stretch")
            leitura("As duas curvas andam quase coladas — o mercado de TI tem "
                    "rotatividade alta. O saldo é a distância entre elas, e é por isso "
                    "que um ano ruim não significa colapso de contratações: significa "
                    "que os desligamentos alcançaram as admissões.")

# ============================================= 2. ONDE O TRABALHO ACONTECE
with aba_onde:
    st.subheader("Setor de TI ou ocupação de TI?")
    st.markdown(
        '<p class="lead">O recorte deste estudo une duas definições que descrevem '
        'populações diferentes: quem trabalha <b>em empresa de tecnologia</b> (CNAE) '
        'e quem exerce <b>ocupação de tecnologia</b> (CBO), em qualquer setor.</p>',
        unsafe_allow_html=True)

    lentes = dados.setor_ti_vs_ocupacao_ti()
    if lentes.empty:
        st.info("Sem dados para o cruzamento das lentes.")
    else:
        resumo = (lentes.groupby("categoria", as_index=False)
                  .agg(admissoes=("admissoes", "sum"), saldo=("saldo", "sum"),
                       salario=("salario_medio", "mean"))
                  .sort_values("admissoes"))
        prof = resumo[resumo["categoria"].str.startswith("Profissional de TI")]
        total_prof = int(prof["admissoes"].sum()) if not prof.empty else 0
        fora = int(prof[prof["categoria"].str.contains("fora")]["admissoes"].sum()) \
            if not prof.empty else 0

        if total_prof:
            leitura(narrativa.texto_lentes(fora / total_prof * 100, total_prof))

        f4 = go.Figure(go.Bar(
            x=resumo["admissoes"], y=resumo["categoria"], orientation="h",
            marker_color=[tema.SERIE_1, tema.SERIE_2, tema.SERIE_3][: len(resumo)],
            customdata=resumo["salario"],
            hovertemplate="%{y}<br>Admissões: %{x:,.0f}"
                          "<br>Salário médio: R$ %{customdata:,.2f}<extra></extra>",
        ))
        lay = tema.layout_base(altura=240, mostrar_legenda=False)
        lay["margin"]["l"] = 290
        f4.update_layout(**lay)
        st.plotly_chart(f4, width="stretch")

        st.subheader("Quem mais contrata profissionais de TI fora do setor")
        fora_setor = (lentes[lentes["categoria"].str.contains("fora")]
                      .groupby("setor_empresa", as_index=False)
                      .agg(admissoes=("admissoes", "sum"), saldo=("saldo", "sum"),
                           salario=("salario_medio", "mean"))
                      .sort_values("admissoes", ascending=False).head(10))
        if not fora_setor.empty:
            t = fora_setor.copy()
            t["Admissões"] = t["admissoes"].map(fmt_num)
            t["Saldo"] = t["saldo"].map(fmt_num)
            t["Salário médio"] = t["salario"].map(fmt_reais)
            st.dataframe(t[["setor_empresa", "Admissões", "Saldo", "Salário médio"]]
                         .rename(columns={"setor_empresa": "Setor da empresa"}),
                         width="stretch", hide_index=True)
            leitura("Bancos, comércio, indústria e administração pública aparecem entre "
                    "os maiores empregadores de profissionais de TI — nenhum deles é "
                    "empresa de tecnologia. Uma análise que olhasse só o CNAE de TI "
                    "não veria esse contingente.")

# ================================================== 3. QUEM É CONTRATADO
with aba_quem:
    if not tem_hist:
        st.info("Requer o arquivo unificado.")
    else:
        demo = dados.demografia_longa()
        if demo.empty:
            st.info("Sem dados demográficos.")
        else:
            anos_demo = sorted(demo["ano"].unique())
            faixa = st.select_slider("Período", options=anos_demo,
                                     value=(anos_demo[0], anos_demo[-1]),
                                     key="faixa_demo")
            demo_f = demo[(demo["ano"] >= faixa[0]) & (demo["ano"] <= faixa[1])]

            d1, d2, d3 = st.columns(3)

            def barra(coluna, titulo, alvo, cor):
                agg = (demo_f.groupby(coluna, as_index=False)
                       .agg(admissoes=("admissoes", "sum"),
                            salario=("salario_medio", "mean"))
                       .sort_values("admissoes"))
                fig = go.Figure(go.Bar(
                    x=agg["admissoes"], y=agg[coluna], orientation="h", marker_color=cor,
                    customdata=agg["salario"],
                    hovertemplate="%{y}<br>Admissões: %{x:,.0f}"
                                  "<br>Salário médio: R$ %{customdata:,.2f}<extra></extra>",
                ))
                lay = tema.layout_base(altura=300, mostrar_legenda=False)
                lay["margin"]["l"] = 155
                fig.update_layout(**lay)
                alvo.markdown(f"**{titulo}**")
                alvo.plotly_chart(fig, width="stretch")

            barra("sexo", "Por sexo", d1, tema.SERIE_1)
            barra("raca_cor", "Por raça/cor", d2, tema.SERIE_3)
            barra("escolaridade", "Por escolaridade", d3, tema.SERIE_5)

            sexo_agg = (demo_f.groupby("sexo", as_index=False)
                        .agg(adm=("admissoes", "sum"), sal=("salario_medio", "mean")))
            mulher = sexo_agg[sexo_agg["sexo"].str.contains("Mulher", na=False)]
            homem = sexo_agg[sexo_agg["sexo"].str.contains("Homem", na=False)]
            if not mulher.empty and not homem.empty:
                part = mulher["adm"].iloc[0] / sexo_agg["adm"].sum() * 100
                dif = (1 - mulher["sal"].iloc[0] / homem["sal"].iloc[0]) * 100
                leitura(
                    f"Mulheres são **{part:.0f}%** das admissões em tecnologia no período, "
                    f"e o salário médio de contratação delas é **{dif:.0f}% menor** que o "
                    f"dos homens. A diferença aqui é bruta: não controla ocupação, "
                    f"escolaridade nem experiência — serve para dimensionar, não para "
                    f"atribuir causa."
                )

            st.subheader("Salário por escolaridade")
            sal = (demo_f.groupby("escolaridade", as_index=False)
                   .agg(admissoes=("admissoes", "sum"), salario=("salario_medio", "mean"))
                   .sort_values("salario", ascending=False))
            if len(sal) > 1:
                topo, base = sal.iloc[0], sal.iloc[-1]
                if base["salario"]:
                    leitura(f"Entre o topo e a base da escolaridade há "
                            f"**{topo['salario'] / base['salario']:.1f}×** de diferença no "
                            f"salário de admissão: {fmt_reais(topo['salario'])} para "
                            f"*{topo['escolaridade']}* contra {fmt_reais(base['salario'])} "
                            f"para *{base['escolaridade']}*.")
            t = sal.copy()
            t["Admissões"] = t["admissoes"].map(fmt_num)
            t["Salário médio"] = t["salario"].map(fmt_reais)
            st.dataframe(t[["escolaridade", "Admissões", "Salário médio"]]
                         .rename(columns={"escolaridade": "Escolaridade"}),
                         width="stretch", hide_index=True)

# ================================================ 4. SETORES E REGIÕES
with aba_setor:
    if not tem_hist:
        st.info("Requer o arquivo unificado.")
    else:
        setor = dados.setor_longo()
        uf = dados.uf_longo()
        ocup = dados.ocupacao_longa()

        anos_s = sorted(setor["ano"].unique()) if not setor.empty else []
        if anos_s:
            faixa_s = st.select_slider("Período", options=anos_s,
                                       value=(anos_s[0], anos_s[-1]), key="faixa_setor")

            def recorte(df):
                return df[(df["ano"] >= faixa_s[0]) & (df["ano"] <= faixa_s[1])] \
                    if not df.empty else df

            c1, c2 = st.columns(2)

            with c1:
                st.subheader("Saldo por setor")
                agg = (recorte(setor).groupby("setor", as_index=False)["saldo"].sum()
                       .sort_values("saldo").tail(12))
                agg["rotulo"] = agg["setor"].str.slice(0, 38)
                f = barras_saldo(agg["saldo"], agg["rotulo"], altura=400, margem_esq=250,
                                 customdata=agg["setor"], horizontal=True,
                                 hover="%{customdata}<br>Saldo: %{x:,.0f}<extra></extra>")
                st.plotly_chart(f, width="stretch")

            with c2:
                st.subheader("Saldo por UF")
                agg_uf = (recorte(uf).groupby("uf", as_index=False)["saldo"].sum()
                          .sort_values("saldo").tail(12))
                f = barras_saldo(agg_uf["saldo"], agg_uf["uf"], altura=400,
                                 margem_esq=120, horizontal=True,
                                 hover="%{y}<br>Saldo: %{x:,.0f}<extra></extra>")
                st.plotly_chart(f, width="stretch")

            leitura("O setor de Informação e Comunicação — onde ficam as empresas de "
                    "tecnologia — não é necessariamente o que mais gera saldo: parte "
                    "relevante do emprego em TI aparece em serviços, comércio e "
                    "administração pública, coerente com o achado da aba anterior.")

            st.subheader("Ocupações que puxam o saldo")
            agg_o = (recorte(ocup).groupby("ocupacao", as_index=False)
                     .agg(saldo=("saldo", "sum"), salario=("salario_medio", "mean")))
            top = agg_o.sort_values("saldo").tail(14)
            top["rotulo"] = top["ocupacao"].str.slice(0, 42)
            f = barras_saldo(
                top["saldo"], top["rotulo"], altura=420, margem_esq=270,
                customdata=top[["ocupacao", "salario"]], horizontal=True,
                hover="%{customdata[0]}<br>Saldo: %{x:,.0f}"
                      "<br>Salário médio: R$ %{customdata[1]:,.2f}<extra></extra>")
            st.plotly_chart(f, width="stretch")

# ==================================================== 5. SOBRE OS DADOS
with aba_dados:
    st.subheader("Como estes números foram construídos")
    st.markdown(f"""
**Fonte.** Microdados do CAGED (Ministério do Trabalho e Emprego), obtidos do
FTP público do PDET. Cada linha é uma movimentação: `saldo = +1` para admissão,
`−1` para desligamento.

**Tradução.** Os microdados são quase todos códigos. Cada coluna codificada foi
traduzida pelos **dicionários oficiais do próprio MTE**, mantendo o código
original ao lado da descrição.

**Recorte de tecnologia.** União de duas definições:
- **Setor de TI** — CNAE de serviços de tecnologia (divisões 62 e 63)
- **Ocupação de TI** — famílias CBO de ocupações de tecnologia, em qualquer setor

O recorte é por *família de código*, nunca por palavra-chave na descrição:
filtrar por "sistemas" traria *Operação de Sistemas de Irrigação por Aspersão*.

**Duas gerações unificadas.** O CAGED mudou de layout em 2020. As bases foram
harmonizadas num arquivo único (2007–2026). O setor do CAGED antigo é
**derivado** do CNAE 2.0 — validado contra a seção oficial do Novo CAGED, com
correspondência integral.

**Limitações que valem citar:**
- Salários são **nominais**, sem correção pela inflação — comparações de longo
  prazo superestimam o ganho real.
- O último ano é **parcial** (a divulgação é mensal).
- O CAGED cobre apenas o **emprego formal celetista**: não inclui PJ,
  cooperados nem servidores estatutários — recorte relevante em tecnologia,
  onde a contratação PJ é comum.
- Diferenças salariais mostradas são **brutas**, sem controle por ocupação,
  experiência ou jornada.

**Dados abertos.** O conjunto tratado está publicado e pode ser consultado
diretamente:

```python
import duckdb
BASE = "https://huggingface.co/datasets/Gianpedro/caged-tecnologia/resolve/main"
duckdb.sql(f"SELECT ano, sum(saldo) FROM read_parquet('{{BASE}}/caged_ti.parquet') GROUP BY 1")
```

*Origem atual dos dados nesta página: {dados.fonte_atual()}.*
""")

st.markdown(
    '<div class="rodape">'
    'Fonte: microdados do CAGED (MTE), traduzidos pelos dicionários oficiais. '
    'Saldo = admissões − desligamentos. Salário médio considera apenas admissões '
    'com valor informado, sem correção pela inflação.'
    '</div>', unsafe_allow_html=True,
)
