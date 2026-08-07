"""
Dashboard do mercado de trabalho em TECNOLOGIA (CAGED).

DuckDB lê a camada silver direto — ela já vem recortada em tecnologia
(~4,5 milhões de movimentações), então não há camada gold no caminho; as
agregações ficam em dados.py, cacheadas pelo Streamlit.

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

from dashboard import dados, tema  # noqa: E402
from dashboard.tema import fmt_compacto, fmt_num, fmt_reais  # noqa: E402

st.set_page_config(page_title="Mercado de Trabalho em TI — CAGED",
                   page_icon="💻", layout="wide")

st.markdown(f"""
<style>
  .block-container {{ padding-top: 2.2rem; max-width: 1400px; }}
  [data-testid="stMetricValue"] {{ font-size: 1.8rem; font-family: {tema.FONTE}; }}
  [data-testid="stMetricLabel"] {{ color: {tema.TEXTO_SEC}; }}
  h1, h2, h3 {{ font-family: {tema.FONTE}; }}
  .rodape {{ color: {tema.MUTED}; font-size: 12px; margin-top: 10px; line-height: 1.6; }}
</style>
""", unsafe_allow_html=True)

st.title("💻 Mercado de Trabalho em Tecnologia")
st.caption("Novo CAGED (2020+) · recorte: setor de TI (CNAE) **ou** ocupação de TI (CBO) · "
           "saldo = admissões − desligamentos")

if not dados.tem_dados():
    st.warning("A silver de `caged_mov` ainda não tem dados. "
               "Rode `python -m silver_caged.construir_silver --tabela caged_mov`.")
    st.stop()

mensal = dados.mensal()
por_uf = dados.mensal_por_uf()

# ------------------------------------------------------------------ filtros
anos = sorted(mensal["competencia"].dt.year.unique()) if not mensal.empty else []
c1, c2 = st.columns([2, 3])
with c1:
    if len(anos) > 1:
        ano_ini, ano_fim = st.select_slider("Período", options=anos, value=(anos[0], anos[-1]))
    else:
        ano_ini = ano_fim = anos[0] if anos else None
with c2:
    ufs = sorted(por_uf["uf"].dropna().unique()) if not por_uf.empty else []
    ufs_sel = st.multiselect("UF (vazio = Brasil)", ufs, default=[])


def por_periodo(df, coluna="competencia"):
    if df.empty or ano_ini is None:
        return df
    anos_df = df[coluna].dt.year if coluna == "competencia" else df[coluna]
    return df[(anos_df >= ano_ini) & (anos_df <= ano_fim)]


# Com UF selecionada o recorte territorial vale para o painel inteiro: a série
# mensal passa a vir de por_uf (que tem a dimensão UF), senão os KPIs mostrariam
# o Brasil enquanto os gráficos abaixo mostrariam a seleção.
uf_f = por_periodo(por_uf)
if ufs_sel:
    uf_f = uf_f[uf_f["uf"].isin(ufs_sel)]
    mensal_f = (uf_f.groupby("competencia", as_index=False)
                .agg(admissoes=("admissoes", "sum"),
                     desligamentos=("desligamentos", "sum"),
                     saldo=("saldo", "sum"),
                     salario_medio=("salario_medio", "mean"),
                     idade_media=("idade_media", "mean")))
else:
    mensal_f = por_periodo(mensal)

# --------------------------------------------------------------------- KPIs
if not mensal_f.empty:
    adm = int(mensal_f["admissoes"].sum())
    desl = int(mensal_f["desligamentos"].sum())
    saldo = int(mensal_f["saldo"].sum())

    k1, k2, k3, k4, k5 = st.columns(5)
    k1.metric("Saldo de empregos em TI", fmt_compacto(saldo))
    k2.metric("Admissões", fmt_compacto(adm))
    k3.metric("Desligamentos", fmt_compacto(desl))
    k4.metric("Salário médio de admissão", fmt_reais(mensal_f["salario_medio"].mean()))
    k5.metric("Idade média na admissão", f"{mensal_f['idade_media'].mean():.0f} anos")

st.divider()

# ---------------------------------------------------- série temporal
if not mensal_f.empty:
    st.subheader("Saldo mensal")
    # A cor carrega o SINAL do saldo — a leitura mais importante do gráfico.
    # Verde/vermelho seria o par intuitivo, mas é justamente o que daltônicos
    # não separam; azul/vermelho preserva a polaridade para todo mundo.
    fig = go.Figure(go.Bar(
        x=mensal_f["competencia"], y=mensal_f["saldo"],
        marker_color=[tema.POSITIVO if v >= 0 else tema.NEGATIVO for v in mensal_f["saldo"]],
        hovertemplate="%{x|%b/%Y}<br>Saldo: %{y:,.0f}<extra></extra>",
    ))
    lay = tema.layout_base(altura=300, mostrar_legenda=False)
    lay["yaxis"]["title"] = "vagas (líquido)"
    fig.update_layout(**lay)
    st.plotly_chart(fig, width='stretch')

    st.subheader("Fluxo bruto e salário de contratação")
    g1, g2 = st.columns([3, 2])

    with g1:
        f2 = go.Figure()
        f2.add_scatter(x=mensal_f["competencia"], y=mensal_f["admissoes"], mode="lines",
                       name="Admissões", line=dict(color=tema.COR_ADMISSAO, width=2),
                       hovertemplate="%{x|%b/%Y}<br>Admissões: %{y:,.0f}<extra></extra>")
        f2.add_scatter(x=mensal_f["competencia"], y=mensal_f["desligamentos"], mode="lines",
                       name="Desligamentos", line=dict(color=tema.COR_DESLIGAMENTO, width=2),
                       hovertemplate="%{x|%b/%Y}<br>Desligamentos: %{y:,.0f}<extra></extra>")
        f2.update_layout(**tema.layout_base(altura=300))
        st.plotly_chart(f2, width='stretch')

    with g2:
        f3 = go.Figure(go.Scatter(
            x=mensal_f["competencia"], y=mensal_f["salario_medio"], mode="lines",
            line=dict(color=tema.SERIE_3, width=2),
            hovertemplate="%{x|%b/%Y}<br>R$ %{y:,.2f}<extra></extra>",
        ))
        lay3 = tema.layout_base(altura=300, mostrar_legenda=False)
        lay3["yaxis"]["title"] = "R$ na admissão"
        f3.update_layout(**lay3)
        st.plotly_chart(f3, width='stretch')

st.divider()

# ------------------------------------------- as duas lentes do recorte
st.subheader("Onde o trabalho de TI acontece")
st.caption("O recorte une duas definições: quem trabalha **em empresa de tecnologia** (CNAE) "
           "e quem exerce **ocupação de tecnologia** (CBO). São populações diferentes.")

lentes = por_periodo(dados.setor_ti_vs_ocupacao_ti(), coluna="ano")
if not lentes.empty:
    resumo = (lentes.groupby("categoria", as_index=False)
              .agg(admissoes=("admissoes", "sum"), saldo=("saldo", "sum"),
                   salario=("salario_medio", "mean"))
              .sort_values("admissoes", ascending=True))

    l1, l2 = st.columns([3, 2])
    with l1:
        f4 = go.Figure(go.Bar(
            x=resumo["admissoes"], y=resumo["categoria"], orientation="h",
            marker_color=[tema.SERIE_1, tema.SERIE_2, tema.SERIE_3][: len(resumo)],
            customdata=resumo["salario"],
            hovertemplate="%{y}<br>Admissões: %{x:,.0f}"
                          "<br>Salário médio: R$ %{customdata:,.2f}<extra></extra>",
        ))
        lay4 = tema.layout_base(altura=260, mostrar_legenda=False)
        lay4["margin"]["l"] = 280
        f4.update_layout(**lay4)
        st.plotly_chart(f4, width='stretch')

    with l2:
        prof_ti = resumo[resumo["categoria"].str.startswith("Profissional de TI")]
        if not prof_ti.empty:
            total_prof = prof_ti["admissoes"].sum()
            fora = prof_ti[prof_ti["categoria"].str.contains("fora")]["admissoes"].sum()
            if total_prof:
                st.metric("Profissionais de TI contratados fora do setor de TI",
                          f"{fora / total_prof * 100:.1f}%",
                          help="Desenvolvedores, analistas e afins contratados por bancos, "
                               "varejo, indústria, saúde — não por empresas de tecnologia.")
                st.caption(f"{fmt_num(fora)} de {fmt_num(total_prof)} admissões de "
                           "profissionais de TI no período.")

    st.markdown("**Setores que mais contratam profissionais de TI**")
    fora_setor = (lentes[lentes["categoria"].str.contains("fora")]
                  .groupby("setor_empresa", as_index=False)
                  .agg(admissoes=("admissoes", "sum"), saldo=("saldo", "sum"),
                       salario=("salario_medio", "mean"))
                  .sort_values("admissoes", ascending=False).head(10))
    if not fora_setor.empty:
        tabela = fora_setor.copy()
        tabela["Admissões"] = tabela["admissoes"].map(fmt_num)
        tabela["Saldo"] = tabela["saldo"].map(fmt_num)
        tabela["Salário médio"] = tabela["salario"].map(fmt_reais)
        st.dataframe(tabela[["setor_empresa", "Admissões", "Saldo", "Salário médio"]]
                     .rename(columns={"setor_empresa": "Setor da empresa"}),
                     width='stretch', hide_index=True)

st.divider()

# ------------------------------------------------------------ UF e ocupações
col_a, col_b = st.columns(2)

with col_a:
    st.subheader("Saldo por UF")
    if not uf_f.empty:
        rank = (uf_f.groupby("uf", as_index=False)["saldo"].sum()
                .sort_values("saldo").tail(15))
        f5 = go.Figure(go.Bar(
            x=rank["saldo"], y=rank["uf"], orientation="h",
            marker_color=[tema.POSITIVO if v >= 0 else tema.NEGATIVO for v in rank["saldo"]],
            hovertemplate="%{y}<br>Saldo: %{x:,.0f}<extra></extra>",
        ))
        lay5 = tema.layout_base(altura=420, mostrar_legenda=False)
        lay5["margin"]["l"] = 130
        f5.update_layout(**lay5)
        st.plotly_chart(f5, width='stretch')

with col_b:
    st.subheader("Ocupações de TI com maior saldo")
    ocup = por_periodo(dados.por_ocupacao(), coluna="ano")
    if not ocup.empty:
        top = (ocup.groupby("ocupacao", as_index=False)
               .agg(saldo=("saldo", "sum"), admissoes=("admissoes", "sum"),
                    salario=("salario_medio", "mean"))
               .sort_values("saldo").tail(15))
        top["rotulo"] = top["ocupacao"].str.slice(0, 40)
        f6 = go.Figure(go.Bar(
            x=top["saldo"], y=top["rotulo"], orientation="h",
            marker_color=[tema.POSITIVO if v >= 0 else tema.NEGATIVO for v in top["saldo"]],
            customdata=top[["ocupacao", "salario"]],
            hovertemplate="%{customdata[0]}<br>Saldo: %{x:,.0f}"
                          "<br>Salário médio: R$ %{customdata[1]:,.2f}<extra></extra>",
        ))
        lay6 = tema.layout_base(altura=420, mostrar_legenda=False)
        lay6["margin"]["l"] = 250
        f6.update_layout(**lay6)
        st.plotly_chart(f6, width='stretch')

st.divider()

# ------------------------------------------------------------- demografia
st.subheader("Quem é contratado em tecnologia")
demo = por_periodo(dados.demografia(), coluna="ano")

if not demo.empty:
    d1, d2, d3 = st.columns(3)

    def barra(coluna, titulo, alvo, cor):
        agg = (demo.groupby(coluna, as_index=False)
               .agg(admissoes=("admissoes", "sum"), salario=("salario_medio", "mean"))
               .sort_values("admissoes"))
        fig = go.Figure(go.Bar(
            x=agg["admissoes"], y=agg[coluna], orientation="h", marker_color=cor,
            customdata=agg["salario"],
            hovertemplate="%{y}<br>Admissões: %{x:,.0f}"
                          "<br>Salário médio: R$ %{customdata:,.2f}<extra></extra>",
        ))
        lay = tema.layout_base(altura=300, mostrar_legenda=False)
        lay["margin"]["l"] = 160
        fig.update_layout(**lay)
        alvo.markdown(f"**{titulo}**")
        alvo.plotly_chart(fig, width='stretch')

    barra("sexo", "Por sexo", d1, tema.SERIE_1)
    barra("raca_cor", "Por raça/cor", d2, tema.SERIE_3)
    barra("escolaridade", "Por escolaridade", d3, tema.SERIE_5)

    # Números exatos importam mais que forma aqui: tabela comunica melhor.
    st.markdown("**Salário médio de admissão por escolaridade**")
    sal = (demo.groupby("escolaridade", as_index=False)
           .agg(admissoes=("admissoes", "sum"), salario=("salario_medio", "mean"))
           .sort_values("salario", ascending=False))
    sal["Admissões"] = sal["admissoes"].map(fmt_num)
    sal["Salário médio"] = sal["salario"].map(fmt_reais)
    st.dataframe(sal[["escolaridade", "Admissões", "Salário médio"]]
                 .rename(columns={"escolaridade": "Escolaridade"}),
                 width='stretch', hide_index=True)

st.markdown(
    '<div class="rodape">'
    'Fonte: microdados do Novo CAGED (MTE), traduzidos pelos dicionários oficiais. '
    'Recorte de tecnologia = CNAE de serviços de TI (divisões 62/63) <b>ou</b> '
    'família CBO de ocupação de TI. '
    'Saldo = admissões − desligamentos. Salário médio considera apenas admissões '
    'com valor informado.'
    '</div>', unsafe_allow_html=True,
)
