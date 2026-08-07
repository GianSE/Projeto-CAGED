"""
Dashboard do mercado de trabalho formal brasileiro (CAGED).

Lê a camada gold (agregados) via DuckDB. Rodar com:

    cd tasks_python
    ..\\.venv\\Scripts\\streamlit run dashboard/app.py

Pré-requisito: a gold precisa existir.
    python -m gold_caged.construir_gold
"""
import plotly.graph_objects as go
import streamlit as st

from dashboard import dados, tema
from dashboard.tema import fmt_compacto, fmt_num, fmt_reais

st.set_page_config(page_title="Mercado de Trabalho — CAGED", page_icon="📊", layout="wide")

st.markdown(f"""
<style>
  .block-container {{ padding-top: 2.2rem; max-width: 1400px; }}
  [data-testid="stMetricValue"] {{ font-size: 1.9rem; font-family: {tema.FONTE}; }}
  [data-testid="stMetricLabel"] {{ color: {tema.TEXTO_SEC}; }}
  h1, h2, h3 {{ font-family: {tema.FONTE}; }}
  .rodape {{ color: {tema.MUTED}; font-size: 12px; margin-top: 8px; }}
</style>
""", unsafe_allow_html=True)


# --------------------------------------------------------------- carregamento
disponiveis = dados.agregados_disponiveis()
if not any(disponiveis.values()):
    st.title("📊 Mercado de Trabalho — CAGED")
    st.warning(
        "A camada **gold** ainda não foi construída.\n\n"
        "Rode `python -m gold_caged.construir_gold` (a partir de `tasks_python`) "
        "depois que a silver estiver pronta."
    )
    st.stop()

mensal = dados.carregar("saldo_mensal")
por_uf = dados.carregar("saldo_uf")
por_setor = dados.carregar("saldo_setor")
perfil = dados.carregar("perfil_demografico")
ocupacoes = dados.carregar("ocupacoes")

st.title("📊 Mercado de Trabalho Formal — CAGED")
st.caption("Novo CAGED (2020+) · saldo = admissões − desligamentos · fonte: microdados do MTE")

# ------------------------------------------------------------------- filtros
base_periodo = mensal if not mensal.empty else por_uf
anos = sorted(base_periodo["competencia"].dt.year.unique()) if not base_periodo.empty else []

col_f1, col_f2 = st.columns([2, 3])
with col_f1:
    if len(anos) > 1:
        ano_ini, ano_fim = st.select_slider(
            "Período", options=anos, value=(anos[0], anos[-1]),
        )
    else:
        ano_ini = ano_fim = anos[0] if anos else None
with col_f2:
    ufs = sorted(por_uf["uf"].dropna().unique()) if not por_uf.empty else []
    ufs_sel = st.multiselect("UF (vazio = Brasil)", ufs, default=[])


def filtrar_periodo(df, coluna="competencia"):
    if df.empty or ano_ini is None:
        return df
    if coluna == "competencia":
        anos_df = df[coluna].dt.year
    else:
        anos_df = df[coluna]
    return df[(anos_df >= ano_ini) & (anos_df <= ano_fim)]


# Com UF selecionada, o recorte territorial vale para todo o painel: a série
# mensal passa a vir de saldo_uf (que tem a dimensão UF) em vez de saldo_mensal,
# senão os KPIs mostrariam o Brasil enquanto o resto mostraria a seleção.
uf_ativa = bool(ufs_sel)
por_uf_f = filtrar_periodo(por_uf)
if uf_ativa:
    por_uf_f = por_uf_f[por_uf_f["uf"].isin(ufs_sel)]
    mensal_f = (por_uf_f.groupby("competencia", as_index=False)
                .agg(admissoes=("admissoes", "sum"),
                     desligamentos=("desligamentos", "sum"),
                     saldo=("saldo", "sum"),
                     salario_medio_admissao=("salario_medio_admissao", "mean")))
else:
    mensal_f = filtrar_periodo(mensal)

# ---------------------------------------------------------------------- KPIs
if not mensal_f.empty:
    tot_adm = int(mensal_f["admissoes"].sum())
    tot_desl = int(mensal_f["desligamentos"].sum())
    saldo = int(mensal_f["saldo"].sum())
    sal_medio = mensal_f["salario_medio_admissao"].mean()

    k1, k2, k3, k4 = st.columns(4)
    k1.metric("Saldo de empregos", fmt_compacto(saldo),
              delta=f"{saldo / tot_adm * 100:.1f}% das admissões" if tot_adm else None)
    k2.metric("Admissões", fmt_compacto(tot_adm))
    k3.metric("Desligamentos", fmt_compacto(tot_desl))
    k4.metric("Salário médio de admissão", fmt_reais(sal_medio))

st.divider()

# ------------------------------------------------- série temporal do saldo
if not mensal_f.empty:
    st.subheader("Saldo mensal de empregos")

    fig = go.Figure()
    # Barra divergente: a cor carrega o SINAL (azul cria vaga, vermelho perde),
    # que é a leitura mais importante do gráfico. Verde/vermelho seria intuitivo
    # mas é o par que daltônicos não separam.
    cores = [tema.POSITIVO if v >= 0 else tema.NEGATIVO for v in mensal_f["saldo"]]
    fig.add_bar(
        x=mensal_f["competencia"], y=mensal_f["saldo"], marker_color=cores,
        name="Saldo",
        hovertemplate="%{x|%b/%Y}<br>Saldo: %{y:,.0f}<extra></extra>",
    )
    layout = tema.layout_base(altura=320, mostrar_legenda=False)
    layout["yaxis"]["title"] = "vagas (líquido)"
    fig.update_layout(**layout)
    st.plotly_chart(fig, use_container_width=True)

    # Fluxo bruto: duas séries de identidade, dois slots categóricos fixos.
    st.subheader("Admissões e desligamentos")
    fig2 = go.Figure()
    fig2.add_scatter(x=mensal_f["competencia"], y=mensal_f["admissoes"],
                     mode="lines", name="Admissões",
                     line=dict(color=tema.COR_ADMISSAO, width=2),
                     hovertemplate="%{x|%b/%Y}<br>Admissões: %{y:,.0f}<extra></extra>")
    fig2.add_scatter(x=mensal_f["competencia"], y=mensal_f["desligamentos"],
                     mode="lines", name="Desligamentos",
                     line=dict(color=tema.COR_DESLIGAMENTO, width=2),
                     hovertemplate="%{x|%b/%Y}<br>Desligamentos: %{y:,.0f}<extra></extra>")
    fig2.update_layout(**tema.layout_base(altura=300))
    st.plotly_chart(fig2, use_container_width=True)

st.divider()

# --------------------------------------------------------- UF e setor
col_a, col_b = st.columns(2)

with col_a:
    st.subheader("Saldo por UF")
    if not por_uf_f.empty:
        rank = (por_uf_f.groupby("uf", as_index=False)["saldo"].sum()
                .sort_values("saldo", ascending=True).tail(15))
        fig3 = go.Figure(go.Bar(
            x=rank["saldo"], y=rank["uf"], orientation="h",
            marker_color=[tema.POSITIVO if v >= 0 else tema.NEGATIVO for v in rank["saldo"]],
            hovertemplate="%{y}<br>Saldo: %{x:,.0f}<extra></extra>",
        ))
        layout = tema.layout_base(altura=420, mostrar_legenda=False)
        layout["margin"]["l"] = 130
        fig3.update_layout(**layout)
        st.plotly_chart(fig3, use_container_width=True)

with col_b:
    st.subheader("Saldo por setor (seção CNAE)")
    setor_f = filtrar_periodo(por_setor)
    if not setor_f.empty:
        rank_s = (setor_f.groupby("setor", as_index=False)["saldo"].sum()
                  .sort_values("saldo", ascending=True).tail(15))
        rank_s["rotulo"] = rank_s["setor"].str.slice(0, 42)
        fig4 = go.Figure(go.Bar(
            x=rank_s["saldo"], y=rank_s["rotulo"], orientation="h",
            marker_color=[tema.POSITIVO if v >= 0 else tema.NEGATIVO for v in rank_s["saldo"]],
            customdata=rank_s["setor"],
            hovertemplate="%{customdata}<br>Saldo: %{x:,.0f}<extra></extra>",
        ))
        layout = tema.layout_base(altura=420, mostrar_legenda=False)
        layout["margin"]["l"] = 260
        fig4.update_layout(**layout)
        st.plotly_chart(fig4, use_container_width=True)
    else:
        st.info("Agregado de setor ainda não construído.")

st.divider()

# ------------------------------------------------------------- demografia
st.subheader("Quem está sendo contratado")
perfil_f = filtrar_periodo(perfil)

if not perfil_f.empty:
    d1, d2, d3 = st.columns(3)

    def barra_dimensao(coluna, titulo, container, cor):
        agg = (perfil_f.groupby(coluna, as_index=False)
               .agg(admissoes=("admissoes", "sum"),
                    salario=("salario_medio_admissao", "mean"))
               .sort_values("admissoes", ascending=True))
        fig = go.Figure(go.Bar(
            x=agg["admissoes"], y=agg[coluna], orientation="h", marker_color=cor,
            customdata=agg["salario"],
            hovertemplate="%{y}<br>Admissões: %{x:,.0f}"
                          "<br>Salário médio: R$ %{customdata:,.2f}<extra></extra>",
        ))
        layout = tema.layout_base(altura=300, mostrar_legenda=False)
        layout["margin"]["l"] = 150
        fig.update_layout(**layout)
        container.markdown(f"**{titulo}**")
        container.plotly_chart(fig, use_container_width=True)

    barra_dimensao("sexo", "Por sexo", d1, tema.SERIE_1)
    barra_dimensao("raca_cor", "Por raça/cor", d2, tema.SERIE_3)
    barra_dimensao("escolaridade", "Por escolaridade", d3, tema.SERIE_5)

    # Salário por escolaridade: aqui o dado é magnitude comparável entre
    # categorias, e a tabela comunica melhor que um gráfico — números exatos
    # importam mais que a forma.
    st.markdown("**Salário médio de admissão por escolaridade**")
    sal_esc = (perfil_f.groupby("escolaridade", as_index=False)
               .agg(admissoes=("admissoes", "sum"),
                    salario_medio=("salario_medio_admissao", "mean"))
               .sort_values("salario_medio", ascending=False))
    sal_esc["salario_medio"] = sal_esc["salario_medio"].map(fmt_reais)
    sal_esc["admissoes"] = sal_esc["admissoes"].map(fmt_num)
    st.dataframe(
        sal_esc.rename(columns={"escolaridade": "Escolaridade",
                                "admissoes": "Admissões",
                                "salario_medio": "Salário médio"}),
        use_container_width=True, hide_index=True,
    )

st.divider()

# -------------------------------------------------------------- ocupações
st.subheader("Ocupações em destaque")
ocup_f = filtrar_periodo(ocupacoes, coluna="ano")

if not ocup_f.empty:
    agg_ocup = (ocup_f.groupby("ocupacao", as_index=False)
                .agg(saldo=("saldo", "sum"),
                     admissoes=("admissoes", "sum"),
                     salario=("salario_medio_admissao", "mean")))

    o1, o2 = st.columns(2)
    for container, titulo, asc in ((o1, "Maiores saldos positivos", False),
                                   (o2, "Maiores saldos negativos", True)):
        top = agg_ocup.sort_values("saldo", ascending=asc).head(10).copy()
        top["Saldo"] = top["saldo"].map(fmt_num)
        top["Salário médio"] = top["salario"].map(fmt_reais)
        container.markdown(f"**{titulo}**")
        container.dataframe(
            top[["ocupacao", "Saldo", "Salário médio"]]
            .rename(columns={"ocupacao": "Ocupação"}),
            use_container_width=True, hide_index=True,
        )

st.markdown(
    '<div class="rodape">Camada gold pré-agregada a partir da silver traduzida pelos '
    'dicionários oficiais do MTE. Saldo = admissões − desligamentos. '
    'Salário médio considera apenas admissões com valor informado.</div>',
    unsafe_allow_html=True,
)
