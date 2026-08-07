"""
Dashboard do mercado de trabalho em TECNOLOGIA (CAGED, 2007–2026).

Estruturado como narrativa, não como painel de gráficos soltos: cada seção
responde uma pergunta e vem com a leitura do dado. Os números do texto são
calculados (ver narrativa.py) — atualizar a base atualiza a interpretação.

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
  .block-container {{ padding-top: 2.2rem; max-width: 1240px; }}
  [data-testid="stMetricValue"] {{ font-size: 1.75rem; font-family: {tema.FONTE}; }}
  [data-testid="stMetricLabel"] {{ color: {tema.TEXTO_SEC}; }}
  h1, h2, h3 {{ font-family: {tema.FONTE}; }}
  .lead {{ font-size: 1.05rem; line-height: 1.65; color: {tema.TEXTO_SEC};
           margin: 0 0 4px 0; }}
  .leitura {{ font-size: 0.95rem; line-height: 1.65; color: {tema.TEXTO_SEC};
              border-left: 3px solid {tema.SERIE_1}; padding: 2px 0 2px 14px;
              margin: 6px 0 18px 0; }}
  .rodape {{ color: {tema.MUTED}; font-size: 12px; margin-top: 12px; line-height: 1.6; }}
</style>
""", unsafe_allow_html=True)


def leitura(texto: str):
    """Bloco de interpretação do dado — o que a seção acima quer dizer."""
    if texto:
        st.markdown(f'<div class="leitura">{texto}</div>', unsafe_allow_html=True)


# ------------------------------------------------------------------ dados
if not dados.tem_dados():
    st.title("💻 Mercado de Trabalho em Tecnologia")
    st.warning("A silver de `caged_mov` ainda não tem dados.")
    st.stop()

tem_historico = dados.tem_serie_longa()
anual = dados.serie_longa_anual() if tem_historico else None
arco = narrativa.arco_historico(anual) if tem_historico and anual is not None else {}

# ----------------------------------------------------------------- abertura
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

st.divider()

# -------------------------------------------------- 1. a série completa
if tem_historico and anual is not None and not anual.empty:
    st.subheader("A série completa")

    por_ano = anual.groupby("ano", as_index=False).agg(
        saldo=("saldo", "sum"), admissoes=("admissoes", "sum"),
        salario_medio=("salario_medio", "mean"))
    ultimo = int(por_ano["ano"].max())

    fig = go.Figure()
    # Cor carrega o SINAL do saldo — a leitura principal do gráfico.
    # Verde/vermelho seria o par intuitivo, mas é justamente o que daltônicos
    # não distinguem, e aqui o sinal é a informação mais importante.
    fig.add_bar(
        x=por_ano["ano"], y=por_ano["saldo"],
        marker_color=[tema.POSITIVO if v >= 0 else tema.NEGATIVO for v in por_ano["saldo"]],
        hovertemplate="%{x}<br>Saldo: %{y:,.0f}<extra></extra>", name="Saldo",
    )
    # O último ano quase sempre está incompleto — sinalizar evita que ele
    # seja lido como queda real.
    fig.add_annotation(x=ultimo, y=por_ano[por_ano["ano"] == ultimo]["saldo"].iloc[0],
                       text="parcial", showarrow=True, arrowhead=0, ay=-28,
                       font=dict(size=10, color=tema.MUTED))
    lay = tema.layout_base(altura=340, mostrar_legenda=False)
    lay["yaxis"]["title"] = "vagas (líquido)"
    fig.update_layout(**lay)
    st.plotly_chart(fig, width="stretch")

    leitura(narrativa.texto_serie(arco))

    # ------------------------------------------------ 2. salário
    st.subheader("O salário nunca recuou")
    f2 = go.Figure(go.Scatter(
        x=por_ano["ano"], y=por_ano["salario_medio"], mode="lines+markers",
        line=dict(color=tema.SERIE_3, width=2), marker=dict(size=6),
        hovertemplate="%{x}<br>R$ %{y:,.2f}<extra></extra>",
    ))
    lay2 = tema.layout_base(altura=280, mostrar_legenda=False)
    lay2["yaxis"]["title"] = "R$ na admissão"
    f2.update_layout(**lay2)
    st.plotly_chart(f2, width="stretch")

    leitura(narrativa.texto_salario(arco))
    st.divider()

# ----------------------------------- 3. o achado: as duas lentes
st.subheader("Onde o trabalho de tecnologia acontece")

lentes = dados.setor_ti_vs_ocupacao_ti()
if not lentes.empty:
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

    f3 = go.Figure(go.Bar(
        x=resumo["admissoes"], y=resumo["categoria"], orientation="h",
        marker_color=[tema.SERIE_1, tema.SERIE_2, tema.SERIE_3][: len(resumo)],
        customdata=resumo["salario"],
        hovertemplate="%{y}<br>Admissões: %{x:,.0f}"
                      "<br>Salário médio: R$ %{customdata:,.2f}<extra></extra>",
    ))
    lay3 = tema.layout_base(altura=240, mostrar_legenda=False)
    lay3["margin"]["l"] = 290
    f3.update_layout(**lay3)
    st.plotly_chart(f3, width="stretch")

    st.markdown("**Quem mais contrata profissionais de TI fora do setor**")
    fora_setor = (lentes[lentes["categoria"].str.contains("fora")]
                  .groupby("setor_empresa", as_index=False)
                  .agg(admissoes=("admissoes", "sum"), saldo=("saldo", "sum"),
                       salario=("salario_medio", "mean"))
                  .sort_values("admissoes", ascending=False).head(8))
    if not fora_setor.empty:
        t = fora_setor.copy()
        t["Admissões"] = t["admissoes"].map(fmt_num)
        t["Saldo"] = t["saldo"].map(fmt_num)
        t["Salário médio"] = t["salario"].map(fmt_reais)
        st.dataframe(t[["setor_empresa", "Admissões", "Saldo", "Salário médio"]]
                     .rename(columns={"setor_empresa": "Setor da empresa"}),
                     width="stretch", hide_index=True)

st.divider()

# ------------------------------------------------ 4. recorte recente
st.subheader("O mercado hoje")
st.caption("Novo CAGED (2020+), onde o layout permite abrir por ocupação e perfil.")

mensal = dados.mensal()
por_uf = dados.mensal_por_uf()

anos = sorted(mensal["competencia"].dt.year.unique()) if not mensal.empty else []
c1, c2 = st.columns([2, 3])
with c1:
    if len(anos) > 1:
        ano_ini, ano_fim = st.select_slider("Período", options=anos,
                                            value=(anos[0], anos[-1]))
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


# Com UF selecionada o recorte vale para o bloco inteiro: a série mensal passa
# a vir de por_uf, senão os números do topo mostrariam o Brasil enquanto os
# gráficos abaixo mostrariam a seleção.
uf_f = por_periodo(por_uf)
if ufs_sel:
    uf_f = uf_f[uf_f["uf"].isin(ufs_sel)]
    mensal_f = (uf_f.groupby("competencia", as_index=False)
                .agg(admissoes=("admissoes", "sum"), desligamentos=("desligamentos", "sum"),
                     saldo=("saldo", "sum"), salario_medio=("salario_medio", "mean"),
                     idade_media=("idade_media", "mean")))
else:
    mensal_f = por_periodo(mensal)

if not mensal_f.empty:
    m1, m2, m3, m4 = st.columns(4)
    m1.metric("Saldo no período", fmt_compacto(int(mensal_f["saldo"].sum())))
    m2.metric("Admissões", fmt_compacto(int(mensal_f["admissoes"].sum())))
    m3.metric("Salário médio", fmt_reais(mensal_f["salario_medio"].mean()))
    m4.metric("Idade média", f"{mensal_f['idade_media'].mean():.0f} anos")

    g1, g2 = st.columns([3, 2])
    with g1:
        f4 = go.Figure()
        f4.add_scatter(x=mensal_f["competencia"], y=mensal_f["admissoes"], mode="lines",
                       name="Admissões", line=dict(color=tema.COR_ADMISSAO, width=2),
                       hovertemplate="%{x|%b/%Y}<br>Admissões: %{y:,.0f}<extra></extra>")
        f4.add_scatter(x=mensal_f["competencia"], y=mensal_f["desligamentos"], mode="lines",
                       name="Desligamentos", line=dict(color=tema.COR_DESLIGAMENTO, width=2),
                       hovertemplate="%{x|%b/%Y}<br>Desligamentos: %{y:,.0f}<extra></extra>")
        f4.update_layout(**tema.layout_base(altura=300))
        st.plotly_chart(f4, width="stretch")

    with g2:
        if not uf_f.empty:
            rank = (uf_f.groupby("uf", as_index=False)["saldo"].sum()
                    .sort_values("saldo").tail(10))
            f5 = go.Figure(go.Bar(
                x=rank["saldo"], y=rank["uf"], orientation="h",
                marker_color=[tema.POSITIVO if v >= 0 else tema.NEGATIVO
                              for v in rank["saldo"]],
                hovertemplate="%{y}<br>Saldo: %{x:,.0f}<extra></extra>",
            ))
            lay5 = tema.layout_base(altura=300, mostrar_legenda=False)
            lay5["margin"]["l"] = 120
            f5.update_layout(**lay5)
            st.plotly_chart(f5, width="stretch")

# ------------------------------------------------------- 5. ocupações
st.subheader("Quais ocupações puxam o saldo")
ocup = por_periodo(dados.por_ocupacao(), coluna="ano")
if not ocup.empty:
    agg = (ocup.groupby("ocupacao", as_index=False)
           .agg(saldo=("saldo", "sum"), admissoes=("admissoes", "sum"),
                salario=("salario_medio", "mean")))
    top = agg.sort_values("saldo").tail(12)
    top["rotulo"] = top["ocupacao"].str.slice(0, 42)
    f6 = go.Figure(go.Bar(
        x=top["saldo"], y=top["rotulo"], orientation="h",
        marker_color=[tema.POSITIVO if v >= 0 else tema.NEGATIVO for v in top["saldo"]],
        customdata=top[["ocupacao", "salario"]],
        hovertemplate="%{customdata[0]}<br>Saldo: %{x:,.0f}"
                      "<br>Salário médio: R$ %{customdata[1]:,.2f}<extra></extra>",
    ))
    lay6 = tema.layout_base(altura=380, mostrar_legenda=False)
    lay6["margin"]["l"] = 260
    f6.update_layout(**lay6)
    st.plotly_chart(f6, width="stretch")

st.divider()

# ------------------------------------------------------- 6. demografia
st.subheader("Quem é contratado")
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
        lay = tema.layout_base(altura=290, mostrar_legenda=False)
        lay["margin"]["l"] = 155
        fig.update_layout(**lay)
        alvo.markdown(f"**{titulo}**")
        alvo.plotly_chart(fig, width="stretch")

    barra("sexo", "Por sexo", d1, tema.SERIE_1)
    barra("raca_cor", "Por raça/cor", d2, tema.SERIE_3)
    barra("escolaridade", "Por escolaridade", d3, tema.SERIE_5)

    # Números exatos importam mais que a forma aqui — tabela comunica melhor.
    sal = (demo.groupby("escolaridade", as_index=False)
           .agg(admissoes=("admissoes", "sum"), salario=("salario_medio", "mean"))
           .sort_values("salario", ascending=False))
    if len(sal) > 1:
        topo, base = sal.iloc[0], sal.iloc[-1]
        if base["salario"]:
            leitura(f"A diferença entre o topo e a base da escolaridade é de "
                    f"**{topo['salario'] / base['salario']:.1f}×** no salário de "
                    f"admissão: {fmt_reais(topo['salario'])} para *{topo['escolaridade']}* "
                    f"contra {fmt_reais(base['salario'])} para *{base['escolaridade']}*.")

    sal["Admissões"] = sal["admissoes"].map(fmt_num)
    sal["Salário médio"] = sal["salario"].map(fmt_reais)
    st.dataframe(sal[["escolaridade", "Admissões", "Salário médio"]]
                 .rename(columns={"escolaridade": "Escolaridade"}),
                 width="stretch", hide_index=True)

st.markdown(
    f'<div class="rodape">'
    f'Fonte: microdados do CAGED (Ministério do Trabalho e Emprego), traduzidos pelos '
    f'dicionários oficiais. Recorte de tecnologia = CNAE de serviços de TI (divisões '
    f'62/63) <b>ou</b> família CBO de ocupação de TI. '
    f'Saldo = admissões − desligamentos; salário médio considera apenas admissões com '
    f'valor informado, sem correção pela inflação. '
    f'Dados: {dados.fonte_atual()}.'
    f'</div>', unsafe_allow_html=True,
)
