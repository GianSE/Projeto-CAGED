"""
Abas de modelagem: previsão, nowcast, sobrevivência, hiato e agrupamento.

O QUE ESTA PARTE DO DASHBOARD TENTA EVITAR
------------------------------------------
Gráfico de previsão sem intervalo, e número de modelo sem comparação com
baseline. As duas coisas dão ao leitor uma confiança que o dado não sustenta, e
são o modo mais comum de um trabalho quantitativo enganar sem mentir.

Por isso, aqui: toda previsão aparece com a faixa; todo modelo aparece ao lado
da regra ingênua que ele precisou superar; e toda leitura diz o que o número
NÃO significa.
"""
import numpy as np
import plotly.graph_objects as go
import streamlit as st

from dashboard import dados_modelos as dm
from dashboard import tema
from dashboard.tema import fmt_compacto, fmt_num


def _leitura(texto: str):
    if texto:
        st.markdown(f'<div class="leitura">{texto}</div>', unsafe_allow_html=True)


def _faltando(comando: str):
    st.info(f"Este modelo ainda não foi materializado. Rode `{comando}`.")


# ============================================================ 1. PREVISÃO
def previsao():
    prev, placar = dm.previsao(), dm.previsao_placar()
    serie = dm.serie_mensal()
    if prev.empty or serie.empty:
        _faltando("python -m ciencia_dados.materializar")
        return

    st.subheader("Saldo mensal: observado e projetado")

    hist = serie.tail(72)
    fig = go.Figure()
    # A faixa vem ANTES da linha para ficar atrás dela no desenho.
    fig.add_trace(go.Scatter(
        x=list(prev["mes"]) + list(prev["mes"][::-1]),
        y=list(prev["superior"]) + list(prev["inferior"][::-1]),
        fill="toself", fillcolor="rgba(120,140,200,0.18)",
        line=dict(width=0), hoverinfo="skip", name="Intervalo de 80%"))
    fig.add_trace(go.Scatter(
        x=hist["mes"], y=hist["saldo"], mode="lines", name="Observado",
        line=dict(color=tema.SERIE_1, width=2),
        hovertemplate="%{x|%Y-%m}<br>saldo: %{y:,.0f}<extra></extra>"))
    fig.add_trace(go.Scatter(
        x=prev["mes"], y=prev["previsao"], mode="lines+markers", name="Previsto",
        line=dict(color=tema.SERIE_2, width=2.5, dash="dot"),
        hovertemplate="%{x|%Y-%m}<br>previsto: %{y:,.0f}<extra></extra>"))
    fig.add_hline(y=0, line_width=1, line_color=tema.GRID)
    fig.update_layout(**tema.layout_base(altura=420, mostrar_legenda=True))
    st.plotly_chart(fig, width='stretch')

    modelo = prev["modelo"].iloc[0] if "modelo" in prev else "—"
    acum = prev["previsao"].sum()
    faixa = (prev["inferior"].sum(), prev["superior"].sum())
    k1, k2, k3 = st.columns(3)
    k1.metric("Modelo escolhido", modelo)
    k2.metric(f"Acumulado em {len(prev)} meses", fmt_compacto(acum))
    k3.metric("Faixa de 80%",
              f"{fmt_compacto(faixa[0])} a {fmt_compacto(faixa[1])}")

    if not placar.empty:
        st.subheader("Por que este modelo, e não outro")
        st.dataframe(placar, width='stretch', hide_index=True)
        base = placar[placar["modelo"] == "naive sazonal"]
        ganho = placar.iloc[0].get("ganho_vs_naive_%", 0)
        _leitura(
            "A escolha é por <strong>validação em origem móvel</strong>, não por "
            "AIC: treina até um ponto, prevê doze meses, anda o ponto, repete. "
            f"O vencedor erra {ganho:.1f}% menos que a regra ingênua "
            "&ldquo;igual ao mesmo mês do ano passado&rdquo;. "
            "Essa regra está na tabela de propósito — a série tem sazonalidade "
            "forte, e um modelo que não a superasse estaria só redescobrindo o "
            "calendário. Repare que um dos candidatos fica <em>abaixo</em> dela."
        )

    st.caption("A faixa é larga porque saldo mensal de emprego é ruidoso. Ler o "
               "ponto isolado como previsão seria o erro que este painel evita: "
               "o número certo é o intervalo.")


# ============================================================= 2. NOWCAST
def nowcast():
    retro, retido = dm.nowcast_retrospectiva(), dm.nowcast_ano_retido()
    proj, pares = dm.nowcast_projecao(), dm.nowcast_pares()
    if retro.empty or pares.empty:
        _faltando("python -m ciencia_dados.materializar")
        return

    st.subheader("Estimar a RAIS antes de ela sair")
    _leitura(
        "A RAIS é anual e sai com cerca de um ano de atraso; o CAGED é mensal e "
        "está sempre em dia. Existe uma janela de quase dois anos em que se sabe "
        "quanto o mercado se moveu, mas não de que tamanho ele ficou. "
        "A correlação entre a variação do estoque e o saldo acumulado é de "
        f"<strong>{pares['delta'].corr(pares['fluxo']):.2f}</strong> — forte o "
        "bastante para estimar, não para somar."
    )

    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=pares["fluxo"], y=pares["delta"], mode="markers+text",
        text=pares["ano"].astype(int).astype(str), textposition="top center",
        textfont=dict(size=9, color=tema.TEXTO_SEC),
        marker=dict(size=11, color=tema.SERIE_1, opacity=0.8),
        hovertemplate="%{text}<br>fluxo CAGED: %{x:,.0f}"
                      "<br>variação RAIS: %{y:,.0f}<extra></extra>",
        name="ano"))
    limite = [pares["fluxo"].min(), pares["fluxo"].max()]
    fig.add_trace(go.Scatter(x=limite, y=limite, mode="lines", name="identidade (1:1)",
                             line=dict(color=tema.SERIE_3, dash="dash", width=1.5)))
    lay = tema.layout_base(altura=400, mostrar_legenda=True)
    fig.update_layout(**lay)
    fig.update_xaxes(title="Saldo acumulado do CAGED no ano")
    fig.update_yaxes(title="Variação do estoque da RAIS")
    st.plotly_chart(fig, width='stretch')

    col1, col2 = st.columns(2)
    with col1:
        st.caption("**Retrospectiva** — cada ano previsto só com os anteriores")
        st.dataframe(retro, width='stretch', hide_index=True)
    with col2:
        if not retido.empty:
            ano = int(retido["ano_teste"].iloc[0])
            st.caption(f"**Ano retido: {ano}** — treinado só até {ano - 1}")
            st.dataframe(retido.drop(columns=["ano_teste"]),
                         width='stretch', hide_index=True)

    vencedor_retro = retro.iloc[0]["estimador"]
    _leitura(
        f"Na retrospectiva de {int(retro.iloc[0]['anos'])} anos quem vence é "
        f"<strong>{vencedor_retro}</strong>. A regressão fica atrás: com 18 "
        "pontos ela superajusta. O achado defensável não é que exista um modelo "
        "sofisticado — é que <strong>o saldo do CAGED acompanha a variação do "
        "estoque da RAIS quase 1:1</strong>. Publicar a regressão como se fosse "
        "melhor seria escolher o estimador pior por parecer mais técnico."
    )

    if not proj.empty:
        p = proj.iloc[0]
        st.subheader(f"Projeção para {int(p['ano'])}")
        k1, k2, k3 = st.columns(3)
        k1.metric(f"CAGED observado ({int(p['meses_observados'])} meses)",
                  fmt_compacto(p["fluxo_observado"]))
        k2.metric("CAGED previsto (resto do ano)", fmt_compacto(p["fluxo_previsto"]))
        k3.metric(f"Estoque estimado em {int(p['ano'])}",
                  fmt_compacto(p["estoque_estimado"]),
                  f"± {fmt_compacto(p['margem'])}")
        st.caption(
            f"O MTE só publica esse número em {int(p['ano']) + 1}. Quando sair, "
            "dá para conferir o acerto — o que transforma a projeção em "
            "validação prospectiva. A margem empilha duas incertezas (o fluxo "
            "previsto e a relação fluxo→estoque) somadas em quadratura; é ordem "
            "de grandeza, não intervalo exato."
        )


# ======================================================= 3. SOBREVIVÊNCIA
def sobrevivencia():
    risco, tabua = dm.sobrevivencia_risco(), dm.sobrevivencia_tabua("todos")
    if risco.empty:
        _faltando("python -m ciencia_dados.materializar")
        return

    ano = int(risco["ano"].iloc[0])
    st.subheader(f"Quanto dura um emprego de tecnologia — {ano}")

    col1, col2 = st.columns(2)
    with col1:
        st.caption("**Risco anual de desligamento** por tempo de casa")
        fig = go.Figure(go.Bar(
            x=risco["tempo_de_casa"], y=risco["risco_%"],
            marker_color=tema.SERIE_2,
            hovertemplate="%{x}<br>%{y:.1f}% ao ano<extra></extra>"))
        fig.update_layout(**tema.layout_base(altura=340, mostrar_legenda=False))
        st.plotly_chart(fig, width='stretch')
    with col2:
        st.caption("**Sobrevivência acumulada** do vínculo")
        if not tabua.empty:
            faixas = [c for c in tabua.columns
                      if c not in ("grupo", "n", "eventos", "mediana_em",
                                   "recorte", "ano")]
            linha = tabua.iloc[0]
            fig = go.Figure(go.Scatter(
                x=faixas, y=[linha[f] for f in faixas], mode="lines+markers",
                line=dict(color=tema.SERIE_1, width=2.5),
                hovertemplate="%{x}<br>%{y:.1f}% ainda no emprego<extra></extra>"))
            fig.add_hline(y=50, line_dash="dot", line_color=tema.GRID)
            fig.update_layout(**tema.layout_base(altura=340, mostrar_legenda=False))
            st.plotly_chart(fig, width='stretch')

    pico = risco.loc[risco["risco_%"].idxmax()]
    fim = risco.iloc[-1]
    _leitura(
        f"O risco tem pico em <strong>{pico['tempo_de_casa']}</strong> "
        f"({pico['risco_%']:.1f}% ao ano) e cai monotonicamente até "
        f"{fim['risco_%']:.1f}% depois de dez anos. É dependência negativa de "
        "duração: quanto mais tempo o vínculo dura, menos provável que termine. "
        "O CAGED não permite esta leitura — ele registra a movimentação, não a "
        "duração."
    )

    for recorte, titulo in (("porte", "Por porte do estabelecimento"),
                            ("area", "Por área de atuação"),
                            ("setor", "Dentro e fora do setor de TI"),
                            ("sexo", "Por sexo")):
        t = dm.sobrevivencia_tabua(recorte)
        if t.empty:
            continue
        st.caption(f"**{titulo}**")
        colunas = ["grupo", "n", "eventos", "5-10a", "10-20a", "mediana_em"]
        st.dataframe(t[[c for c in colunas if c in t.columns]],
                     width='stretch', hide_index=True)

    multi = dm.sobrevivencia_multivariado()
    if not multi.empty:
        st.subheader("O que muda o risco, controlando pelo resto")
        st.dataframe(multi.head(14), width='stretch', hide_index=True)
        st.caption(
            "Modelo de risco em tempo discreto, não Cox: o Cox monta o conjunto "
            "de risco pela duração observada, e num corte transversal isso "
            "supõe que quem tem dez anos de casa estava em risco aos doze meses "
            "— onde nunca esteve durante o ano observado. Razão de chances "
            "acima de 1 = mais risco de desligamento."
        )

    st.caption("Leitura de período: as taxas são de um ano só, como numa tábua "
               "de vida demográfica. Responde &ldquo;como seria a trajetória se "
               "as taxas de hoje valessem sempre&rdquo;, não o que aconteceu "
               "com quem entrou em 2015.")


# ============================================================== 4. HIATO
def hiato():
    comparacoes = dm.comparacoes_do_hiato()
    if not comparacoes:
        _faltando("python -m ciencia_dados.hiato_salarial --serie")
        return

    st.subheader("Hiato salarial: o que a composição explica e o que sobra")
    escolha = st.selectbox("Comparação", comparacoes)
    df = dm.hiato(escolha).sort_values("ano")
    if df.empty:
        return

    fig = go.Figure()
    fig.add_trace(go.Bar(x=df["ano"], y=df["explicada"], name="Explicada",
                         marker_color=tema.SERIE_1,
                         hovertemplate="%{x}<br>explicada: %{y:.4f}<extra></extra>"))
    fig.add_trace(go.Bar(x=df["ano"], y=df["nao_explicada"], name="Não explicada",
                         marker_color=tema.SERIE_2,
                         hovertemplate="%{x}<br>não explicada: %{y:.4f}<extra></extra>"))
    fig.add_trace(go.Scatter(x=df["ano"], y=df["hiato_log"], name="Hiato total",
                             mode="lines+markers",
                             line=dict(color=tema.TEXTO, width=2)))
    fig.add_hline(y=0, line_width=1, line_color=tema.GRID)
    fig.update_layout(barmode="relative",
                      **tema.layout_base(altura=420, mostrar_legenda=True))
    st.plotly_chart(fig, width='stretch')

    primeiro, ultimo = df.iloc[0], df.iloc[-1]
    k1, k2, k3 = st.columns(3)
    k1.metric(f"Hiato em {int(ultimo['ano'])}", f"{ultimo['hiato_pct']:+.1f}%",
              f"{ultimo['hiato_pct'] - primeiro['hiato_pct']:+.1f} p.p. "
              f"desde {int(primeiro['ano'])}")
    k2.metric("Parte não explicada", f"{ultimo['nao_explicada']:.4f} log",
              f"{ultimo['nao_explicada'] - primeiro['nao_explicada']:+.4f}")
    k3.metric("Parte explicada", f"{ultimo['explicada']:.4f} log")

    if ultimo["explicada"] < 0:
        _leitura(
            "A parte explicada é <strong>negativa</strong>, e esse sinal é o "
            "achado. As mulheres em tecnologia têm características que deveriam "
            "fazê-las ganhar <em>mais</em> — mais escolaridade, empresas "
            "maiores, áreas melhor pagas. Se só as características contassem, "
            f"elas ganhariam {abs(ultimo['explicada']):.4f} log acima dos "
            f"homens; ganham {ultimo['hiato_log']:.4f} abaixo. "
            f"O hiato bruto de {ultimo['hiato_pct']:.1f}% "
            "<strong>subestima</strong> a diferença de retorno, que é de "
            f"{ultimo['nao_explicada']:.4f} log."
        )
    else:
        share = ultimo["explicada_share"]
        _leitura(
            f"Em {int(ultimo['ano'])}, "
            f"<strong>{share:.0f}% do hiato</strong> vem de diferença de "
            "características — onde se trabalha, em quê, com qual formação. "
            f"O restante ({100 - share:.0f}%) é diferença de retorno: mesma "
            "escolaridade, mesmo tempo de casa, mesma ocupação, e ainda assim "
            "salários diferentes."
        )

    st.dataframe(
        df[["ano", "salario_a", "salario_b", "hiato_pct", "explicada",
            "nao_explicada", "explicada_share"]],
        width='stretch', hide_index=True)

    st.warning(
        "**A parte não explicada não é medida de discriminação.** Ela contém "
        "tudo que afeta salário e não está no modelo: experiência anterior, "
        "interrupções de carreira, senioridade dentro do cargo, empresa "
        "específica. O que se pode afirmar é o limite superior da diferença de "
        "retorno — não a sua causa."
    )


# ========================================================== 5. AGRUPAMENTO
def clusters():
    df = dm.clusters()
    if df.empty:
        _faltando("python -m ciencia_dados.clusters_municipios")
        return

    st.subheader("Municípios agrupados por trajetória")
    _leitura(
        "Ordenar municípios por estoque devolve sempre a lista das cidades "
        "grandes — o ranking acaba medindo população. O agrupamento pergunta "
        "outra coisa: quais municípios se <strong>parecem</strong> na forma "
        "como o mercado de TI se comporta neles. Vínculos por mil habitantes "
        "entra justamente para separar polo de tecnologia de cidade grande "
        "qualquer, e só existe porque a população vem do IBGE."
    )

    perfil = df.groupby("nome").agg(
        municipios=("cod_municipio", "count"),
        estoque_total=("estoque", "sum"),
        estoque_mediano=("estoque", "median"),
        por_mil_hab=("por_mil_hab", "median"),
        crescimento=("crescimento", "median"),
        remuneracao=("remuneracao_sm_mediana", "median"),
    ).reset_index().sort_values("estoque_total", ascending=False)
    st.dataframe(perfil, width='stretch', hide_index=True)

    fig = go.Figure()
    for i, (nome, sub) in enumerate(df.groupby("nome")):
        fig.add_trace(go.Scatter(
            x=sub["por_mil_hab"], y=sub["crescimento"] * 100,
            mode="markers", name=str(nome),
            text=sub["municipio"],
            marker=dict(size=np.sqrt(sub["estoque"]) / 8 + 4,
                        color=tema.CATEGORICA[i % 5], opacity=0.6),
            hovertemplate=("<b>%{text}</b><br>%{x:.1f} por mil hab"
                           "<br>crescimento: %{y:.0f}%<extra></extra>")))
    lay = tema.layout_base(altura=460, mostrar_legenda=True)
    fig.update_layout(**lay)
    fig.update_xaxes(title="Vínculos de TI por mil habitantes (especialização)")
    fig.update_yaxes(title="Crescimento do estoque na janela (%)")
    st.plotly_chart(fig, width='stretch')

    st.caption("Tamanho da bolha = estoque. k escolhido por silhueta, não por "
               "cotovelo lido a olho.")

    grupo = st.selectbox("Ver municípios do grupo", sorted(df["nome"].unique()))
    st.dataframe(
        df[df["nome"] == grupo]
        .nlargest(25, "estoque")[["municipio", "uf", "estoque", "por_mil_hab",
                                  "crescimento", "remuneracao_sm_mediana"]],
        width='stretch', hide_index=True)

    st.warning(
        "**Localização é a do estabelecimento.** A empresa de TI escolhe onde "
        "se registrar por causa do ISS, e municípios de incentivo fiscal "
        "aparecem com concentração irreal — Barueri tem 200 vínculos de TI por "
        "mil habitantes. O agrupamento exige estoque mínimo também no ano-base, "
        "o que remove o caso grosseiro, mas não corrige o fenômeno."
    )
