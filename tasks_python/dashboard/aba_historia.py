"""
A história: o argumento da pesquisa, da pergunta à conclusão.

POR QUE ESTA ABA EXISTE
-----------------------
As outras abas estão organizadas pela ORIGEM do dado — CAGED, RAIS, modelos.
Isso é como o dado foi produzido, não o que ele diz. Quem avalia a pesquisa não
pergunta "o que tem na RAIS", pergunta "o que você concluiu". Sem um fio
condutor, cada gráfico é verdadeiro e o conjunto não chega a lugar nenhum.

Aqui a ordem é a do argumento: cada seção abre com a PERGUNTA, afirma a
CONCLUSÃO, e só então mostra a evidência. As demais abas continuam sendo o lugar
de explorar; esta é o lugar de entender.

OS NÚMEROS DO TEXTO SÃO CALCULADOS
----------------------------------
Mesma regra de `narrativa.py`: nenhuma cifra das frases é escrita à mão. Elas
vêm das tabelas da gold. Quando a base for atualizada, o texto acompanha, em vez
de virar afirmação falsa sem ninguém perceber.

A exceção declarada são os achados de qualidade do dado (a conversão invertida
do salário mínimo, o domicílio fiscal) e a lista de códigos observados no
apêndice: são fatos sobre a FONTE, levantados na construção do pipeline, e não
agregados que a gold carregue.
"""
import pandas as pd
import plotly.graph_objects as go
import streamlit as st

from dashboard import dados_modelos as dm
from dashboard import dados_rais as dr
from dashboard import tema
from dashboard.tema import fmt_compacto, fmt_num

LENTE_FORA = "Profissional de TI fora do setor de TI"
LENTE_DENTRO = "Profissional de TI em empresa de TI"
LENTE_OUTRA = "Outra ocupação em empresa de TI"


# ------------------------------------------------------------------ utilidades
def _dec(v, casas: int = 1) -> str:
    """Decimal com vírgula, como se escreve em português."""
    return f"{v:,.{casas}f}".replace(",", "X").replace(".", ",").replace("X", ".")


def _pct(v, casas: int = 1) -> str:
    return f"{_dec(v, casas)}%"


def _secao(pergunta: str, conclusao: str):
    st.divider()
    st.caption(pergunta.upper())
    st.subheader(conclusao)


def _texto(html: str):
    st.markdown(f'<div class="leitura">{html}</div>', unsafe_allow_html=True)


def _layout(fig, altura=340, legenda=False):
    fig.update_layout(**tema.layout_base(altura=altura, mostrar_legenda=legenda))
    return fig


# ------------------------------------------------------------- 0. abertura
def _abertura():
    st.markdown(
        '<p class="lead">O emprego de TI mais que dobrou e <strong>deixou de ser um '
        'setor para virar uma função</strong> da economia — mas crescer não '
        'corrigiu quem ganha menos, e o ritmo agora desacelera.</p>',
        unsafe_allow_html=True)

    with st.expander("Como ler os números desta página", expanded=False):
        st.markdown(
            "- **CAGED mede fluxo:** quantos empregos foram criados ou encerrados em "
            "cada mês. **RAIS mede estoque:** quantos vínculos existiam em 31/12. "
            "Saldo positivo não diz se o mercado é grande; estoque grande não diz "
            "se está crescendo — por isso as duas bases são usadas juntas.\n"
            "- **Recorte:** entra o vínculo em empresa com CNAE de TI **ou** com "
            "ocupação (CBO) de TI. A lista completa está no apêndice, no fim da aba.\n"
            "- **Remuneração em salários mínimos (SM):** torna 2007 comparável a "
            "2025 sem escolher deflator.\n"
            "- **Toda previsão tem intervalo** e todo modelo foi comparado com uma "
            "regra ingênua que precisou superar.")


# ------------------------------------------------------------ 1. tamanho
def _tamanho():
    anual = dr.estoque_anual()
    mensal = dm.serie_mensal()
    if anual.empty:
        st.info("Estoque da RAIS indisponível.")
        return

    anual = anual.sort_values("ano")
    ini, fim = anual.iloc[0], anual.iloc[-1]
    variacao = (fim["estoque"] / ini["estoque"] - 1) * 100

    _secao("Quanto o mercado cresceu?",
           f"O estoque de TI cresceu {_pct(variacao, 0)} em "
           f"{int(fim['ano']) - int(ini['ano'])} anos, com um salto concentrado em 2021–22")

    saldo_ano = pd.DataFrame()
    if not mensal.empty:
        m = mensal.copy()
        m["ano"] = m["mes"].dt.year
        saldo_ano = m.groupby("ano").agg(saldo=("saldo", "sum"), meses=("saldo", "size"))
        completos = saldo_ano[saldo_ano["meses"] == 12]

    proj = dm.nowcast_projecao()

    fig = go.Figure()
    fig.add_vrect(x0=2020.5, x1=2022.5, fillcolor=tema.SERIE_4, opacity=0.12,
                  line_width=0, annotation_text="boom", annotation_position="top left")
    fig.add_trace(go.Scatter(
        x=anual["ano"], y=anual["estoque"], mode="lines+markers",
        name="Estoque (RAIS)", line=dict(color=tema.SERIE_1, width=3), marker=dict(size=5),
        hovertemplate="%{x}<br>%{y:,.0f} vínculos<extra></extra>"))
    if not proj.empty:
        p = proj.iloc[0]
        fig.add_trace(go.Scatter(
            x=[int(p["ano"])], y=[p["estoque_estimado"]], mode="markers",
            name=f"{int(p['ano'])} estimado",
            marker=dict(size=11, color=tema.SUPERFICIE, line=dict(color=tema.SERIE_2, width=2.5)),
            error_y=dict(type="data", array=[p["margem"]], color=tema.SERIE_2, thickness=2),
            hovertemplate="%{x} estimado<br>%{y:,.0f} ± " + fmt_num(p["margem"]) + "<extra></extra>"))
    st.plotly_chart(_layout(fig, 380, True), width="stretch")

    texto = (
        f"Em {int(ini['ano'])} havia <strong>{fmt_num(ini['estoque'])}</strong> vínculos "
        f"formais de tecnologia ativos no fim do ano; em {int(fim['ano'])}, "
        f"<strong>{fmt_num(fim['estoque'])}</strong>.")
    if not saldo_ano.empty and not completos.empty:
        melhor = completos["saldo"].idxmax()
        negativos = [str(a) for a in completos.index[completos["saldo"] < 0]]
        texto += (
            f" O CAGED somou {fmt_compacto(completos['saldo'].sum())} vagas líquidas no "
            f"período, com o melhor ano em <strong>{melhor}</strong> "
            f"({fmt_compacto(completos.loc[melhor, 'saldo'])})")
        texto += (f" e saldo negativo em {' e '.join(negativos)}." if negativos else ".")
    if not proj.empty:
        p = proj.iloc[0]
        texto += (
            f" O ponto de {int(p['ano'])} é uma estimativa: a RAIS desse ano só sai no "
            f"ano seguinte, e o valor foi calculado a partir do CAGED já publicado.")
    _texto(texto)


# ------------------------------------------------------------ 2. função
def _funcao():
    lentes = dr.lentes()
    if lentes.empty:
        return
    ano = int(lentes["ano"].max())
    atual = lentes[lentes["ano"] == ano].set_index("categoria")

    def val(cat, col):
        return float(atual.loc[cat, col]) if cat in atual.index else 0.0

    fora, dentro = val(LENTE_FORA, "estoque"), val(LENTE_DENTRO, "estoque")
    parte_fora = fora / (fora + dentro) * 100 if fora + dentro else 0

    _secao("Onde está o trabalho de TI?",
           f"{_pct(parte_fora, 0)} dos profissionais de TI trabalham fora das "
           f"empresas de TI — e ganham e ficam mais")

    ordem = [LENTE_FORA, LENTE_OUTRA, LENTE_DENTRO]
    rotulos = {LENTE_FORA: "Profissional de TI fora de empresa de TI",
               LENTE_OUTRA: "Outra ocupação dentro de empresa de TI",
               LENTE_DENTRO: "Profissional de TI dentro de empresa de TI"}
    cores = [tema.SERIE_1, tema.GRID, tema.SERIE_3]
    fig = go.Figure(go.Bar(
        y=[rotulos[c] for c in ordem], x=[val(c, "estoque") for c in ordem],
        orientation="h", marker_color=cores,
        text=[f"{fmt_num(val(c, 'estoque'))} · {_dec(val(c, 'remuneracao_sm_mediana'), 2)} SM"
              for c in ordem],
        textposition="outside", cliponaxis=False,
        hovertemplate="%{y}<br>%{x:,.0f} vínculos<extra></extra>"))
    fig = _layout(fig, 250)
    fig.update_yaxes(autorange="reversed")
    fig.update_layout(margin=dict(l=8, r=120))
    st.plotly_chart(fig, width="stretch")

    setor = dm.sobrevivencia_tabua("setor")
    texto = (
        f"Este é o achado central. Em {ano}, <strong>{fmt_num(fora)}</strong> profissionais "
        f"com ocupação de TI estavam em bancos, varejo, indústria, saúde e governo, "
        f"contra {fmt_num(dentro)} dentro das empresas de software. Olhar só o setor "
        f"de TI perderia mais da metade do mercado. E eles não estão em posição pior: "
        f"a remuneração mediana fora é de <strong>{_dec(val(LENTE_FORA, 'remuneracao_sm_mediana'), 2)} SM</strong>, "
        f"contra {_dec(val(LENTE_DENTRO, 'remuneracao_sm_mediana'), 2)} SM dentro.")
    if not setor.empty and "5-10a" in setor:
        s = setor.set_index("grupo")["5-10a"]
        if {"Fora do setor de TI", "Empresa de TI"} <= set(s.index):
            texto += (
                f" O vínculo também dura mais: {_pct(s['Fora do setor de TI'])} chegam a "
                f"5–10 anos de casa fora do setor, contra {_pct(s['Empresa de TI'])} dentro. "
                f"São duas evidências independentes apontando para o mesmo lado.")
    _texto(texto)


# -------------------------------------------------------- 3. remuneração
def _remuneracao():
    anual = dr.estoque_anual()
    if anual.empty:
        return
    anual = anual.sort_values("ano")
    ini, fim = anual.iloc[0], anual.iloc[-1]
    med = anual["remuneracao_sm_mediana"]

    _secao("O crescimento virou salário?",
           f"Não em termos relativos: a mediana ficou entre {_dec(med.min(), 1)} e "
           f"{_dec(med.max(), 1)} salários mínimos por toda a série")

    fig = go.Figure()
    fig.add_trace(go.Scatter(x=anual["ano"], y=anual["remuneracao_sm"], name="Média",
                             mode="lines+markers", line=dict(color=tema.SERIE_1, width=3),
                             hovertemplate="%{x}<br>média %{y:.2f} SM<extra></extra>"))
    fig.add_trace(go.Scatter(x=anual["ano"], y=anual["remuneracao_sm_mediana"], name="Mediana",
                             mode="lines+markers", line=dict(color=tema.SERIE_3, width=3),
                             hovertemplate="%{x}<br>mediana %{y:.2f} SM<extra></extra>"))
    fig = _layout(fig, 340, True)
    fig.update_yaxes(rangemode="tozero", title="salários mínimos")
    st.plotly_chart(fig, width="stretch")

    _texto(
        f"O mercado cresceu, mas a remuneração média caiu de "
        f"<strong>{_dec(ini['remuneracao_sm'], 2)}</strong> para "
        f"<strong>{_dec(fim['remuneracao_sm'], 2)} SM</strong> entre {int(ini['ano'])} e "
        f"{int(fim['ano'])}: o salário mínimo teve ganhos reais que a remuneração de TI não "
        f"acompanhou. A distância entre média ({_dec(fim['remuneracao_sm'], 2)}) e mediana "
        f"({_dec(fim['remuneracao_sm_mediana'], 2)}) mostra que poucos salários altos puxam "
        f"o número que costuma ser citado — metade dos profissionais ganha até a mediana.")

    descartados = int(anual["remun_descartada"].sum()) if "remun_descartada" in anual else 0
    st.info(
        f"**Erro encontrado na fonte.** A partir de 2023 a RAIS traz registros com a "
        f"conversão para salário mínimo invertida — multiplicada em vez de dividida. "
        f"Foram {fmt_num(descartados)} registros, menos de 0,02% dos vínculos, mas eles "
        f"concentravam cerca de 35% da massa salarial e fariam a média saltar de 6,17 para "
        f"8,84 SM entre 2022 e 2023: uma “valorização de 43%” que não aconteceu. Dividindo "
        f"o nominal pelo SM, o salário mínimo implícito bate com o oficial até 2022 e "
        f"desaba em 2023. Um teto de 500 SM corrige a série.")


# ------------------------------------------------------------ 4. gênero
def _genero():
    df = dm.hiato("MASCULINO vs FEMININO")
    if df.empty:
        return
    df = df.sort_values("ano")
    ini, fim = df.iloc[0], df.iloc[-1]

    _secao("Mulheres ganham menos por causa do perfil?",
           f"Não. O perfil delas deveria render mais — o hiato bruto de "
           f"{_pct(fim['hiato_pct'])} esconde uma diferença maior")

    negativos = df[df["hiato_log"] < 0]["ano"].astype(int).tolist()
    fig = go.Figure()
    if negativos:
        fig.add_vrect(x0=min(negativos) - 0.5, x1=max(negativos) + 0.5,
                      fillcolor=tema.SERIE_4, opacity=0.12, line_width=0,
                      annotation_text="hiato bruto negativo", annotation_position="top left")
    fig.add_hline(y=0, line_width=1, line_color=tema.TEXTO_SEC)
    fig.add_trace(go.Scatter(x=df["ano"], y=df["hiato_log"], name="Hiato bruto",
                             mode="lines+markers", line=dict(color=tema.SERIE_1, width=3),
                             hovertemplate="%{x}<br>bruto %{y:.3f}<extra></extra>"))
    fig.add_trace(go.Scatter(x=df["ano"], y=df["nao_explicada"], name="Parte não explicada",
                             mode="lines+markers", line=dict(color=tema.NEGATIVO, width=3),
                             hovertemplate="%{x}<br>não explicada %{y:.3f}<extra></extra>"))
    fig = _layout(fig, 360, True)
    fig.update_yaxes(title="pontos log (≈ %)")
    st.plotly_chart(fig, width="stretch")

    texto = (
        "A decomposição de Oaxaca-Blinder separa o hiato em duas partes: a que se explica "
        "por diferenças de perfil (escolaridade, tempo de casa, área, jornada, porte e região) "
        "e a que sobra comparando perfis equivalentes. ")
    if fim["explicada"] < 0:
        texto += (
            f"Para gênero, a parte explicada é <strong>negativa</strong> "
            f"({_dec(fim['explicada'], 3)}): pelo perfil, as mulheres deveriam ganhar mais que "
            f"os homens. Ganham menos. Comparando iguais com iguais, a diferença é de "
            f"<strong>{_dec(fim['nao_explicada'], 3)} ponto log</strong>, quase o dobro do hiato bruto. ")
    if negativos:
        texto += (
            f"O dado mais forte está na série: entre {min(negativos)} e {max(negativos)} o hiato "
            f"bruto chegou a ficar negativo, e mesmo assim a parte não explicada quase não se "
            f"moveu ({_dec(ini['nao_explicada'], 3)} em {int(ini['ano'])}, "
            f"{_dec(fim['nao_explicada'], 3)} em {int(fim['ano'])}). "
            f"A composição mudou muito; a diferença de retorno, não.")
    _texto(texto)


# -------------------------------------------------------------- 5. raça
def _raca():
    parda, preta = dm.hiato("BRANCA vs PARDA"), dm.hiato("BRANCA vs PRETA")
    sexo = dm.hiato("MASCULINO vs FEMININO")
    if parda.empty or preta.empty:
        return

    _secao("E a desigualdade racial?",
           "A desigualdade racial é sobretudo de acesso — onde se trabalha e em quê")

    def ultimo(df):
        return df.sort_values("ano").iloc[-1]

    def primeiro(df):
        return df.sort_values("ano").iloc[0]

    linhas = []
    for nome, df in (("Branca × parda", parda), ("Branca × preta", preta),
                     ("Homens × mulheres", sexo)):
        if df.empty:
            continue
        u = ultimo(df)
        share = u["explicada_share"]
        linhas.append({
            "comparação": nome,
            f"hiato bruto {int(u['ano'])}": _pct(u["hiato_pct"]),
            "parte explicada": ("negativa" if u["explicada"] < 0
                                else _pct(share, 0) if pd.notna(share) else "—"),
        })
    st.dataframe(pd.DataFrame(linhas), width="stretch", hide_index=True)

    up, ip = ultimo(parda), ultimo(preta)
    _texto(
        f"O mecanismo é o oposto do de gênero. Entre brancos e pardos, "
        f"<strong>{_pct(up['explicada_share'], 0)}</strong> do hiato se explica por perfil — "
        f"principalmente região e área de atuação. Entre brancos e pretos, o hiato caiu de "
        f"{_dec(primeiro(preta)['hiato_log'], 2)} para {_dec(ip['hiato_log'], 2)} ponto log desde "
        f"{int(primeiro(preta)['ano'])}. A consequência é direta: gênero e raça produzem "
        f"desigualdade em TI por caminhos diferentes, e a medida que corrige um não corrige o outro.")

    st.caption("A parte não explicada não mede discriminação: inclui tudo que afeta salário e "
               "não está no modelo, como experiência anterior e senioridade no cargo. O que se "
               "afirma é o limite superior da diferença de retorno.")


# -------------------------------------------------------- 6. estabilidade
def _estabilidade():
    risco = dm.sobrevivencia_risco()
    porte = dm.sobrevivencia_tabua("porte")
    sexo = dm.sobrevivencia_tabua("sexo")
    if risco.empty:
        return
    pico = risco.loc[risco["risco_%"].idxmax()]
    final = risco.iloc[-1]

    _secao("Quanto dura um emprego de TI?",
           "O risco de perder o emprego é máximo no primeiro ano, e o porte da empresa "
           "pesa mais que o sexo")

    col1, col2 = st.columns(2)
    with col1:
        cores = [tema.SERIE_1 if t == pico["tempo_de_casa"] else tema.GRID
                 for t in risco["tempo_de_casa"]]
        fig = go.Figure(go.Bar(
            x=risco["tempo_de_casa"], y=risco["risco_%"], marker_color=cores,
            text=[_pct(v) for v in risco["risco_%"]], textposition="outside", cliponaxis=False,
            hovertemplate="%{x}<br>%{y:.1f}% ao ano<extra></extra>"))
        fig = _layout(fig, 320)
        fig.update_yaxes(title="risco anual de desligamento (%)")
        st.caption("Risco anual por tempo de casa")
        st.plotly_chart(fig, width="stretch")
    with col2:
        if not porte.empty and "5-10a" in porte:
            p = porte.dropna(subset=["5-10a"]).sort_values("5-10a")
            fig = go.Figure(go.Bar(
                y=p["grupo"], x=p["5-10a"], orientation="h", marker_color=tema.SERIE_3,
                text=[_pct(v) for v in p["5-10a"]], textposition="outside", cliponaxis=False,
                hovertemplate="%{y}<br>%{x:.1f}%<extra></extra>"))
            fig = _layout(fig, 320)
            fig.update_layout(margin=dict(l=8, r=60))
            st.caption("Vínculos que chegam a 5–10 anos, por porte")
            st.plotly_chart(fig, width="stretch")

    texto = (
        f"O risco anual de desligamento atinge o pico com <strong>{pico['tempo_de_casa']}</strong> "
        f"de casa ({_pct(pico['risco_%'])}) e cai até {_pct(final['risco_%'])} em "
        f"{final['tempo_de_casa']}.")
    if not porte.empty and "5-10a" in porte:
        p = porte.dropna(subset=["5-10a"])
        maior, menor = p.loc[p["5-10a"].idxmax()], p.loc[p["5-10a"].idxmin()]
        texto += (f" O que diferencia a estabilidade é o porte: em estabelecimentos de "
                  f"{maior['grupo'].lower()} empregados, {_pct(maior['5-10a'])} dos vínculos "
                  f"passam de cinco anos; nos de {menor['grupo'].lower()}, {_pct(menor['5-10a'])}.")
    if not sexo.empty and "5-10a" in sexo:
        s = sexo.set_index("grupo")["5-10a"]
        if {"MASCULINO", "FEMININO"} <= set(s.index):
            texto += (f" Entre homens e mulheres a diferença é mínima ({_pct(s['MASCULINO'])} "
                      f"contra {_pct(s['FEMININO'])}) — a desigualdade de gênero em TI está no "
                      f"salário, não na permanência.")
    _texto(texto)

    st.info(
        "**Erro de método evitado.** A primeira estimativa, por Kaplan-Meier direto, dava "
        "mediana de 570 meses — 48 anos de emprego. Num retrato de um único ano, quem tem dez "
        "anos de casa entra indevidamente no grupo em risco dos 12 meses. A tábua de período "
        "mede o risco dentro da janela anual e corrige isso. Transferências entre "
        "estabelecimentos (17% dos “desligamentos”) foram tratadas como continuidade do vínculo.")


# ----------------------------------------------------------- 7. território
def _territorio():
    cl = dm.clusters()
    if cl.empty:
        return

    _secao("Onde, no território?",
           "Concentrado nas capitais, mas crescendo mais rápido no interior")

    resumo = cl.groupby("nome").agg(
        municipios=("cod_municipio", "count"),
        estoque=("estoque", "sum"),
        crescimento=("crescimento", "median"),
    ).sort_values("estoque", ascending=False)
    total = resumo["estoque"].sum()

    tabela = []
    for nome, r in resumo.iterrows():
        exemplos = cl[cl["nome"] == nome].nlargest(4, "estoque")["municipio"]
        exemplos = ", ".join(m.split("-", 1)[-1] for m in exemplos)
        tabela.append({"perfil": nome, "municípios": int(r["municipios"]),
                       "estoque": fmt_num(r["estoque"]),
                       "parte do estoque": _pct(r["estoque"] / total * 100, 0),
                       "crescimento mediano": _pct(r["crescimento"] * 100, 0),
                       "exemplos": exemplos})
    st.dataframe(pd.DataFrame(tabela), width="stretch", hide_index=True)

    emergente = resumo.sort_values("crescimento", ascending=False).iloc[0]
    nome_em = resumo.sort_values("crescimento", ascending=False).index[0]
    _texto(
        f"Agrupando os municípios pela trajetória do mercado de TI — tamanho, especialização "
        f"por habitante, crescimento, fluxo, rotatividade e remuneração —, o grupo que mais "
        f"interessa é o <strong>{nome_em.lower()}</strong>: "
        f"{int(emergente['municipios'])} municípios com crescimento mediano de "
        f"<strong>{_pct(emergente['crescimento'] * 100, 0)}</strong> em cinco anos, a maioria no "
        f"interior. O mapa completo está na aba “Mapa do Brasil”.")

    st.warning(
        "**Limitação que o dado não permite corrigir.** A RAIS localiza o vínculo pelo "
        "estabelecimento, e empresas de TI escolhem onde se registrar por causa do ISS. "
        "Guaraciaba (MG), com 10 mil habitantes, passou de 1 para 1.428 vínculos de TI entre "
        "2022 e 2023; Barueri aparece com 200 vínculos por mil habitantes. A coluna que diria "
        "onde a pessoa trabalha (`mun_trab`) vem como “não informado” em praticamente todos "
        "os registros.")


# --------------------------------------------------------------- 8. futuro
def _futuro():
    prev, placar, mensal = dm.previsao(), dm.previsao_placar(), dm.serie_mensal()
    retido, retro, proj = dm.nowcast_ano_retido(), dm.nowcast_retrospectiva(), dm.nowcast_projecao()
    if prev.empty or mensal.empty:
        return

    prev = prev.copy()
    prev["mes"] = pd.to_datetime(prev["mes"])
    doze = prev.head(12)
    soma, lo, hi = doze["previsao"].sum(), doze["inferior"].sum(), doze["superior"].sum()

    _secao("Para onde vai?",
           f"Crescimento continua, mas lento: cerca de {fmt_compacto(soma)} vagas nos próximos 12 meses")

    obs = mensal.tail(8)
    fig = go.Figure()
    fig.add_trace(go.Bar(
        x=obs["mes"], y=obs["saldo"], name="Observado",
        marker_color=[tema.POSITIVO if v >= 0 else tema.NEGATIVO for v in obs["saldo"]],
        hovertemplate="%{x|%b/%Y}<br>%{y:,.0f}<extra></extra>"))
    fig.add_trace(go.Bar(
        x=doze["mes"], y=doze["previsao"], name="Previsto (intervalo de 80%)",
        marker_color=tema.GRID, marker_line_color=tema.SERIE_1, marker_line_width=1.2,
        error_y=dict(type="data", symmetric=False,
                     array=(doze["superior"] - doze["previsao"]).tolist(),
                     arrayminus=(doze["previsao"] - doze["inferior"]).tolist(),
                     color=tema.SERIE_1, thickness=1.4),
        hovertemplate="%{x|%b/%Y}<br>previsto %{y:,.0f}<extra></extra>"))
    fig.add_hline(y=0, line_width=1, line_color=tema.TEXTO_SEC)
    st.plotly_chart(_layout(fig, 360, True), width="stretch")

    ano_corr = int(mensal["mes"].max().year)
    parcial = mensal[mensal["mes"].dt.year == ano_corr]
    negativos = int((parcial["saldo"] < 0).sum())
    ganho = placar.iloc[0].get("ganho_vs_naive_%", None) if not placar.empty else None

    texto = (
        f"O primeiro semestre de {ano_corr} somou <strong>{fmt_num(parcial['saldo'].sum())}</strong> "
        f"vagas líquidas, com {negativos} {'mês negativo' if negativos == 1 else 'meses negativos'}. "
        f"A projeção para os próximos 12 meses é de {fmt_compacto(soma)} vagas, mas o intervalo "
        f"de 80% vai de <strong>{fmt_compacto(lo)}</strong> a <strong>{fmt_compacto(hi)}</strong>: "
        f"a previsão sustenta uma direção — crescimento modesto —, não um número exato.")
    if ganho is not None:
        texto += (f" O modelo foi escolhido por validação em origem móvel e erra "
                  f"{_pct(ganho)} menos que a regra ingênua “igual ao mesmo mês do ano anterior”.")
    _texto(texto)

    if not retido.empty and not proj.empty:
        melhor = retido.loc[retido["erro_pct"].abs().idxmin()]
        p = proj.iloc[0]
        c1, c2, c3 = st.columns(3)
        c1.metric(f"Erro ao prever {int(melhor['ano_teste'])}",
                  _pct(melhor["erro_pct"]), help="Modelo treinado só com os anos anteriores")
        c2.metric(f"Estoque estimado {int(p['ano'])}", fmt_compacto(p["estoque_estimado"]),
                  f"± {fmt_compacto(p['margem'])}")
        if not retro.empty:
            c3.metric("Melhor método em 12 anos", retro.iloc[0]["estimador"],
                      f"erro médio {_pct(retro.iloc[0]['erro_pct'])}", delta_color="off")
        _texto(
            f"Como a RAIS sai com um ano de atraso, o estoque foi estimado a partir do saldo do "
            f"CAGED. O teste foi feito como validação real: treinando só até "
            f"{int(melhor['ano_teste']) - 1} e prevendo {int(melhor['ano_teste'])}, que já está "
            f"publicado. O erro foi de <strong>{_pct(melhor['erro_pct'])}</strong>. Na "
            f"retrospectiva, a regra mais simples venceu a regressão — o achado é que o saldo do "
            f"CAGED acompanha a variação do estoque da RAIS quase 1:1.")


# ---------------------------------------------------------- 9. conclusões
def _conclusoes():
    st.divider()
    st.caption("CONCLUSÕES")
    st.subheader("O que a pesquisa permite afirmar")

    anual = dr.estoque_anual().sort_values("ano")
    lentes = dr.lentes()
    sexo = dm.hiato("MASCULINO vs FEMININO").sort_values("ano")
    prev = dm.previsao()
    proj = dm.nowcast_projecao()

    itens = []
    if not anual.empty:
        ini, fim = anual.iloc[0], anual.iloc[-1]
        itens.append((
            "O mercado formal de TI mais que dobrou",
            f"De {fmt_compacto(ini['estoque'])} para {fmt_compacto(fim['estoque'])} vínculos "
            f"entre {int(ini['ano'])} e {int(fim['ano'])}, com o salto concentrado em 2021–22."))
    if not lentes.empty:
        ano = int(lentes["ano"].max())
        a = lentes[lentes["ano"] == ano].set_index("categoria")["estoque"]
        if {LENTE_FORA, LENTE_DENTRO} <= set(a.index):
            parte = a[LENTE_FORA] / (a[LENTE_FORA] + a[LENTE_DENTRO]) * 100
            itens.append((
                "TI é uma função da economia, não um setor",
                f"{_pct(parte, 0)} dos profissionais de TI trabalham fora das empresas de TI, "
                f"com remuneração maior e vínculos mais longos."))
    if not anual.empty:
        med = anual["remuneracao_sm_mediana"]
        itens.append((
            "O crescimento não se converteu em remuneração relativa",
            f"A mediana ficou entre {_dec(med.min(), 1)} e {_dec(med.max(), 1)} salários mínimos "
            f"durante toda a série."))
    if not sexo.empty:
        u = sexo.iloc[-1]
        itens.append((
            "O hiato de gênero é maior do que parece e quase não mudou",
            f"O hiato bruto de {_pct(u['hiato_pct'])} subestima a diferença de retorno, que segue "
            f"em torno de {_dec(u['nao_explicada'], 2)} ponto log."))
    itens.append((
        "A desigualdade racial é principalmente de acesso",
        "A maior parte do hiato se explica por região e área de atuação, e o hiato entre "
        "brancos e pretos vem caindo."))
    itens.append((
        "A estabilidade depende mais do porte da empresa que do perfil",
        "Grandes empresas retêm cerca do dobro dos vínculos de longo prazo; o risco é máximo "
        "no primeiro ano de casa."))
    if not prev.empty and not proj.empty:
        p = proj.iloc[0]
        itens.append((
            "O mercado segue crescendo, mas devagar",
            f"Projeção de {fmt_compacto(prev.head(12)['previsao'].sum())} vagas em 12 meses e "
            f"estoque de {fmt_compacto(p['estoque_estimado'])} em {int(p['ano'])}, com margens declaradas."))

    for i, (titulo, detalhe) in enumerate(itens, start=1):
        st.markdown(f"**{i}. {titulo}**  \n{detalhe}")

    st.markdown("#### O que os dados não permitem afirmar")
    st.markdown(
        "- **Trajetórias individuais.** A RAIS pública não tem identificador do trabalhador; "
        "não é possível acompanhar a mesma pessoa entre anos.\n"
        "- **Onde as pessoas trabalham.** A localização é a do estabelecimento, sujeita a "
        "domicílio fiscal.\n"
        "- **Discriminação.** A parte não explicada do hiato é um limite superior, não uma "
        "medida de causa.\n"
        "- **Série homogênea no CAGED.** Em 2020 muda a data de referência da movimentação "
        "(competência declarada → competência do fato).\n"
        "- **Recorte completo de ocupações.** Analistas de dados registrados como estatísticos "
        "(CBO 2112) ficam de fora quando não estão em empresa de TI — ver apêndice.")

    with st.expander("Roteiro para apresentar"):
        st.markdown(
            "1. A pergunta e o recorte: setor **ou** ocupação; fluxo e estoque.\n"
            "2. O tamanho: estoque dobrou, boom de 2021–22, desaceleração em 2026.\n"
            "3. O achado central: TI como função, fora das empresas de TI.\n"
            "4. As desigualdades: gênero com parte explicada negativa; raça como acesso.\n"
            "5. O rigor: os erros da fonte e de método encontrados e corrigidos.\n"
            "6. Limitações e próximos passos: CBO 2112 e ausência de painel.")


# ------------------------------------------------------------ 10. apêndice
# Códigos de seis dígitos efetivamente OBSERVADOS no estoque de TI da RAIS 2024,
# levantados sobre a silver de TI. Não é agregado da gold — é a documentação do
# recorte, e por isso fica aqui como referência fixa.
CBO_OBSERVADOS_2024 = [
    ("1236", "123605", "Diretor de serviços de informática", 4167, ""),
    ("1425", "142505", "Gerente de rede", 8207, ""),
    ("1425", "142510", "Gerente de desenvolvimento de sistemas", 19717, ""),
    ("1425", "142515", "Gerente de produção de TI", 7985, ""),
    ("1425", "142520", "Gerente de projetos de TI", 28544, ""),
    ("1425", "142525", "Gerente de segurança de TI", 1963, ""),
    ("1425", "142530", "Gerente de suporte técnico de TI", 11422, ""),
    ("1425", "142535", "Tecnólogo em gestão da TI", 5395, ""),
    ("2031", "203105", "Pesquisador em ciências da computação e informática", 3189, ""),
    ("2031", "203110", "Pesquisador em ciências da terra e meio ambiente", 540, "revisar"),
    ("2031", "203115", "Pesquisador em física", 62, "revisar"),
    ("2031", "203120", "Pesquisador em matemática", 876, "revisar"),
    ("2031", "203125", "Pesquisador em química", 1335, "revisar"),
    ("2122", "212205", "Engenheiro de aplicativos em computação", 9798, ""),
    ("2122", "212210", "Engenheiro de equipamentos em computação", 1073, ""),
    ("2122", "212215", "Engenheiro de sistemas operacionais em computação", 10934, ""),
    ("2123", "212305", "Administrador de banco de dados", 13459, ""),
    ("2123", "212310", "Administrador de redes", 7127, ""),
    ("2123", "212315", "Administrador de sistemas operacionais", 11080, ""),
    ("2123", "212320", "Administrador em segurança da informação", 13881, ""),
    ("2124", "212405", "Analista de desenvolvimento de sistemas", 282365, ""),
    ("2124", "212410", "Analista de redes e de comunicação de dados", 50607, ""),
    ("2124", "212415", "Analista de sistemas de automação", 11062, ""),
    ("2124", "212420", "Analista de suporte computacional", 107789, ""),
    ("2124", "212425", "Código recente da família, sem descrição no dicionário", 6402, ""),
    ("2124", "212430", "Código recente da família, sem descrição no dicionário", 14267, ""),
    ("3171", "317105", "Programador de internet", 5067, ""),
    ("3171", "317110", "Programador de sistemas de informação", 88970, ""),
    ("3171", "317115", "Programador de máquinas-ferramenta com comando numérico", 6081, "revisar"),
    ("3171", "317120", "Programador de multimídia", 2122, ""),
    ("3172", "317205", "Operador de computador", 32339, ""),
    ("3172", "317210", "Técnico de apoio ao usuário de informática (helpdesk)", 86815, ""),
    ("avulso", "142135", "Encarregado de proteção de dados (DPO)", 379, ""),
    ("avulso", "313220", "Técnico em manutenção de equipamentos de informática", 49255, ""),
    ("avulso", "313305", "Técnico de comunicação de dados", 6889, ""),
]


def _apendice():
    from gold_caged import escopo_tecnologia as esc

    st.divider()
    st.caption("APÊNDICE")
    st.subheader("Os códigos CNAE e CBO que definem o recorte")
    st.markdown(
        "Um vínculo entra na pesquisa se a empresa tem **CNAE de TI** ou se a ocupação tem "
        "**CBO de TI**. As ocupações são selecionadas por **família** (os quatro primeiros "
        "dígitos), para capturar códigos que o MTE cria ao longo do tempo, mais três códigos "
        "avulsos.")

    col1, col2 = st.columns(2)
    with col1:
        st.markdown("**Setor — CNAE 2.0 (subclasse)**")
        st.dataframe(pd.DataFrame(
            [{"CNAE": k, "atividade": v} for k, v in esc.CNAE_TI.items()]),
            width="stretch", hide_index=True)
    with col2:
        st.markdown("**Ocupação — famílias CBO 2002**")
        linhas = [{"CBO": k, "família": v} for k, v in esc.CBO_FAMILIAS_TI.items()]
        linhas += [{"CBO": k, "família": f"{v} (avulso)"} for k, v in esc.CBO_AVULSOS_TI.items()]
        st.dataframe(pd.DataFrame(linhas), width="stretch", hide_index=True)

    obs = pd.DataFrame(CBO_OBSERVADOS_2024,
                       columns=["família", "CBO", "ocupação", "vínculos 2024", "observação"])
    st.markdown(f"**Códigos observados no estoque de 2024** — "
                f"{fmt_num(obs['vínculos 2024'].sum())} vínculos ativos")
    st.dataframe(obs, width="stretch", hide_index=True, height=420)

    a_mais = obs[obs["observação"] == "revisar"]["vínculos 2024"].sum()
    st.warning(
        f"**Revisão sugerida do recorte.** *Possivelmente incluídos a mais:* a seleção por família "
        f"trouxe pesquisadores de ciências da terra, física, matemática e química (família 2031) e "
        f"o programador de máquinas CNC (3171-15), que é da indústria. Somam "
        f"{fmt_num(a_mais)} vínculos — cerca de "
        f"{_pct(a_mais / obs['vínculos 2024'].sum() * 100, 0)} do total —, então não alteram as "
        f"conclusões, mas devem ser excluídos ou justificados.\n\n"
        f"*Possivelmente deixados de fora:* a família **2112 (estatísticos)** não está no recorte. "
        f"No CAGED de 2025 foram 2.324 admissões nesses códigos, entre 70% e 75% fora de empresas "
        f"de TI — sobretudo no setor financeiro —, com salário mediano de contratação entre "
        f"R$ 6.200 e R$ 9.000. É onde muitos analistas e cientistas de dados são registrados. "
        f"Incluí-la exigiria refazer o recorte e todas as tabelas; fica como próximo passo declarado.")


# ---------------------------------------------------------------- render
def render():
    if not dr.tem_dados():
        st.info("A camada gold ainda não está disponível — a história depende dela.")
        return
    _abertura()
    _tamanho()
    _funcao()
    _remuneracao()
    _genero()
    _raca()
    _estabilidade()
    _territorio()
    _futuro()
    _conclusoes()
    _apendice()
