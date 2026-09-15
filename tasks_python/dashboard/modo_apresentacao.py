"""
Modo apresentação: o storytelling da pesquisa em slides de tela cheia.

POR QUE UM MODO À PARTE, E NÃO MAIS UMA ABA
-------------------------------------------
Uma aba compete com as outras doze pela atenção e herda o cabeçalho, as abas e
a barra do Streamlit. Numa banca isso é ruído: o que se quer projetar é UMA
afirmação e a evidência dela, grande, sem nada em volta.

Aqui cada slide é uma conclusão com um único visual. A navegação vai para o
endereço da página (`?modo=apresentacao&slide=5`), o que dá três coisas de
graça: recarregar a página não volta ao começo, dá para mandar o link de um
slide específico, e o botão "voltar" do navegador funciona.

O Streamlit não tem modo de slides nativo. O que falta é suprido por um
componente mínimo: botão de tela cheia e atalhos de teclado (setas e Page
Up/Down, que é o que o passador de slides envia). Os atalhos só clicam nos
botões que já existem na página — não há navegação paralela para divergir da
que o Streamlit controla.

O MESMO CONTEÚDO EM DOIS FORMATOS
---------------------------------
Slide é para projetar; texto corrido é para ler antes ou depois. O formato
"texto corrido" reaproveita `aba_historia`, que tem a argumentação completa. Os
números vêm da mesma gold nos dois, então não há como os dois contarem
histórias diferentes.
"""
import re

import numpy as np
import pandas as pd
import plotly.graph_objects as go
import streamlit as st
import streamlit.components.v1 as components

from dashboard import aba_historia as ah
from dashboard import aba_mapa
from dashboard import dados_modelos as dm
from dashboard import dados_rais as dr
from dashboard import tema
from dashboard.tema import fmt_compacto, fmt_num

PARAM_MODO, VALOR_MODO = "modo", "apresentacao"
PARAM_SLIDE, PARAM_FORMATO = "slide", "formato"
ALTURA = 540

_dec, _pct = ah._dec, ah._pct


# ================================================================ controle
def ativo() -> bool:
    return st.query_params.get(PARAM_MODO) == VALOR_MODO


def _entrar():
    st.query_params[PARAM_MODO] = VALOR_MODO
    st.query_params[PARAM_SLIDE] = "1"


def _sair():
    for chave in (PARAM_MODO, PARAM_SLIDE, PARAM_FORMATO):
        if chave in st.query_params:
            del st.query_params[chave]


def _ir(n: int):
    st.query_params[PARAM_SLIDE] = str(n)


def _alternar_formato():
    if st.query_params.get(PARAM_FORMATO) == "texto":
        del st.query_params[PARAM_FORMATO]
    else:
        st.query_params[PARAM_FORMATO] = "texto"


def botao():
    """O botão que abre o storytelling, colocado no topo da página normal."""
    st.button("modo-apresentacao", icon=":material/slideshow:", type="primary",
              on_click=_entrar, key="abrir_apresentacao",
              help="Abre o storytelling da pesquisa em slides. Setas do teclado "
                   "navegam; o botão de tela cheia fica no topo.")


# ================================================================== estilo
def _css():
    st.markdown(f"""
    <style>
      [data-testid="stHeader"], [data-testid="stToolbar"], [data-testid="stDecoration"],
      [data-testid="stSidebar"], [data-testid="stSidebarCollapsedControl"], footer {{
        display: none !important;
      }}
      [data-testid="stMainBlockContainer"], .block-container {{
        padding-top: 1.1rem !important; padding-bottom: 1.2rem !important;
        max-width: 1400px !important;
      }}
      .slide-rotulo {{
        font-size: 0.78rem; letter-spacing: 0.14em; text-transform: uppercase;
        color: {tema.TEXTO_SEC}; margin: 0.4rem 0 0.2rem;
      }}
      .slide-titulo {{
        font-size: clamp(1.9rem, 3.3vw, 3rem); font-weight: 750; line-height: 1.1;
        letter-spacing: -0.01em; margin: 0 0 0.9rem; color: {tema.TEXTO};
        text-wrap: balance; max-width: 30ch;
      }}
      .slide-tese {{
        font-size: clamp(1.35rem, 2.3vw, 2rem); line-height: 1.3; max-width: 38ch;
        color: {tema.TEXTO}; margin: 0.4rem 0 1.4rem;
      }}
      .slide-tese strong {{ color: {tema.SERIE_1}; }}
      .slide-lista {{ font-size: 1.12rem; line-height: 1.55; }}
      .slide-cartao {{
        border-top: 3px solid {tema.SERIE_2}; padding: 0.8rem 0 0.4rem;
      }}
      .slide-cartao .grande {{ font-size: 2.3rem; font-weight: 750; line-height: 1.05; color: {tema.TEXTO}; }}
      .slide-cartao .titulo {{ font-weight: 650; font-size: 1.05rem; margin: 0.35rem 0 0.25rem; }}
      .slide-cartao p {{ color: {tema.TEXTO_SEC}; font-size: 0.98rem; margin: 0; }}
    </style>
    """, unsafe_allow_html=True)


def _controles_navegador():
    """
    Tela cheia e atalhos de teclado, que o Streamlit não oferece.

    O componente roda num iframe da mesma origem, então alcança a página que o
    contém. A trava `__atalhosApresentacao` impede que cada rerun empilhe mais
    um ouvinte de teclado — sem ela, a terceira troca de slide já avançaria
    três de uma vez.
    """
    html = """
    <style>
      body { margin: 0; background: transparent; }
      button { width: 100%; padding: 8px 10px; border-radius: 8px; cursor: pointer;
               font: 600 14px system-ui, -apple-system, "Segoe UI", sans-serif;
               color: __COR__; background: transparent; border: 1px solid __BORDA__; }
      button:hover { border-color: __COR__; }
    </style>
    <button id="tela-cheia" title="Tela cheia (ou F11)">⛶ Tela cheia</button>
    <script>
      const pai = window.parent;
      document.getElementById("tela-cheia").onclick = () => {
        const d = pai.document;
        if (d.fullscreenElement) { d.exitFullscreen(); return; }
        const el = d.documentElement;
        (el.requestFullscreen ? el.requestFullscreen() : Promise.reject())
          .catch(() => alert("Este navegador bloqueou a tela cheia. Use F11."));
      };
      if (!pai.__atalhosApresentacao) {
        pai.__atalhosApresentacao = true;
        pai.document.addEventListener("keydown", (e) => {
          const tag = (e.target.tagName || "").toLowerCase();
          if (["input", "textarea", "select"].includes(tag)) return;
          let rotulo = null;
          if (["ArrowRight", "PageDown"].includes(e.key)) rotulo = "Próximo";
          if (["ArrowLeft", "PageUp"].includes(e.key)) rotulo = "Anterior";
          if (!rotulo) return;
          const alvo = [...pai.document.querySelectorAll("button")]
            .find((b) => b.innerText.includes(rotulo));
          if (alvo && !alvo.disabled) { e.preventDefault(); alvo.click(); }
        });
      }
    </script>
    """
    html = html.replace("__COR__", tema.TEXTO).replace("__BORDA__", tema.GRID)
    # `components.html` está descontinuado, com remoção anunciada. O deploy não
    # fixa a versão do Streamlit, então ele pode sumir numa atualização. Tela
    # cheia e teclado são conforto, não função: sem eles os botões Anterior e
    # Próximo continuam navegando, e F11 continua abrindo tela cheia. Por isso a
    # falha aqui é contida em vez de derrubar a apresentação inteira.
    try:
        components.html(html, height=44)
    except Exception:  # noqa: BLE001
        st.caption("F11: tela cheia")


# ============================================================== utilidades
def _cabecalho(rotulo: str, titulo: str):
    st.markdown(f'<p class="slide-rotulo">{rotulo}</p>'
                f'<p class="slide-titulo">{titulo}</p>', unsafe_allow_html=True)


def _roteiro(itens: list[str]):
    """Notas de fala: fechadas por padrão, para não aparecerem no projetor."""
    if itens:
        with st.expander("Roteiro da fala"):
            st.markdown("\n".join(f"- {i}" for i in itens))


def _layout(fig, altura=ALTURA, legenda=False):
    fig.update_layout(**tema.layout_base(altura=altura, mostrar_legenda=legenda))
    fig.update_layout(font=dict(size=15))
    return fig


def _grafico(fig):
    st.plotly_chart(fig, width="stretch", config={"displayModeBar": False})


# ================================================================= slides
def s_capa():
    anual = dr.estoque_anual().sort_values("ano")
    lentes = dr.lentes()
    mensal = dm.serie_mensal()

    st.markdown('<p class="slide-rotulo">Pesquisa de TCC · microdados do CAGED e da RAIS</p>'
                '<p class="slide-titulo" style="font-size:clamp(2.6rem,5vw,4.4rem);max-width:18ch;">'
                'Vinte anos de emprego em tecnologia</p>', unsafe_allow_html=True)
    st.markdown('<p class="slide-tese">O emprego de TI mais que dobrou e <strong>deixou de ser um '
                'setor para virar uma função</strong> da economia — mas crescer não corrigiu quem '
                'ganha menos, e o ritmo agora desacelera.</p>', unsafe_allow_html=True)

    c = st.columns(4)
    if not anual.empty:
        ini, fim = anual.iloc[0], anual.iloc[-1]
        c[0].metric(f"Vínculos de TI em {int(fim['ano'])}", fmt_compacto(fim["estoque"]))
        c[1].metric(f"Crescimento desde {int(ini['ano'])}",
                    _pct((fim["estoque"] / ini["estoque"] - 1) * 100, 0))
    if not lentes.empty:
        a = lentes[lentes["ano"] == lentes["ano"].max()].set_index("categoria")["estoque"]
        if {ah.LENTE_FORA, ah.LENTE_DENTRO} <= set(a.index):
            c[2].metric("Fora das empresas de TI",
                        _pct(a[ah.LENTE_FORA] / (a[ah.LENTE_FORA] + a[ah.LENTE_DENTRO]) * 100, 0))
    if not mensal.empty:
        c[3].metric("Meses de CAGED analisados", fmt_num(len(mensal)))
    st.caption("Fontes: CAGED e RAIS (Ministério do Trabalho e Emprego) · IBGE. "
               "Recorte: empresa com CNAE de TI ou ocupação com CBO de TI.")


def s_bases():
    _cabecalho("Como ler os números",
               "Duas bases, duas perguntas: quanto o mercado se move e de que tamanho ele é")
    c1, c2, c3 = st.columns(3)
    blocos = [
        ("CAGED", "fluxo mensal",
         "Quantos empregos foram criados ou encerrados em cada mês. Mostra a direção do mercado."),
        ("RAIS", "estoque anual",
         "Quantos vínculos existiam em 31 de dezembro, com remuneração e tempo de casa. "
         "Mostra o tamanho."),
        ("Recorte", "setor OU ocupação",
         "Entra quem trabalha em empresa de TI (CNAE) ou exerce ocupação de TI (CBO) — "
         "o desenvolvedor do banco e a equipe da software house."),
    ]
    for col, (titulo, grande, texto) in zip((c1, c2, c3), blocos):
        col.markdown(f'<div class="slide-cartao"><div class="grande">{titulo}</div>'
                     f'<div class="titulo">{grande}</div><p>{texto}</p></div>',
                     unsafe_allow_html=True)
    st.markdown("")
    st.markdown('<p class="slide-lista">Remuneração em <strong>salários mínimos</strong> torna 2007 '
                'comparável a 2025 sem escolher deflator. Toda previsão aparece com intervalo, e todo '
                'modelo foi comparado com uma regra ingênua que precisou superar.</p>',
                unsafe_allow_html=True)
    _roteiro(["Saldo positivo não diz se o mercado é grande; estoque grande não diz se está crescendo.",
              "Por isso a pesquisa usa as duas bases juntas — e algumas análises só existem porque as duas se cruzam."])


def s_tamanho():
    anual = dr.estoque_anual().sort_values("ano")
    proj = dm.nowcast_projecao()
    if anual.empty:
        return
    ini, fim = anual.iloc[0], anual.iloc[-1]
    var = (fim["estoque"] / ini["estoque"] - 1) * 100
    _cabecalho("Quanto o mercado cresceu?",
               f"O estoque de TI cresceu {_pct(var, 0)}, com um salto concentrado em 2021–22")

    fig = go.Figure()
    fig.add_vrect(x0=2020.5, x1=2022.5, fillcolor=tema.SERIE_4, opacity=0.13, line_width=0,
                  annotation_text="boom", annotation_position="top left")
    fig.add_trace(go.Scatter(x=anual["ano"], y=anual["estoque"], mode="lines+markers",
                             name="Vínculos ativos (RAIS)", line=dict(color=tema.SERIE_1, width=4),
                             marker=dict(size=7),
                             hovertemplate="%{x}<br>%{y:,.0f}<extra></extra>"))
    if not proj.empty:
        p = proj.iloc[0]
        fig.add_trace(go.Scatter(
            x=[int(p["ano"])], y=[p["estoque_estimado"]], mode="markers+text",
            name=f"{int(p['ano'])} estimado", text=[f"{fmt_compacto(p['estoque_estimado'])} (est.)"],
            textposition="top center",
            marker=dict(size=14, color=tema.SUPERFICIE, line=dict(color=tema.SERIE_2, width=3)),
            error_y=dict(type="data", array=[p["margem"]], color=tema.SERIE_2, thickness=2.5)))
    fig.add_annotation(x=int(ini["ano"]), y=ini["estoque"], text=fmt_compacto(ini["estoque"]),
                       showarrow=False, yshift=-24, font=dict(size=16))
    fig.add_annotation(x=int(fim["ano"]), y=fim["estoque"], text=fmt_compacto(fim["estoque"]),
                       showarrow=False, yshift=26, font=dict(size=16))
    _grafico(_layout(fig, legenda=True))

    mensal = dm.serie_mensal()
    notas = [f"{fmt_num(ini['estoque'])} vínculos em {int(ini['ano'])}; {fmt_num(fim['estoque'])} em {int(fim['ano'])}."]
    if not mensal.empty:
        m = mensal.assign(ano=mensal["mes"].dt.year).groupby("ano")["saldo"].agg(["sum", "size"])
        m = m[m["size"] == 12]
        notas.append(f"Melhor ano no CAGED: {m['sum'].idxmax()} ({fmt_compacto(m['sum'].max())} vagas líquidas).")
        neg = [str(a) for a in m.index[m["sum"] < 0]]
        if neg:
            notas.append(f"Anos com saldo negativo: {', '.join(neg)}.")
    notas.append("O ponto âmbar é estimado: a RAIS desse ano ainda não saiu (ver slide de futuro).")
    _roteiro(notas)


def s_funcao():
    lentes = dr.lentes()
    if lentes.empty:
        return
    ano = int(lentes["ano"].max())
    a = lentes[lentes["ano"] == ano].set_index("categoria")
    fora, dentro = a.loc[ah.LENTE_FORA, "estoque"], a.loc[ah.LENTE_DENTRO, "estoque"]
    parte = fora / (fora + dentro) * 100
    _cabecalho("Onde está o trabalho de TI?",
               f"{_pct(parte, 0)} dos profissionais de TI trabalham fora das empresas de TI")

    col_g, col_n = st.columns([2.2, 1])
    with col_g:
        ordem = [ah.LENTE_FORA, ah.LENTE_DENTRO, ah.LENTE_OUTRA]
        nomes = {ah.LENTE_FORA: "Profissional de TI<br>fora de empresa de TI",
                 ah.LENTE_DENTRO: "Profissional de TI<br>dentro de empresa de TI",
                 ah.LENTE_OUTRA: "Outra ocupação<br>dentro de empresa de TI"}
        fig = go.Figure(go.Bar(
            y=[nomes[c] for c in ordem], x=[a.loc[c, "estoque"] for c in ordem], orientation="h",
            marker_color=[tema.SERIE_1, tema.SERIE_3, tema.GRID],
            text=[fmt_compacto(a.loc[c, "estoque"]) for c in ordem], textposition="outside",
            cliponaxis=False, textfont=dict(size=18)))
        fig = _layout(fig, 430)
        fig.update_yaxes(autorange="reversed")
        fig.update_layout(margin=dict(l=8, r=80))
        _grafico(fig)
    with col_n:
        st.metric("Mediana fora", f"{_dec(a.loc[ah.LENTE_FORA, 'remuneracao_sm_mediana'], 2)} SM")
        st.metric("Mediana dentro", f"{_dec(a.loc[ah.LENTE_DENTRO, 'remuneracao_sm_mediana'], 2)} SM")
        setor = dm.sobrevivencia_tabua("setor")
        if not setor.empty and "5-10a" in setor:
            s = setor.set_index("grupo")["5-10a"]
            if {"Fora do setor de TI", "Empresa de TI"} <= set(s.index):
                st.metric("Chegam a 5–10 anos de casa",
                          _pct(s["Fora do setor de TI"]),
                          f"{_dec(s['Fora do setor de TI'] - s['Empresa de TI'], 1)} p.p. acima de quem está dentro")
    _roteiro([
        "Achado central da pesquisa: tecnologia é função da economia, não um setor.",
        f"Em {ano}, {fmt_num(fora)} profissionais de TI estão em bancos, varejo, indústria, saúde e governo; {fmt_num(dentro)} em empresas de software.",
        "E não estão em posição pior: ganham mais e ficam mais tempo — duas evidências independentes na mesma direção.",
    ])


def _figura_mapa(ano: int, altura: int):
    uf = dm.mapa_uf(ano)
    muni = dm.municipios_com_coordenada(ano, minimo=300)
    clusters, geo_nomes = dm.clusters(), dm.ler("geo_municipios")
    geo = aba_mapa._geojson()
    if uf.empty or muni.empty or geo is None:
        return None, {}

    if not geo_nomes.empty:
        muni = muni.merge(geo_nomes[["cod6", "nome"]], on="cod6", how="left")
    else:
        muni["nome"] = muni["municipio"].str.split("-", n=1).str[-1]
    if not clusters.empty:
        muni = muni.merge(clusters[["cod_municipio", "nome"]].rename(columns={"nome": "perfil"}),
                          on="cod_municipio", how="left")
    else:
        muni["perfil"] = None
    muni["perfil"] = muni["perfil"].fillna("Sem perfil atribuído")

    fig = go.Figure()
    fig.add_trace(go.Choropleth(
        geojson=geo, locations=uf["uf"], featureidkey="properties.sigla", z=uf["estoque"],
        colorscale=[[0, tema.SUPERFICIE], [1, tema.SERIE_3]], marker_line_color=tema.GRID,
        marker_line_width=0.7, showscale=False, name="Estoque por UF",
        hovertemplate="<b>%{location}</b><br>%{z:,.0f} vínculos<extra></extra>"))

    cores = {"Polo maduro": tema.SERIE_1, "Emergente": tema.SERIE_2,
             "Mercado incipiente": tema.SERIE_5, "Sem perfil atribuído": tema.TEXTO_SEC}
    maior = np.sqrt(muni["estoque"].max()) or 1
    for perfil, sub in muni.groupby("perfil"):
        fig.add_trace(go.Scattergeo(
            lon=sub["longitude"], lat=sub["latitude"], mode="markers", name=perfil,
            text=sub["nome"], customdata=sub[["estoque", "saldo"]],
            marker=dict(size=np.sqrt(sub["estoque"]) / maior * 46 + 4,
                        color=cores.get(perfil, tema.TEXTO_SEC), opacity=0.7,
                        line=dict(width=0.6, color=tema.SUPERFICIE)),
            hovertemplate="<b>%{text}</b><br>%{customdata[0]:,.0f} vínculos"
                          "<br>saldo no ano: %{customdata[1]:+,.0f}<extra></extra>"))

    # Os pontos que a fala precisa apontar. Rotular tudo seria ilegível; estes
    # são os três tipos de cidade que o argumento usa.
    fiscal = muni[muni["municipio"].isin(["Sp-Barueri", "Mg-Guaraciaba"])]
    grandes = muni[~muni.index.isin(fiscal.index)].nlargest(4, "estoque")
    emergentes = muni[muni["perfil"] == "Emergente"].nlargest(3, "estoque")
    for df, cor, posicao, sufixo in (
            (grandes, tema.TEXTO, "top right", ""),
            (emergentes, tema.SERIE_2, "bottom right", " · emergente"),
            (fiscal, tema.NEGATIVO, "middle left", " · domicílio fiscal")):
        if df.empty:
            continue
        fig.add_trace(go.Scattergeo(
            lon=df["longitude"], lat=df["latitude"], mode="text", showlegend=False,
            text=[f"<b>{n}</b> {fmt_compacto(e)}{sufixo}" for n, e in zip(df["nome"], df["estoque"])],
            textposition=posicao, textfont=dict(size=14, color=cor), hoverinfo="skip"))

    fig.update_geos(projection_type="mercator", lataxis_range=[-34.5, 6], lonaxis_range=[-75, -33],
                    showland=True, landcolor=tema.SUPERFICIE, showcountries=True,
                    countrycolor=tema.GRID, showocean=False, showlakes=False,
                    bgcolor="rgba(0,0,0,0)", fitbounds=False)
    fig.update_layout(height=altura, margin=dict(l=0, r=0, t=6, b=0),
                      paper_bgcolor="rgba(0,0,0,0)", font=dict(color=tema.TEXTO, size=14),
                      legend=dict(orientation="h", yanchor="bottom", y=0.01, x=0.01,
                                  bgcolor="rgba(0,0,0,0)"))

    fatos = {"uf": uf, "emergentes": muni[muni["perfil"] == "Emergente"],
             "grandes": grandes, "fiscal": fiscal}
    return fig, fatos


def s_mapa():
    anos = dm.anos_do_mapa()
    todos = dm.mapa_uf()
    if not anos or todos.empty:
        return
    com_estoque = sorted(todos[todos["estoque"] > 0]["ano"].unique())
    ano = int(com_estoque[-1]) if com_estoque else anos[-1]

    clusters = dm.clusters()
    cres_em = None
    if not clusters.empty:
        em = clusters[clusters["nome"] == "Emergente"]
        if not em.empty:
            cres_em = em["crescimento"].median() * 100

    _cabecalho(f"Onde, no território? · {ano}",
               "O emprego de TI se concentra no Sudeste, mas o interior cresce mais rápido")

    col_m, col_n = st.columns([2.6, 1])
    with col_m:
        fig, fatos = _figura_mapa(ano, 600)
        if fig is None:
            st.info("Agregados territoriais indisponíveis.")
            return
        _grafico(fig)
    with col_n:
        uf = fatos["uf"]
        total = uf["estoque"].sum()
        lider = uf.nlargest(1, "estoque").iloc[0]
        st.metric(f"Maior estoque: {lider['uf']}", fmt_compacto(lider["estoque"]),
                  f"{_pct(lider['estoque'] / total * 100, 0)} do país", delta_color="off")
        sudeste = uf[uf["uf"].isin(["SP", "RJ", "MG", "ES"])]["estoque"].sum()
        st.metric("Sudeste", _pct(sudeste / total * 100, 0), "do estoque nacional", delta_color="off")
        if not clusters.empty:
            st.metric("Municípios emergentes", fmt_num((clusters["nome"] == "Emergente").sum()),
                      f"crescimento mediano de {_pct(cres_em, 0)} em 5 anos" if cres_em else None,
                      delta_color="off")
        st.caption("Bolha: município, tamanho = vínculos, cor = perfil de trajetória. "
                   "Cor do estado: estoque. Vermelho: distorção por domicílio fiscal.")

    notas = []
    if not fatos["grandes"].empty:
        notas.append("Maiores praças: " + ", ".join(
            f"{n} ({fmt_compacto(e)})" for n, e in zip(fatos["grandes"]["nome"], fatos["grandes"]["estoque"])) + ".")
    if not fatos["emergentes"].empty:
        top = fatos["emergentes"].nlargest(4, "estoque")["nome"].tolist()
        notas.append("Emergentes são sobretudo do interior: " + ", ".join(top) + ".")
    notas.append("Ressalva obrigatória: a RAIS localiza pela sede do estabelecimento. Barueri e "
                 "Guaraciaba (MG, de 1 para 1.428 vínculos entre 2022 e 2023) refletem registro por "
                 "causa do ISS, não onde as pessoas trabalham.")
    _roteiro(notas)


def s_remuneracao():
    anual = dr.estoque_anual().sort_values("ano")
    if anual.empty:
        return
    med = anual["remuneracao_sm_mediana"]
    ini, fim = anual.iloc[0], anual.iloc[-1]
    _cabecalho("O crescimento virou salário?",
               f"Não em termos relativos: a mediana ficou entre {_dec(med.min(), 1)} e "
               f"{_dec(med.max(), 1)} salários mínimos")
    fig = go.Figure()
    fig.add_trace(go.Scatter(x=anual["ano"], y=anual["remuneracao_sm"], name="Média",
                             mode="lines+markers", line=dict(color=tema.SERIE_1, width=4)))
    fig.add_trace(go.Scatter(x=anual["ano"], y=anual["remuneracao_sm_mediana"], name="Mediana",
                             mode="lines+markers", line=dict(color=tema.SERIE_3, width=4)))
    fig.add_trace(go.Scatter(x=[2022, 2023, 2024, 2025], y=[6.17, 8.84, 8.82, 7.24],
                             name="Sem correção da fonte", mode="lines",
                             line=dict(color=tema.NEGATIVO, width=2, dash="dash")))
    fig = _layout(fig, legenda=True)
    fig.update_yaxes(title="salários mínimos", rangemode="tozero")
    _grafico(fig)
    _roteiro([
        f"Média caiu de {_dec(ini['remuneracao_sm'], 2)} para {_dec(fim['remuneracao_sm'], 2)} SM: o mínimo teve ganho real que a remuneração de TI não acompanhou.",
        f"Média ({_dec(fim['remuneracao_sm'], 2)}) bem acima da mediana ({_dec(fim['remuneracao_sm_mediana'], 2)}): poucos salários altos puxam o número citado.",
        "Tracejado vermelho: o que a série mostraria sem corrigir a conversão invertida da fonte — uma valorização falsa de 43%.",
    ])


def s_genero():
    df = dm.hiato("MASCULINO vs FEMININO").sort_values("ano")
    if df.empty:
        return
    ini, fim = df.iloc[0], df.iloc[-1]
    _cabecalho("Mulheres ganham menos por causa do perfil?",
               f"Não. Pelo perfil elas deveriam ganhar mais — o hiato bruto de "
               f"{_pct(fim['hiato_pct'])} esconde uma diferença maior")
    negativos = df[df["hiato_log"] < 0]["ano"].astype(int).tolist()
    fig = go.Figure()
    if negativos:
        fig.add_vrect(x0=min(negativos) - 0.5, x1=max(negativos) + 0.5, fillcolor=tema.SERIE_4,
                      opacity=0.13, line_width=0, annotation_text="hiato bruto negativo",
                      annotation_position="top left")
    fig.add_hline(y=0, line_width=1, line_color=tema.TEXTO_SEC)
    fig.add_trace(go.Scatter(x=df["ano"], y=df["hiato_log"], name="Hiato bruto",
                             mode="lines+markers", line=dict(color=tema.SERIE_1, width=4)))
    fig.add_trace(go.Scatter(x=df["ano"], y=df["nao_explicada"], name="Diferença comparando perfis iguais",
                             mode="lines+markers", line=dict(color=tema.NEGATIVO, width=4)))
    fig = _layout(fig, legenda=True)
    fig.update_yaxes(title="pontos log (≈ %)")
    _grafico(fig)
    _roteiro([
        "Oaxaca-Blinder separa o hiato em perfil (escolaridade, tempo de casa, área, jornada, porte, região) e no que sobra comparando iguais.",
        f"A parte explicada é negativa ({_dec(fim['explicada'], 3)}): mulheres em TI têm perfil que deveria render mais.",
        f"A linha vermelha quase não se move: {_dec(ini['nao_explicada'], 3)} em {int(ini['ano'])}, {_dec(fim['nao_explicada'], 3)} em {int(fim['ano'])} — mesmo quando o hiato bruto ficou negativo.",
        "Não é medida de discriminação: é o limite superior da diferença de retorno.",
    ])


def s_raca():
    comparacoes = [("Homens × mulheres", "MASCULINO vs FEMININO"),
                   ("Branca × parda", "BRANCA vs PARDA"),
                   ("Branca × preta", "BRANCA vs PRETA")]
    linhas = []
    for nome, chave in comparacoes:
        df = dm.hiato(chave)
        if not df.empty:
            u = df.sort_values("ano").iloc[-1]
            linhas.append((nome, u))
    if not linhas:
        return
    ano = int(linhas[-1][1]["ano"])
    _cabecalho(f"E a desigualdade racial? · {ano}",
               "Raça: a desigualdade é de acesso. Gênero: é de retorno")
    nomes = [n for n, _ in linhas]
    fig = go.Figure()
    fig.add_trace(go.Bar(y=nomes, x=[u["explicada"] for _, u in linhas], orientation="h",
                         name="Explicada pelo perfil", marker_color=tema.SERIE_3))
    fig.add_trace(go.Bar(y=nomes, x=[u["nao_explicada"] for _, u in linhas], orientation="h",
                         name="Não explicada", marker_color=tema.NEGATIVO))
    fig.add_trace(go.Scatter(y=nomes, x=[u["hiato_log"] for _, u in linhas], mode="markers+text",
                             name="Hiato total", marker=dict(symbol="diamond", size=16, color=tema.TEXTO),
                             text=[_pct(u["hiato_pct"]) for _, u in linhas], textposition="middle right",
                             textfont=dict(size=16)))
    fig.add_vline(x=0, line_width=1, line_color=tema.TEXTO_SEC)
    fig = _layout(fig, 460, legenda=True)
    fig.update_layout(barmode="relative")
    fig.update_yaxes(autorange="reversed")
    fig.update_xaxes(title="pontos log")
    _grafico(fig)
    notas = []
    for nome, u in linhas:
        if u["explicada"] < 0:
            notas.append(f"{nome}: hiato {_pct(u['hiato_pct'])}, parte explicada negativa — o perfil não explica.")
        elif pd.notna(u["explicada_share"]):
            notas.append(f"{nome}: hiato {_pct(u['hiato_pct'])}, {_pct(u['explicada_share'], 0)} explicado por perfil (região e área).")
    notas.append("Consequência: a política que corrige uma desigualdade não corrige a outra.")
    _roteiro(notas)


def s_estabilidade():
    risco, porte, sexo = dm.sobrevivencia_risco(), dm.sobrevivencia_tabua("porte"), dm.sobrevivencia_tabua("sexo")
    if risco.empty:
        return
    pico = risco.loc[risco["risco_%"].idxmax()]
    _cabecalho("Quanto dura um emprego de TI?",
               "O risco de desligamento é máximo no primeiro ano, e o porte da empresa pesa mais que o sexo")
    c1, c2 = st.columns(2)
    with c1:
        fig = go.Figure(go.Bar(
            x=risco["tempo_de_casa"], y=risco["risco_%"],
            marker_color=[tema.SERIE_1 if t == pico["tempo_de_casa"] else tema.GRID for t in risco["tempo_de_casa"]],
            text=[_pct(v) for v in risco["risco_%"]], textposition="outside", cliponaxis=False,
            textfont=dict(size=16)))
        fig = _layout(fig, 470)
        fig.update_layout(title=dict(text="Risco anual por tempo de casa", font=dict(size=16)))
        _grafico(fig)
    with c2:
        if not porte.empty and "5-10a" in porte:
            p = porte.dropna(subset=["5-10a"]).sort_values("5-10a")
            fig = go.Figure(go.Bar(y=p["grupo"], x=p["5-10a"], orientation="h", marker_color=tema.SERIE_3,
                                   text=[_pct(v) for v in p["5-10a"]], textposition="outside",
                                   cliponaxis=False, textfont=dict(size=15)))
            fig = _layout(fig, 470)
            fig.update_layout(title=dict(text="Chegam a 5–10 anos, por porte", font=dict(size=16)),
                              margin=dict(l=8, r=60))
            _grafico(fig)
    notas = [f"Pico de risco com {pico['tempo_de_casa']} de casa: {_pct(pico['risco_%'])} ao ano."]
    if not sexo.empty and "5-10a" in sexo:
        s = sexo.set_index("grupo")["5-10a"]
        if {"MASCULINO", "FEMININO"} <= set(s.index):
            notas.append(f"Homens {_pct(s['MASCULINO'])} contra mulheres {_pct(s['FEMININO'])}: a desigualdade de gênero está no salário, não na permanência.")
    notas.append("Método: tábua de período. Kaplan-Meier direto dava 48 anos de mediana — erro de corte transversal que foi corrigido.")
    _roteiro(notas)


def s_futuro():
    prev, mensal, placar = dm.previsao(), dm.serie_mensal(), dm.previsao_placar()
    retido, proj = dm.nowcast_ano_retido(), dm.nowcast_projecao()
    if prev.empty or mensal.empty:
        return
    prev = prev.assign(mes=pd.to_datetime(prev["mes"]))
    doze = prev.head(12)
    soma, lo, hi = doze["previsao"].sum(), doze["inferior"].sum(), doze["superior"].sum()
    _cabecalho("Para onde vai?",
               f"Crescimento continua, mas lento: cerca de {fmt_compacto(soma)} vagas em 12 meses")
    col_g, col_n = st.columns([2.4, 1])
    with col_g:
        obs = mensal.tail(8)
        fig = go.Figure()
        fig.add_trace(go.Bar(x=obs["mes"], y=obs["saldo"], name="Observado",
                             marker_color=[tema.POSITIVO if v >= 0 else tema.NEGATIVO for v in obs["saldo"]]))
        fig.add_trace(go.Bar(
            x=doze["mes"], y=doze["previsao"], name="Previsto (intervalo de 80%)",
            marker_color=tema.GRID, marker_line_color=tema.SERIE_1, marker_line_width=1.4,
            error_y=dict(type="data", symmetric=False,
                         array=(doze["superior"] - doze["previsao"]).tolist(),
                         arrayminus=(doze["previsao"] - doze["inferior"]).tolist(),
                         color=tema.SERIE_1, thickness=1.6)))
        fig.add_hline(y=0, line_width=1, line_color=tema.TEXTO_SEC)
        _grafico(_layout(fig, 500, legenda=True))
    with col_n:
        st.metric("Próximos 12 meses", fmt_compacto(soma),
                  f"faixa de {fmt_compacto(lo)} a {fmt_compacto(hi)}", delta_color="off")
        if not proj.empty:
            p = proj.iloc[0]
            st.metric(f"Estoque estimado {int(p['ano'])}", fmt_compacto(p["estoque_estimado"]),
                      f"± {fmt_compacto(p['margem'])}", delta_color="off")
        if not retido.empty:
            m = retido.loc[retido["erro_pct"].abs().idxmin()]
            st.metric(f"Erro ao prever {int(m['ano_teste'])}", _pct(m["erro_pct"]),
                      f"treinado só até {int(m['ano_teste']) - 1}", delta_color="off")
    notas = ["A previsão sustenta uma direção — crescimento modesto —, não um número exato. O certo é ler o intervalo."]
    if not placar.empty and "ganho_vs_naive_%" in placar:
        notas.append(f"SARIMA escolhido por validação em origem móvel: erra {_pct(placar.iloc[0]['ganho_vs_naive_%'])} menos que a regra ingênua.")
    notas.append("Nowcast: como a RAIS sai com um ano de atraso, o estoque é estimado pelo CAGED — validado prevendo 2025 com dados só até 2024.")
    _roteiro(notas)


def s_rigor():
    anual = dr.estoque_anual()
    descartados = int(anual["remun_descartada"].sum()) if not anual.empty and "remun_descartada" in anual else 0
    _cabecalho("Rigor",
               "Três armadilhas na fonte e no método — encontradas, medidas e corrigidas")
    c1, c2, c3 = st.columns(3)
    cartoes = [
        ("+43%", "Valorização salarial que não existiu",
         f"A RAIS de 2023 inverte a conversão para salário mínimo em {fmt_num(descartados)} registros. "
         "Eram 0,02% dos vínculos e 35% da massa salarial. Detectado pelo salário mínimo implícito."),
        ("48 anos", "Duração de emprego impossível",
         "Kaplan-Meier aplicado a um retrato de um ano inflava o grupo em risco. A tábua de período "
         "corrigiu para uma mediana de 2 a 3 anos."),
        ("1 → 1.428", "Cidade de 10 mil habitantes",
         "Guaraciaba (MG) em um ano: registro de empresa por causa do ISS. A RAIS localiza a sede, "
         "não o trabalho — limitação declarada nos mapas."),
    ]
    for col, (grande, titulo, texto) in zip((c1, c2, c3), cartoes):
        col.markdown(f'<div class="slide-cartao"><div class="grande">{grande}</div>'
                     f'<div class="titulo">{titulo}</div><p>{texto}</p></div>', unsafe_allow_html=True)
    _roteiro(["Os três problemas passavam sem erro de execução: o número saía plausível e errado.",
              "O que os revelou foi conferir números contra referências externas — o salário mínimo oficial, o bom senso de duração de emprego, a população do município."])


def s_conclusoes():
    _cabecalho("Conclusões", "O que a pesquisa permite afirmar")
    anual = dr.estoque_anual().sort_values("ano")
    lentes = dr.lentes()
    sexo = dm.hiato("MASCULINO vs FEMININO").sort_values("ano")
    prev, proj = dm.previsao(), dm.nowcast_projecao()

    itens = []
    if not anual.empty:
        ini, fim = anual.iloc[0], anual.iloc[-1]
        itens.append(f"**O mercado formal de TI mais que dobrou** — de {fmt_compacto(ini['estoque'])} para {fmt_compacto(fim['estoque'])} vínculos.")
    if not lentes.empty:
        a = lentes[lentes["ano"] == lentes["ano"].max()].set_index("categoria")["estoque"]
        if {ah.LENTE_FORA, ah.LENTE_DENTRO} <= set(a.index):
            itens.append(f"**TI é função, não setor** — {_pct(a[ah.LENTE_FORA] / (a[ah.LENTE_FORA] + a[ah.LENTE_DENTRO]) * 100, 0)} dos profissionais estão fora das empresas de TI, ganhando mais.")
    if not anual.empty:
        med = anual["remuneracao_sm_mediana"]
        itens.append(f"**Crescer não virou salário relativo** — mediana entre {_dec(med.min(), 1)} e {_dec(med.max(), 1)} SM em toda a série.")
    if not sexo.empty:
        itens.append(f"**O hiato de gênero é maior do que parece** — o bruto de {_pct(sexo.iloc[-1]['hiato_pct'])} esconde uma diferença de retorno que quase não mudou.")
    itens.append("**A desigualdade racial é de acesso** — explicada sobretudo por região e área de atuação.")
    itens.append("**Estabilidade depende do porte da empresa** — risco máximo no primeiro ano.")
    if not prev.empty and not proj.empty:
        itens.append(f"**O mercado segue crescendo, devagar** — {fmt_compacto(prev.head(12)['previsao'].sum())} vagas em 12 meses; estoque de {fmt_compacto(proj.iloc[0]['estoque_estimado'])} em {int(proj.iloc[0]['ano'])}.")

    metade = (len(itens) + 1) // 2
    c1, c2 = st.columns(2)
    for col, bloco, inicio in ((c1, itens[:metade], 1), (c2, itens[metade:], metade + 1)):
        # Dentro de HTML o markdown não é interpretado: `**x**` sairia com os
        # asteriscos visíveis no projetor. A troca por <strong> é explícita.
        col.markdown('<div class="slide-lista">' + "".join(
            f"<p>{i}. {re.sub(r'[*][*](.+?)[*][*]', r'<strong>\1</strong>', t)}</p>"
            for i, t in enumerate(bloco, start=inicio)) + "</div>",
            unsafe_allow_html=True)


def s_limites():
    _cabecalho("Limitações e próximos passos", "O que os dados não permitem afirmar — e o que vem depois")
    obs = pd.DataFrame(ah.CBO_OBSERVADOS_2024, columns=["familia", "cbo", "ocupacao", "vinculos", "obs"])
    a_mais = obs[obs["obs"] == "revisar"]["vinculos"].sum()
    parte = a_mais / obs["vinculos"].sum() * 100
    c1, c2 = st.columns(2)
    c1.markdown(
        '<div class="slide-lista">'
        "<p><strong>Trajetórias individuais.</strong> A RAIS pública não tem identificador do trabalhador.</p>"
        "<p><strong>Onde as pessoas trabalham.</strong> A localização é a sede do estabelecimento.</p>"
        "<p><strong>Discriminação.</strong> A parte não explicada do hiato é limite superior, não causa.</p>"
        "<p><strong>Série do CAGED.</strong> Em 2020 muda a data de referência da movimentação.</p>"
        "</div>", unsafe_allow_html=True)
    c2.markdown(
        '<div class="slide-lista">'
        f"<p><strong>Recorte a revisar.</strong> A seleção por família de CBO trouxe ocupações que não são de TI "
        f"(pesquisadores de ciências naturais, programador CNC): {fmt_num(a_mais)} vínculos, ~{_pct(parte, 0)}.</p>"
        "<p><strong>Recorte a ampliar.</strong> A família 2112 (estatísticos), onde muitos analistas de dados "
        "são registrados, ficou de fora: 2.324 admissões no CAGED de 2025, 70–75% fora de empresa de TI.</p>"
        "<p><strong>Extensões.</strong> Retenção por gênero (entrada no CAGED × estoque na RAIS) e "
        "decomposição regional shift-share.</p>"
        "</div>", unsafe_allow_html=True)


def s_apendice():
    ah._apendice()


SLIDES = [
    ("Capa", s_capa),
    ("Como ler", s_bases),
    ("Tamanho", s_tamanho),
    ("TI como função", s_funcao),
    ("Mapa", s_mapa),
    ("Remuneração", s_remuneracao),
    ("Gênero", s_genero),
    ("Raça", s_raca),
    ("Estabilidade", s_estabilidade),
    ("Futuro", s_futuro),
    ("Rigor", s_rigor),
    ("Conclusões", s_conclusoes),
    ("Limitações", s_limites),
    ("Apêndice: recorte", s_apendice),
]


# ================================================================= render
def render():
    _css()
    total = len(SLIDES)
    try:
        atual = int(st.query_params.get(PARAM_SLIDE, "1"))
    except ValueError:
        atual = 1
    atual = min(max(atual, 1), total)
    texto_corrido = st.query_params.get(PARAM_FORMATO) == "texto"

    topo = st.columns([5, 1.6, 1.2, 1])
    with topo[0]:
        if texto_corrido:
            st.caption("MODO APRESENTAÇÃO · texto corrido")
        else:
            nome = SLIDES[atual - 1][0]
            escolha = st.selectbox("Ir para", [f"{i}. {n}" for i, (n, _) in enumerate(SLIDES, 1)],
                                   index=atual - 1, label_visibility="collapsed",
                                   key=f"ir_para_{atual}")
            destino = int(escolha.split(".")[0])
            if destino != atual:
                _ir(destino)
                st.rerun()
    topo[1].button("Ver como slides" if texto_corrido else "Ver texto corrido",
                   on_click=_alternar_formato, width="stretch", key="formato")
    with topo[2]:
        _controles_navegador()
    topo[3].button("Sair", on_click=_sair, width="stretch", key="sair",
                   icon=":material/close:")

    if texto_corrido:
        ah.render()
        return

    nome, slide = SLIDES[atual - 1]
    try:
        slide()
    except Exception as e:  # noqa: BLE001
        # Numa apresentação ao vivo, um gráfico que falha não pode derrubar a
        # página inteira — mas também não pode sumir calado.
        st.warning(f"O slide “{nome}” não pôde ser montado: {str(e)[:200]}")

    st.markdown("")
    baixo = st.columns([1, 4, 1])
    baixo[0].button("← Anterior", on_click=_ir, args=(atual - 1,), disabled=atual == 1,
                    width="stretch", key="anterior")
    with baixo[1]:
        st.progress(atual / total, text=f"{atual} / {total} · {nome} · use as setas do teclado")
    baixo[2].button("Próximo →", on_click=_ir, args=(atual + 1,), disabled=atual == total,
                    width="stretch", key="proximo", type="primary")
