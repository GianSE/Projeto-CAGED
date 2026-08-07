"""
Paleta e formatação do dashboard, com suporte a tema claro e escuro.

DOIS TEMAS, NÃO UM INVERTIDO
----------------------------
As cores do modo escuro não são as do claro com brilho ajustado: são passos
próprios das mesmas famílias de cor, escolhidos para a superfície escura.
Inverter mecanicamente produz cores que ou somem no fundo ou vibram demais —
o azul claro sobre preto, por exemplo, cansa a vista e perde contraste
relativo com o vermelho ao lado.

O QUE NÃO MUDA ENTRE OS TEMAS
-----------------------------
O PAPEL de cada cor. Saldo continua usando escala divergente (azul/vermelho,
porque tem polaridade), admissões e desligamentos continuam nos mesmos dois
slots categóricos. Só o passo muda.

Verde/vermelho seria o par intuitivo para ganho/perda, mas é justamente o que
daltônicos não distinguem — e o sinal do saldo é a informação principal do
painel. Azul/vermelho preserva a leitura para todo mundo, nos dois temas.
"""
import streamlit as st

FONTE = 'system-ui, -apple-system, "Segoe UI", sans-serif'

# Slots categóricos, sempre nesta ordem — a cor segue a entidade, não o
# ranking, para uma série não trocar de cor quando um filtro muda a ordenação.
_CLARO = {
    "serie_1": "#2a78d6",   # azul
    "serie_2": "#eb6834",   # laranja
    # Um passo mais escuro que o verde-água padrão da paleta: o original
    # (#1baf7a) fica em 2,7:1 sobre a superfície clara, abaixo do mínimo de
    # 3:1. Como esta cor desenha a LINHA do gráfico de salário — onde o traço
    # é o próprio dado, sem tabela ao lado — ela precisa se sustentar sozinha.
    "serie_3": "#158a62",
    "serie_4": "#a86e00",   # âmbar; o amarelo puro não alcança 3:1 no claro
    "serie_5": "#d55181",   # magenta, um passo mais escuro pelo mesmo motivo
    "positivo": "#2a78d6",
    "negativo": "#e34948",
    "texto": "#0b0b0b",
    "texto_sec": "#52514e",
    "muted": "#898781",
    "grid": "#e1e0d9",
    "superficie": "#fcfcfb",
}

_ESCURO = {
    "serie_1": "#3987e5",
    "serie_2": "#d95926",
    "serie_3": "#199e70",
    "serie_4": "#c98500",
    "serie_5": "#d55181",
    "positivo": "#3987e5",
    "negativo": "#e66767",
    "texto": "#ffffff",
    "texto_sec": "#c3c2b7",
    "muted": "#898781",   # funciona nos dois: fica a meio caminho
    "grid": "#2c2c2a",
    "superficie": "#1a1a19",
}


def _escuro() -> bool:
    """
    Detecta o tema ativo.

    `st.context.theme` reflete a escolha do usuário em tempo de execução
    (inclusive o botão de tema do menu), diferente de `get_option`, que só
    enxerga a configuração do servidor. Se a API não estiver disponível,
    assume claro — o padrão do Streamlit — em vez de arriscar texto branco
    sobre fundo branco.
    """
    try:
        return (st.context.theme.type or "light").lower() == "dark"
    except Exception:
        return False


def cores() -> dict:
    return _ESCURO if _escuro() else _CLARO


# ---------------------------------------------------------------------------
# Acesso por atributo (tema.SERIE_1, tema.POSITIVO, ...). Resolvido a cada
# leitura via __getattr__ do módulo, porque o tema pode mudar entre execuções
# do script sem o processo reiniciar — constantes fixadas na importação
# ficariam presas no tema da primeira sessão.
# ---------------------------------------------------------------------------
_ALIAS = {
    "SERIE_1": "serie_1", "SERIE_2": "serie_2", "SERIE_3": "serie_3",
    "SERIE_4": "serie_4", "SERIE_5": "serie_5",
    "POSITIVO": "positivo", "NEGATIVO": "negativo",
    "COR_ADMISSAO": "serie_1", "COR_DESLIGAMENTO": "serie_2",
    "TEXTO": "texto", "TEXTO_SEC": "texto_sec", "MUTED": "muted",
    "GRID": "grid", "SUPERFICIE": "superficie",
}


def __getattr__(nome: str):
    if nome in _ALIAS:
        return cores()[_ALIAS[nome]]
    if nome == "CATEGORICA":
        c = cores()
        return [c["serie_1"], c["serie_2"], c["serie_3"], c["serie_4"], c["serie_5"]]
    raise AttributeError(f"module 'tema' has no attribute {nome!r}")


def layout_base(altura=340, mostrar_legenda=True, hover_unificado=False):
    """
    Layout comum dos gráficos: grade discreta, sem moldura, fundo transparente.

    O fundo transparente é o que faz o gráfico acompanhar o tema da página
    sem precisar repintar nada — só a tinta (texto, grade) muda.

    `hover_unificado` liga a leitura em conjunto para séries temporais: uma
    linha-guia vertical segue o cursor e o tooltip traz TODAS as séries
    daquele instante de uma vez. Sem isso, comparar admissões e desligamentos
    exigiria passar o mouse em cada curva separadamente e guardar o número de
    cabeça — justamente a comparação que o gráfico existe para permitir.
    """
    c = cores()
    layout = dict(
        height=altura,
        margin=dict(l=8, r=8, t=28, b=8),
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        font=dict(family=FONTE, size=12, color=c["texto_sec"]),
        xaxis=dict(gridcolor=c["grid"], zeroline=False, linecolor=c["grid"]),
        yaxis=dict(gridcolor=c["grid"], zeroline=True,
                   zerolinecolor=c["muted"], linecolor=c["grid"]),
        showlegend=mostrar_legenda,
        legend=dict(orientation="h", yanchor="bottom", y=1.0, xanchor="left", x=0,
                    font=dict(size=11)),
        hoverlabel=dict(font=dict(family=FONTE, size=12, color=c["texto"]),
                        bgcolor=c["superficie"], bordercolor=c["grid"]),
    )

    if hover_unificado:
        layout["hovermode"] = "x unified"
        # A linha-guia mostra exatamente qual ponto do eixo está sendo lido —
        # com duas curvas próximas, sem ela a leitura fica ambígua.
        layout["xaxis"].update(
            showspikes=True, spikemode="across", spikesnap="cursor",
            spikecolor=c["muted"], spikethickness=1, spikedash="dot",
        )

    return layout


def css() -> str:
    """CSS da página, montado com as cores do tema ativo."""
    c = cores()
    return f"""
<style>
  .block-container {{ padding-top: 2rem; max-width: 1240px; }}
  [data-testid="stMetricValue"] {{ font-size: 1.7rem; font-family: {FONTE}; }}
  [data-testid="stMetricLabel"] {{ color: {c['texto_sec']}; }}
  h1, h2, h3 {{ font-family: {FONTE}; }}
  .lead {{ font-size: 1.05rem; line-height: 1.65; color: {c['texto_sec']};
           margin-bottom: 6px; }}
  .leitura {{ font-size: 0.95rem; line-height: 1.65; color: {c['texto_sec']};
              border-left: 3px solid {c['serie_1']}; padding: 2px 0 2px 14px;
              margin: 8px 0 18px 0; }}
  .rodape {{ color: {c['muted']}; font-size: 12px; margin-top: 16px; line-height: 1.6; }}
  .stTabs [data-baseweb="tab"] {{ font-size: 0.95rem; }}
</style>
"""


# ------------------------------------------------------------- formatação
def fmt_num(n) -> str:
    """Inteiro no padrão brasileiro (1.234.567)."""
    if n is None:
        return "–"
    return f"{int(n):,}".replace(",", ".")


def fmt_compacto(n) -> str:
    """Números grandes em escala legível: 1,2 mi em vez de 1.234.567."""
    if n is None:
        return "–"
    n = float(n)
    sinal = "-" if n < 0 else ""
    n = abs(n)
    if n >= 1e6:
        return f"{sinal}{n / 1e6:.2f} mi".replace(".", ",")
    if n >= 1e3:
        return f"{sinal}{n / 1e3:.0f} mil"
    return f"{sinal}{n:.0f}"


def fmt_reais(v) -> str:
    if v is None:
        return "–"
    return f"R$ {v:,.2f}".replace(",", "@").replace(".", ",").replace("@", ".")
