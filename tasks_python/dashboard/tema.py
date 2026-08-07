"""
Paleta e helpers de formatação do dashboard.

As cores seguem uma paleta categórica validada para daltonismo (separação
mínima em CVD) e para contraste sobre a superfície clara. O ponto principal:
cada cor tem um PAPEL, e o papel é escolhido pelo trabalho que o dado faz.

  - saldo é uma grandeza com POLARIDADE (positivo = vagas criadas, negativo =
    vagas perdidas), então usa uma escala divergente azul <-> vermelho com
    cinza no meio. Verde/vermelho seria a escolha intuitiva, mas é justamente
    o par que daltônicos não distinguem — e o sinal do saldo é a informação
    mais importante do painel inteiro.
  - admissões vs desligamentos são duas séries de IDENTIDADE, então usam dois
    slots categóricos fixos (nunca reciclados entre gráficos).
"""

# Slots categóricos, sempre nesta ordem — cor segue a entidade, não o ranking,
# para a mesma série não trocar de cor quando um filtro muda a ordenação.
SERIE_1 = "#2a78d6"  # azul
SERIE_2 = "#eb6834"  # laranja
SERIE_3 = "#1baf7a"  # verde-água
SERIE_4 = "#eda100"  # amarelo
SERIE_5 = "#e87ba4"  # magenta
CATEGORICA = [SERIE_1, SERIE_2, SERIE_3, SERIE_4, SERIE_5, "#008300", "#4a3aa7", "#e34948"]

# Escala divergente para o saldo (polaridade)
POSITIVO = "#2a78d6"
NEGATIVO = "#e34948"
NEUTRO = "#f0efec"

# Papéis fixos das duas séries de fluxo
COR_ADMISSAO = SERIE_1
COR_DESLIGAMENTO = SERIE_2

# Tinta e superfícies
TEXTO = "#0b0b0b"
TEXTO_SEC = "#52514e"
MUTED = "#898781"
GRID = "#e1e0d9"
SUPERFICIE = "#fcfcfb"

FONTE = 'system-ui, -apple-system, "Segoe UI", sans-serif'


def layout_base(altura=340, mostrar_legenda=True):
    """Layout comum: grade discreta, sem moldura, tipografia do sistema."""
    return dict(
        height=altura,
        margin=dict(l=8, r=8, t=28, b=8),
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        font=dict(family=FONTE, size=12, color=TEXTO_SEC),
        xaxis=dict(gridcolor=GRID, zeroline=False, linecolor=GRID),
        yaxis=dict(gridcolor=GRID, zeroline=True, zerolinecolor=MUTED, linecolor=GRID),
        showlegend=mostrar_legenda,
        legend=dict(orientation="h", yanchor="bottom", y=1.0, xanchor="left", x=0,
                    font=dict(size=11)),
        hoverlabel=dict(font=dict(family=FONTE, size=12)),
    )


def fmt_num(n) -> str:
    """Formata inteiro no padrão brasileiro (1.234.567)."""
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
