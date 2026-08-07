"""
Texto interpretativo derivado dos próprios dados.

Um dashboard que só empilha gráficos deixa a leitura por conta do visitante.
Aqui cada seção vem com uma frase que diz o que o dado mostra — mas os
números dessa frase são CALCULADOS, nunca escritos à mão: quando a série for
atualizada (novos meses, novo recorte), o texto acompanha em vez de virar
mentira silenciosa.
"""
import pandas as pd


def _fmt(n) -> str:
    return f"{int(n):+,}".replace(",", ".")


def _fmt_abs(n) -> str:
    return f"{int(abs(n)):,}".replace(",", ".")


def arco_historico(anual: pd.DataFrame) -> dict:
    """
    Extrai os marcos da série: melhor ano, pior ano, anos negativos e a
    correção mais forte. É o esqueleto da narrativa da página.
    """
    if anual.empty:
        return {}

    # O último ano costuma estar incompleto (a divulgação do CAGED é mensal);
    # incluí-lo nas comparações faria o "pior ano" ser sempre o ano corrente.
    ultimo = int(anual["ano"].max())
    fechados = anual[anual["ano"] < ultimo]
    if fechados.empty:
        fechados = anual

    melhor = fechados.loc[fechados["saldo"].idxmax()]
    negativos = fechados[fechados["saldo"] < 0].sort_values("ano")

    # Maior queda ano a ano, em pontos de saldo
    serie = fechados.sort_values("ano").reset_index(drop=True)
    serie["variacao"] = serie["saldo"].diff()
    queda = serie.loc[serie["variacao"].idxmin()] if len(serie) > 1 else None

    primeiro = int(anual["ano"].min())
    sal_ini = anual[anual["ano"] == primeiro]["salario_medio"].iloc[0]
    sal_fim = anual[anual["ano"] == ultimo]["salario_medio"].iloc[0]

    return {
        "ano_inicio": primeiro,
        "ano_fim": ultimo,
        "saldo_total": int(anual["saldo"].sum()),
        "admissoes_total": int(anual["admissoes"].sum()),
        "melhor_ano": int(melhor["ano"]),
        "melhor_saldo": int(melhor["saldo"]),
        "anos_negativos": [int(a) for a in negativos["ano"]],
        "saldo_negativo": int(negativos["saldo"].sum()) if not negativos.empty else 0,
        "ano_queda": int(queda["ano"]) if queda is not None else None,
        "queda_valor": int(queda["variacao"]) if queda is not None else None,
        "queda_de": int(serie.loc[serie["variacao"].idxmin() - 1, "saldo"])
                    if queda is not None and serie["variacao"].idxmin() > 0 else None,
        "salario_inicio": float(sal_ini) if pd.notna(sal_ini) else None,
        "salario_fim": float(sal_fim) if pd.notna(sal_fim) else None,
    }


def texto_abertura(a: dict) -> str:
    if not a:
        return ""
    anos = a["ano_fim"] - a["ano_inicio"]
    return (
        f"Entre {a['ano_inicio']} e {a['ano_fim']}, o mercado formal de tecnologia "
        f"no Brasil gerou **{_fmt(a['saldo_total'])} vagas líquidas** — resultado de "
        f"{_fmt_abs(a['admissoes_total'])} admissões e dos desligamentos do período. "
        f"Em {anos} anos, o setor fechou no vermelho em apenas "
        f"**{len(a['anos_negativos'])}**."
    )


def texto_serie(a: dict) -> str:
    if not a:
        return ""
    partes = []

    if a["anos_negativos"]:
        anos = " e ".join(str(x) for x in a["anos_negativos"])
        partes.append(
            f"Os únicos anos de saldo negativo foram **{anos}**, no auge da recessão "
            f"brasileira — e ainda assim a perda somada ({_fmt_abs(a['saldo_negativo'])} "
            f"vagas) foi menor que o ganho de um único ano bom."
        )

    partes.append(
        f"O pico veio em **{a['melhor_ano']}**, com {_fmt(a['melhor_saldo'])} vagas: "
        f"a digitalização acelerada pela pandemia."
    )

    if a["ano_queda"] and a["queda_de"]:
        queda_pct = abs(a["queda_valor"]) / a["queda_de"] * 100 if a["queda_de"] else 0
        partes.append(
            f"A correção veio em **{a['ano_queda']}**, quando o saldo caiu "
            f"{queda_pct:.0f}% — a onda global de demissões em tecnologia."
        )

    return " ".join(partes)


def _reais(v) -> str:
    """R$ no padrão brasileiro. Formata só o número — aplicar replace na frase
    inteira trocaria também as vírgulas do texto por pontos."""
    return "R$ " + f"{v:,.0f}".replace(",", ".")


def texto_salario(a: dict) -> str:
    if not a or not a["salario_inicio"] or not a["salario_fim"]:
        return ""
    mult = f"{a['salario_fim'] / a['salario_inicio']:.1f}".replace(".", ",")
    return (
        f"O salário médio de admissão saiu de {_reais(a['salario_inicio'])} para "
        f"{_reais(a['salario_fim'])} — **{mult}× em termos nominais**. "
        f"Note que ele seguiu subindo mesmo nos anos de saldo negativo: a crise "
        f"cortou vagas, não remuneração."
    )


def texto_lentes(fora_pct: float, total_prof: int) -> str:
    return (
        f"**{fora_pct:.0f}% dos profissionais de tecnologia contratados não trabalham "
        f"em empresas de tecnologia.** De {_fmt_abs(total_prof)} admissões em ocupações "
        f"de TI, a maioria foi feita por bancos, varejo, indústria, saúde e governo. "
        f"Uma análise que olhasse só para o CNAE de tecnologia perderia esse contingente."
    )
