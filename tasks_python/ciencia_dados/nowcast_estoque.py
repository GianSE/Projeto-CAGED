"""
Nowcast do estoque da RAIS a partir do fluxo do CAGED.

O PROBLEMA QUE ISTO RESOLVE
---------------------------
A RAIS é anual e sai com cerca de um ano de atraso. O CAGED é mensal e está
sempre em dia. Então existe uma janela — hoje, de quase dois anos — em que se
sabe quanto o mercado se moveu, mas não de que tamanho ele ficou.

A ponte é contábil na intuição: o estoque de dezembro deste ano é o de
dezembro passado mais o que entrou e menos o que saiu no meio. Se o saldo do
CAGED medisse exatamente esse líquido, bastaria somar.

Não mede. As duas bases têm universos e regras de captação diferentes, e a
razão entre a variação do estoque e o saldo acumulado varia de 0,12 a 2,43 ao
longo da série. Mas a correlação é de 0,82, e é isso que sustenta estimar em
vez de somar.

COMO ISTO É VALIDADO
--------------------
Treinando só com o passado e prevendo um ano que o modelo nunca viu — a ideia
é do usuário, e é a certa. Duas camadas:

  1. RETROSPECTIVA ANO A ANO: para cada ano-alvo, ajusta só com os anos
     anteriores e prevê. Nunca usa informação do futuro. Acertar um ano pode
     ser sorte; acertar uma sequência, não.
  2. O ANO RETIDO: treina até 2024, prevê 2025 e compara com o valor real, que
     já está publicado. É o teste que o usuário pediu, e é o mais próximo de
     usar o modelo para valer.

BASELINES OBRIGATÓRIOS
----------------------
Três, e o modelo precisa ganhar dos três para justificar existir:

    ingênuo     -> o estoque não muda
    identidade  -> o estoque muda exatamente o saldo do CAGED
    razão       -> muda a razão MEDIANA histórica vezes o saldo

A identidade é a hipótese que qualquer pessoa tentaria primeiro. Se a
regressão não a superar, a resposta honesta é usar a identidade e dizer que a
relação é 1:1 — não publicar um modelo mais complicado pelo mesmo resultado.

Uso:
    python -m ciencia_dados.nowcast_estoque
    python -m ciencia_dados.nowcast_estoque --ano-teste 2024
"""
import argparse
import sys
import warnings

import numpy as np
import pandas as pd

from ciencia_dados import serie as sr
from extracao_ftp.config_extracao import BUCKET_GOLD, conectar_duckdb

warnings.simplefilter("ignore")

SQL_ESTOQUE = f"""
SELECT ano, sum(estoque_3112) AS estoque
FROM read_parquet('s3://{BUCKET_GOLD}/rais_estoque_anual.parquet')
WHERE setor_ti OR ocupacao_ti
GROUP BY 1 ORDER BY 1
"""


def montar(con=None) -> pd.DataFrame:
    """
    Uma linha por ano: estoque da RAIS, saldo do CAGED, e a variação a explicar.

    Anos INCOMPLETOS do CAGED são descartados. O ano corrente tem só parte dos
    meses, e usá-lo compararia um saldo parcial contra um estoque de dezembro
    — o modelo aprenderia que o fluxo "encolheu" no último ano e projetaria
    queda onde não há.
    """
    con = con or conectar_duckdb()
    con.execute("SET enable_progress_bar=false")

    estoque = con.execute(SQL_ESTOQUE).df()
    estoque["ano"] = estoque["ano"].astype(int)

    mensal = sr.carregar(con)
    fluxo = mensal.groupby(mensal.index.year).agg(
        fluxo=("saldo", "sum"), meses=("saldo", "size")).reset_index()
    fluxo.columns = ["ano", "fluxo", "meses"]

    df = estoque.merge(fluxo, on="ano", how="inner")
    incompletos = df.loc[df["meses"] < 12, "ano"].tolist()
    df = df[df["meses"] == 12].copy()
    df["delta"] = df["estoque"].diff()
    df["estoque_anterior"] = df["estoque"].shift(1)
    df.attrs["incompletos"] = incompletos
    return df.dropna(subset=["delta"]).reset_index(drop=True)


# --- estimadores -----------------------------------------------------------
# Cada um recebe o histórico de treino e devolve a variação prevista para um
# saldo do CAGED. Mesma assinatura para todos, para que o backtest não precise
# saber qual é qual.
def _ingenuo(treino: pd.DataFrame, fluxo: float) -> float:
    return 0.0


def _identidade(treino: pd.DataFrame, fluxo: float) -> float:
    return float(fluxo)


def _razao(treino: pd.DataFrame, fluxo: float) -> float:
    """Razão MEDIANA, não média: 2017 tem razão -5,63 e destruiria a média."""
    razoes = treino["delta"] / treino["fluxo"].replace(0, np.nan)
    return float(np.nanmedian(razoes) * fluxo)


def _ols(treino: pd.DataFrame, fluxo: float) -> float:
    import statsmodels.api as sm

    X = sm.add_constant(treino[["fluxo"]], has_constant="add")
    modelo = sm.OLS(treino["delta"], X).fit()
    return float(modelo.predict([[1.0, fluxo]])[0])


ESTIMADORES = {
    "ingênuo (sem mudança)": _ingenuo,
    "identidade (delta = fluxo)": _identidade,
    "razão mediana": _razao,
    "regressão OLS": _ols,
}


def retrospectiva(df: pd.DataFrame, primeiro_alvo: int = 2014) -> pd.DataFrame:
    """
    Para cada ano-alvo, ajusta só com os anos ANTERIORES e prevê.

    Janela expansiva, nunca olhando para frente. É a diferença entre "o modelo
    descreve o passado" e "o modelo teria funcionado".
    """
    linhas = []
    for nome, fn in ESTIMADORES.items():
        erros, erros_pct = [], []
        for _, alvo in df[df["ano"] >= primeiro_alvo].iterrows():
            treino = df[df["ano"] < alvo["ano"]]
            if len(treino) < 5:
                continue
            previsto = alvo["estoque_anterior"] + fn(treino, alvo["fluxo"])
            erro = previsto - alvo["estoque"]
            erros.append(abs(erro))
            erros_pct.append(abs(erro) / alvo["estoque"] * 100)
        if erros:
            linhas.append({
                "estimador": nome,
                "anos": len(erros),
                "erro_medio": float(np.mean(erros)),
                "erro_pct": float(np.mean(erros_pct)),
            })
    return pd.DataFrame(linhas).sort_values("erro_pct").reset_index(drop=True)


def detalhe_ano(df: pd.DataFrame, ano_teste: int) -> pd.DataFrame:
    """Previsão de cada estimador para um ano retido, treinando só com o passado."""
    treino = df[df["ano"] < ano_teste]
    alvo = df[df["ano"] == ano_teste]
    if treino.empty or alvo.empty:
        return pd.DataFrame()
    alvo = alvo.iloc[0]

    linhas = []
    for nome, fn in ESTIMADORES.items():
        previsto = alvo["estoque_anterior"] + fn(treino, alvo["fluxo"])
        erro = previsto - alvo["estoque"]
        linhas.append({
            "estimador": nome,
            "previsto": previsto,
            "real": alvo["estoque"],
            "erro": erro,
            "erro_pct": erro / alvo["estoque"] * 100,
        })
    return pd.DataFrame(linhas).reindex(
        pd.DataFrame(linhas)["erro_pct"].abs().sort_values().index).reset_index(drop=True)


def intervalo_ols(treino: pd.DataFrame, fluxo: float, estoque_anterior: float,
                  alpha: float = 0.20):
    """Intervalo de PREVISÃO (não de confiança da média): inclui o erro do novo ponto."""
    import statsmodels.api as sm

    X = sm.add_constant(treino[["fluxo"]], has_constant="add")
    modelo = sm.OLS(treino["delta"], X).fit()
    pred = modelo.get_prediction([[1.0, fluxo]])
    faixa = pred.conf_int(obs=True, alpha=alpha)[0]
    return (estoque_anterior + faixa[0], estoque_anterior + faixa[1],
            modelo.rsquared, modelo.params.iloc[1], modelo.pvalues.iloc[1])


def projetar_ano_corrente(df: pd.DataFrame, con=None) -> dict:
    """
    Estima o estoque do ano que a RAIS ainda não publicou.

    É o pagamento de todo o exercício: a RAIS de 2026 só sai em 2027, e aqui
    ela é estimada com o CAGED que já existe.

    O ano corrente tem só parte dos meses, então o saldo que falta vem do
    SARIMA de `previsao_saldo`. Isso EMPILHA duas incertezas — a do fluxo
    previsto e a da relação fluxo→estoque — e a faixa reportada soma as duas
    em quadratura. É aproximação: assume que os dois erros são independentes,
    o que é razoável (um é sazonalidade do mercado, outro é diferença de
    universo entre as bases) mas não é demonstrado. A faixa deve ser lida como
    ordem de grandeza da incerteza, não como intervalo exato.
    """
    from ciencia_dados import previsao_saldo as ps

    con = con or conectar_duckdb()
    mensal = sr.carregar(con)
    por_ano = mensal.groupby(mensal.index.year)["saldo"].agg(["sum", "size"])
    parciais = por_ano[por_ano["size"] < 12]
    if parciais.empty:
        return {}

    ano = int(parciais.index[-1])
    observado = float(parciais.iloc[-1]["sum"])
    faltam = 12 - int(parciais.iloc[-1]["size"])

    y = mensal["saldo"].dropna()
    ordem, sazonal = ps.CANDIDATOS[0]
    previsao, _ = ps.prever(y, ordem, sazonal, h=faltam)
    fluxo_previsto = float(previsao["previsao"].sum())
    erro_fluxo = float((previsao["superior"] - previsao["previsao"]).sum())

    fluxo_total = observado + fluxo_previsto
    ultimo = df.iloc[-1]
    # A identidade vence a retrospectiva, então é ela que projeta. Usar a
    # regressão aqui seria escolher o estimador pior por parecer mais técnico.
    estoque = float(ultimo["estoque"]) + fluxo_total

    treino = df
    _, sup_ols, *_ = intervalo_ols(treino, fluxo_total, float(ultimo["estoque"]))
    erro_relacao = sup_ols - (float(ultimo["estoque"]) + _ols(treino, fluxo_total))
    margem = float(np.hypot(erro_fluxo, erro_relacao))

    return {
        "ano": ano,
        "meses_observados": 12 - faltam,
        "fluxo_observado": observado,
        "fluxo_previsto": fluxo_previsto,
        "fluxo_total": fluxo_total,
        "estoque_base": float(ultimo["estoque"]),
        "ano_base": int(ultimo["ano"]),
        "estoque_estimado": estoque,
        "margem": margem,
    }


def main() -> int:
    p = argparse.ArgumentParser(description="Nowcast do estoque da RAIS pelo CAGED.")
    p.add_argument("--ano-teste", type=int, default=None,
                   help="Ano retido para validação. Padrão: o último disponível.")
    p.add_argument("--sem-projecao", action="store_true",
                   help="Só valida; não projeta o ano que a RAIS ainda não publicou.")
    args = p.parse_args()

    print("=" * 74)
    print("  NOWCAST DO ESTOQUE DA RAIS A PARTIR DO FLUXO DO CAGED")
    print("=" * 74)

    df = montar()
    incompletos = df.attrs.get("incompletos", [])
    ano_teste = args.ano_teste or int(df["ano"].max())

    print(f"\n📊 {len(df)} pares ano a ano, de {int(df['ano'].min())} a {int(df['ano'].max())}")
    if incompletos:
        print(f"   descartados por CAGED incompleto: {incompletos} "
              f"(saldo parcial contra estoque de dezembro não se compara)")
    corr = df["delta"].corr(df["fluxo"])
    print(f"   correlação entre variação do estoque e saldo do CAGED: {corr:.3f}")

    print(f"\n🧪 Retrospectiva ano a ano (treina só com o passado de cada alvo)")
    retro = retrospectiva(df)
    print(retro.to_string(index=False, float_format=lambda v: f"{v:,.1f}"))

    print(f"\n🎯 Ano retido: {ano_teste} — treinado apenas com {int(df['ano'].min())}"
          f"–{ano_teste - 1}")
    det = detalhe_ano(df, ano_teste)
    if det.empty:
        print("   sem dados para esse ano.")
        return 1
    print(det.to_string(index=False, float_format=lambda v: f"{v:,.1f}"))

    treino = df[df["ano"] < ano_teste]
    alvo = df[df["ano"] == ano_teste].iloc[0]
    inf, sup, r2, beta, pvalor = intervalo_ols(
        treino, alvo["fluxo"], alvo["estoque_anterior"])
    dentro = inf <= alvo["estoque"] <= sup
    print(f"\n   Intervalo de previsão de 80% (OLS): "
          f"{inf:,.0f} a {sup:,.0f}")
    print(f"   Valor real: {alvo['estoque']:,.0f}  ->  "
          f"{'DENTRO do intervalo ✅' if dentro else 'FORA do intervalo ❌'}")
    print(f"   R² do treino {r2:.3f} · coeficiente do fluxo {beta:.2f} "
          f"(p={pvalor:.4f})")

    melhor = det.iloc[0]
    print(f"\n💡 Melhor no ano retido: {melhor['estimador']} — erro de "
          f"{melhor['erro']:+,.0f} vínculos ({melhor['erro_pct']:+.2f}%)")

    vencedor_retro = retro.iloc[0]["estimador"]
    if vencedor_retro != melhor["estimador"]:
        print(f"   Atenção: na retrospectiva quem vence é '{vencedor_retro}'. "
              f"Um ano só não decide —")
        print(f"   a sequência de anos é a evidência mais forte das duas.")

    if args.sem_projecao:
        return 0

    proj = projetar_ano_corrente(df)
    if not proj:
        print("\n(sem ano parcial no CAGED — nada a projetar)")
        return 0

    print(f"\n{'=' * 74}")
    print(f"  PROJEÇÃO: o estoque de {proj['ano']}, que a RAIS ainda não publicou")
    print(f"{'=' * 74}")
    print(f"   CAGED observado ({proj['meses_observados']} meses): "
          f"{proj['fluxo_observado']:+,.0f}")
    print(f"   CAGED previsto (SARIMA, {12 - proj['meses_observados']} meses): "
          f"{proj['fluxo_previsto']:+,.0f}")
    print(f"   fluxo total estimado do ano: {proj['fluxo_total']:+,.0f}")
    print(f"\n   estoque de {proj['ano_base']} (real): {proj['estoque_base']:,.0f}")
    print(f"   estoque de {proj['ano']} (estimado): "
          f"{proj['estoque_estimado']:,.0f} ± {proj['margem']:,.0f}")
    variacao = (proj["estoque_estimado"] / proj["estoque_base"] - 1) * 100
    print(f"   variação implícita: {variacao:+.1f}%")
    print(f"\n   A margem empilha a incerteza do fluxo previsto com a da relação")
    print(f"   fluxo→estoque. É ordem de grandeza, não intervalo exato.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
