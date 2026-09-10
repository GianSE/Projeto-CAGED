"""
Quanto dura um emprego de tecnologia — análise de sobrevivência sobre a RAIS.

POR QUE ISTO SÓ DÁ COM A RAIS
-----------------------------
O CAGED registra a movimentação, não a duração: sabe-se que houve admissão e
que houve desligamento, mas não se são da mesma pessoa no mesmo vínculo. A
RAIS traz `tempo_emprego` — quanto o vínculo já durou — e diz quem seguia
ativo em 31/12. Isso é exatamente dado CENSURADO À DIREITA, o formato para o
qual a análise de sobrevivência foi inventada:

    desligado no ano  -> observou-se a duração completa   (evento)
    ativo em 31/12    -> a duração é ao menos esta        (censura)

Descartar os censurados e olhar só quem saiu daria a resposta errada e para
menos: quem está há dez anos no emprego nunca apareceria na conta, e a
"duração típica" cairia para a dos vínculos curtos.

TRANSFERÊNCIA NÃO É FIM DE VÍNCULO
----------------------------------
Os motivos 3x — transferência com e sem ônus, redistribuição — significam que
a pessoa continua empregada, em outro estabelecimento da mesma organização.
São 87 mil casos em 2024, 17% de todos os "desligamentos". Contá-los como fim
inflaria a rotatividade em quase um quinto. Aqui eles entram como CENSURA: o
que se sabe é que o vínculo durou ao menos aquilo, e o que veio depois saiu do
campo de visão.

Falecimento (motivo 60) também é censurado. É um fim, mas não uma decisão do
mercado de trabalho, e misturá-lo com demissão responderia outra pergunta.

A INTERPRETAÇÃO CORRETA: TÁBUA DE PERÍODO
-----------------------------------------
Um ano de RAIS é uma FOTOGRAFIA, não o acompanhamento de uma turma ao longo do
tempo. A curva daqui é uma tábua de período — o mesmo que um demógrafo faz ao
estimar expectativa de vida com os óbitos de um ano só. Ela responde "como
seria a trajetória de um vínculo se as taxas de hoje valessem para sempre",
não "o que aconteceu com quem entrou em 2015".

A hipótese embutida é que o risco de desligamento por tempo de casa é estável
no tempo. É razoável e é padrão na literatura de mercado de trabalho, mas é
hipótese, e um trabalho que use estes números precisa dizê-la.

Uso:
    python -m ciencia_dados.sobrevivencia
    python -m ciencia_dados.sobrevivencia --ano 2023 --amostra-cox 100000

A opção mantém o nome --amostra-cox por compatibilidade, mas o modelo passou a
ser de risco em tempo discreto (ver risco_multivariado).
"""
import argparse
import sys
import warnings

import numpy as np
import pandas as pd

from extracao_ftp.config_extracao import BUCKET_SILVER_TI, conectar_duckdb
from gold_caged import escopo_tecnologia as esc

warnings.simplefilter("ignore")

# Motivos em que o vínculo NÃO terminou de fato (ou terminou por fora do
# mercado de trabalho). Entram como censura.
MOTIVOS_CENSURA = ("30", "31", "34", "35", "36", "40", "60")


def _sql(ano: int, amostra: int | None) -> str:
    censura = ", ".join(f"'{m}'" for m in MOTIVOS_CENSURA)
    limite = f"USING SAMPLE {amostra} ROWS" if amostra else ""
    return f"""
        SELECT
            tempo_emprego AS duracao,
            -- Tempo de casa no INÍCIO do ano, que é o que define a faixa de
            -- risco. Para quem seguia ativo, é a duração menos os 12 meses do
            -- ano; para quem saiu, é a duração menos os meses que trabalhou
            -- antes de sair. Negativo significa admitido durante o ano.
            CASE
                WHEN vinculo_ativo_3112 = '1' THEN tempo_emprego - 12
                ELSE tempo_emprego - TRY_CAST(mes_desligamento AS INTEGER)
            END AS tempo_inicio,
            CASE
                WHEN vinculo_ativo_3112 = '1' THEN 0
                WHEN motivo_desligamento IN ({censura}) THEN 0
                ELSE 1
            END AS evento,
            sexo_trabalhador_descricao        AS sexo,
            raca_cor_descricao                AS raca,
            escolaridade_apos_2005_descricao  AS escolaridade,
            tamanho_estabelecimento_descricao AS porte,
            idade,
            vl_remun_media_sm                 AS remuneracao_sm,
            {esc.sql_area_ti('cbo_ocupacao_2002')} AS area,
            CASE WHEN {esc.sql_filtro_cnae('cnae_20_subclasse')}
                 THEN 'Empresa de TI' ELSE 'Fora do setor de TI' END AS setor
        FROM read_parquet('s3://{BUCKET_SILVER_TI}/rais_vinc/ano_particao={ano}/**/*.parquet')
        WHERE tempo_emprego IS NOT NULL AND tempo_emprego >= 0
          AND {esc.sql_filtro_cbo('cbo_ocupacao_2002')}
        {limite}
    """


def carregar(ano: int, amostra: int | None = None, con=None) -> pd.DataFrame:
    """
    Vínculos com OCUPAÇÃO de TI — a lente certa para esta pergunta.

    "Quanto dura um emprego de tecnologia" é sobre a carreira da pessoa, não
    sobre o setor da empresa. A recepcionista da software house não responde a
    pergunta; o desenvolvedor do banco responde.
    """
    con = con or conectar_duckdb()
    con.execute("SET enable_progress_bar=false")
    con.execute("SET threads=4")
    return con.execute(_sql(ano, amostra)).df()


# Faixas de tempo de casa, em anos completos no início do ano. O primeiro ano
# é partido em semestres porque é onde o risco se concentra e onde a média
# anual esconderia mais do que mostra.
# A última faixa é FECHADA de propósito. Aberta (120 a ∞) ela seria projetada
# por séculos e a sobrevivência acumulada zeraria — foi o que aconteceu na
# primeira execução, com a coluna final saindo 0,0 para todo grupo. Uma tábua
# só pode acumular sobre faixas de largura conhecida.
FAIXAS = [(0, 6), (6, 12), (12, 24), (24, 36), (36, 60), (60, 120), (120, 240)]
ROTULOS = ["0-6m", "6-12m", "1-2a", "2-3a", "3-5a", "5-10a", "10-20a"]


def tabua_periodo(df: pd.DataFrame, grupo: str | None = None) -> pd.DataFrame:
    """
    Tábua de período: risco anual de desligamento por tempo de casa.

    POR QUE NÃO KAPLAN-MEIER DIRETO
    -------------------------------
    O KM sobre um corte transversal supõe que quem tem 100 meses de casa
    estava em risco de se desligar aos 12 — mas essa pessoa passou pelos 12
    meses anos atrás, fora da janela observada. O conjunto de risco fica
    inflado, o risco sai subestimado e a mediana explode: a primeira versão
    deste módulo devolvia 570 meses, quase 48 anos, o que não é crível para
    duração de emprego.

    A construção correta usa a janela de UM ANO: para cada faixa de tempo de
    casa no início do ano, quantos se desligaram durante o ano dividido por
    quantos estavam ali. Depois a sobrevivência acumulada é o produto dos
    complementos, como numa tábua de vida.

    APROXIMAÇÃO ASSUMIDA
    --------------------
    Quem foi admitido durante o ano teve menos de doze meses de exposição, e
    aqui entra na faixa 0-6m com peso cheio. Isso SUPERESTIMA o risco da
    primeira faixa. Corrigir exigiria exposição em pessoa-mês, que a RAIS
    anual não dá diretamente. A distorção é conhecida, é de uma faixa só, e
    empurra na direção conservadora — o vínculo novo aparece mais frágil do
    que é, não mais estável.
    """
    def _uma(sub: pd.DataFrame, rotulo: str) -> dict:
        # Tempo de casa no início do ano; negativo = admitido no próprio ano.
        t0 = sub["tempo_inicio"].fillna(0).clip(lower=0)
        linha = {"grupo": rotulo, "n": len(sub), "eventos": int(sub["evento"].sum())}
        sobrevivencia = 1.0
        acumulada = {}
        for (ini, fim), nome in zip(FAIXAS, ROTULOS):
            faixa = sub[(t0 >= ini) & (t0 < fim)]
            if len(faixa) < 30:
                acumulada[nome] = np.nan
                continue
            risco = float(faixa["evento"].mean())
            # Anualiza a faixa: uma faixa de 6 meses vê metade do risco anual,
            # uma de 5 anos vê o risco anual repetido cinco vezes.
            anos = (fim - ini) / 12
            sobrevivencia *= (1 - risco) ** anos if anos > 1 else (1 - risco * anos)
            acumulada[nome] = sobrevivencia * 100
        linha.update(acumulada)
        # A mediana é o ponto em que a sobrevivência acumulada cruza 50%.
        cruzou = [n for n in ROTULOS
                  if not np.isnan(acumulada.get(n, np.nan)) and acumulada[n] <= 50]
        linha["mediana_em"] = cruzou[0] if cruzou else "acima de 10a"
        return linha

    if grupo is None:
        return pd.DataFrame([_uma(df, "todos")])
    linhas = [_uma(sub, str(v)) for v, sub in df.groupby(grupo, dropna=True)
              if len(sub) >= 500]
    tabela = pd.DataFrame(linhas)
    return tabela.sort_values("5-10a", ascending=False).reset_index(drop=True)


def risco_por_faixa(df: pd.DataFrame) -> pd.DataFrame:
    """Risco anual bruto em cada faixa — o insumo da tábua, exposto."""
    t0 = df["tempo_inicio"].fillna(0).clip(lower=0)
    linhas = []
    for (ini, fim), nome in zip(FAIXAS, ROTULOS):
        faixa = df[(t0 >= ini) & (t0 < fim)]
        if len(faixa) < 30:
            continue
        linhas.append({
            "tempo_de_casa": nome,
            "vinculos": len(faixa),
            "desligamentos": int(faixa["evento"].sum()),
            "risco_%": float(faixa["evento"].mean()) * 100,
        })
    return pd.DataFrame(linhas)


def risco_multivariado(df: pd.DataFrame) -> pd.DataFrame:
    """
    O que muda o risco de desligamento, controlando pelo resto.

    POR QUE NÃO COX
    ---------------
    O Cox monta o conjunto de risco pela DURAÇÃO observada, e num corte
    transversal isso sofre exatamente o viés que `tabua_periodo` corrige:
    quem tem dez anos de casa entra no conjunto de risco dos doze meses, onde
    nunca esteve durante o ano observado. Consertar a curva e deixar o
    multivariado com o mesmo defeito seria incoerente.

    O modelo aqui é de risco em TEMPO DISCRETO: a unidade é "este vínculo, em
    2024", a resposta é "desligou-se ou não", e o tempo de casa no início do
    ano entra como variável. É a formulação padrão quando a observação vem em
    janelas anuais — e faz o que o Cox faria, sem a suposição que a fonte não
    sustenta.

    A leitura é razão de chances (odds ratio), que aqui aproxima bem a razão
    de risco porque o evento é relativamente raro: 1,20 é 20% mais chance de
    desligamento no ano, contra a categoria de referência, com as demais
    variáveis fixas.
    """
    import statsmodels.api as sm

    d = df.dropna(subset=["evento", "sexo", "escolaridade", "idade",
                          "remuneracao_sm", "area", "porte"]).copy()
    # Remuneração implausível (ver gold_rais: conversão invertida na fonte a
    # partir de 2023) distorceria o coeficiente inteiro.
    d = d[(d["remuneracao_sm"] > 0) & (d["remuneracao_sm"] <= 500)]
    if d.empty:
        return pd.DataFrame()

    t0 = d["tempo_inicio"].fillna(0).clip(lower=0)
    d["tempo_de_casa"] = pd.cut(t0, bins=[b[0] for b in FAIXAS] + [FAIXAS[-1][1]],
                                labels=ROTULOS, right=False, include_lowest=True)
    # log da remuneração: o efeito de mais um salário mínimo não é o mesmo
    # para quem ganha 2 e para quem ganha 20.
    d["log_remun"] = np.log(d["remuneracao_sm"])

    desenho = pd.get_dummies(
        d[["sexo", "escolaridade", "area", "porte", "tempo_de_casa"]].astype(str),
        drop_first=True, dtype=float)
    desenho["idade"] = d["idade"].astype(float)
    desenho["log_remun"] = d["log_remun"].astype(float)
    desenho = sm.add_constant(desenho, has_constant="add")

    modelo = sm.Logit(d["evento"].astype(float).to_numpy(),
                      desenho.to_numpy()).fit(disp=False)
    saida = pd.DataFrame({
        "variavel": desenho.columns,
        "razao_de_chances": np.exp(modelo.params),
        "p": modelo.pvalues,
    })
    saida = saida[saida["variavel"] != "const"]
    return saida.reindex(
        (saida["razao_de_chances"] - 1).abs().sort_values(ascending=False).index
    ).reset_index(drop=True)


def main() -> int:
    p = argparse.ArgumentParser(description="Quanto dura um emprego de TI.")
    p.add_argument("--ano", type=int, default=2024)
    p.add_argument("--amostra-cox", type=int, default=150_000,
                   help="O multivariado sobre 1,2 milhão de linhas leva muito "
                        "tempo e não muda a conclusão; a amostra é aleatória.")
    args = p.parse_args()

    print("=" * 78)
    print(f"  QUANTO DURA UM EMPREGO DE TECNOLOGIA — RAIS {args.ano}")
    print("=" * 78)

    df = carregar(args.ano)
    censurados = int((df["evento"] == 0).sum())
    print(f"\n📊 {len(df):,} vínculos com ocupação de TI")
    print(f"   {int(df['evento'].sum()):,} desligamentos observados (evento)")
    print(f"   {censurados:,} censurados — ainda ativos, transferidos ou falecidos")
    print(f"   censura: {censurados / len(df) * 100:.1f}%")

    print("\n📉 Risco anual de desligamento por tempo de casa")
    print(risco_por_faixa(df).to_string(index=False,
                                        float_format=lambda v: f"{v:,.1f}"))

    print("\n⏳ Sobrevivência acumulada do vínculo (tábua de período, %)")
    print(tabua_periodo(df).to_string(index=False,
                                      float_format=lambda v: f"{v:,.1f}"))

    for coluna, titulo in (("sexo", "por sexo"),
                           ("area", "por área de atuação"),
                           ("setor", "dentro e fora do setor de TI"),
                           ("porte", "por porte do estabelecimento")):
        tabela = tabua_periodo(df, coluna)
        if tabela.empty:
            continue
        print(f"\n⏳ {titulo.capitalize()}")
        print(tabela.to_string(index=False, float_format=lambda v: f"{v:,.1f}"))

    print(f"\n🧮 Risco em tempo discreto — o que muda a chance de desligamento "
          f"(amostra de {args.amostra_cox:,})")
    amostra = df.sample(min(args.amostra_cox, len(df)), random_state=42)
    resultado = risco_multivariado(amostra)
    if resultado.empty:
        print("   sem dados suficientes.")
        return 1
    print(resultado.head(14).to_string(index=False,
                                       float_format=lambda v: f"{v:,.4f}"))
    print("\n   Razão de chances > 1 = mais risco de desligamento no ano; < 1 = vínculo")
    print("   mais estável. Categorias de referência omitidas pelo desenho.")
    print("\n   Leitura de período: as taxas são de um ano só (ver docstring).")
    return 0


if __name__ == "__main__":
    sys.exit(main())
