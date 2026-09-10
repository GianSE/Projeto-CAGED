"""
Decomposição de Oaxaca-Blinder do hiato salarial em tecnologia.

A PERGUNTA
----------
Mulheres ganham menos que homens em TI. Pretos e pardos ganham menos que
brancos. Isso o cruzamento simples já mostra. A pergunta que ele NÃO responde
é a que importa: quanto dessa diferença vem de as pessoas terem características
diferentes — escolaridade, tempo de casa, ocupação, jornada, porte da empresa —
e quanto sobra depois de comparar iguais com iguais.

O MÉTODO
--------
Ajusta-se uma equação de salário para cada grupo e separa-se a diferença das
médias em duas partes:

    hiato = (X̄a - X̄b)'β*        <- EXPLICADA: diferença de características
          + X̄a'(βa - β*) + X̄b'(β* - βb)   <- NÃO EXPLICADA: diferença de retorno

β* é a estrutura de uma regressão conjunta, usada como referência — a proposta
de Neumark (1988). A alternativa de usar o β de um dos grupos como referência
embute a escolha de qual estrutura é "a correta", e essa escolha muda o
resultado.

A parte explicada responde "se as mulheres tivessem as mesmas características
médias dos homens, quanto do hiato sumiria". A não explicada é o que resta:
mesma escolaridade, mesmo tempo de casa, mesma ocupação, mesma jornada — e
ainda assim salários diferentes.

O QUE A PARTE NÃO EXPLICADA NÃO É
---------------------------------
Não é medida de discriminação, e chamá-la assim é o erro mais comum de quem
usa este método. Ela contém TUDO que afeta salário e não está no modelo:
experiência anterior, interrupções de carreira, senioridade dentro do cargo,
negociação, empresa específica. Discriminação está lá dentro, mas não sozinha,
e o dado não permite separá-la do resto.

O que se pode afirmar com honestidade é o limite superior: a diferença de
retorno é NO MÁXIMO isto. E também o piso do que a composição explica.

A ESCOLHA DO SALÁRIO
--------------------
O logaritmo da remuneração em salários mínimos. Log porque a distribuição
salarial é assimétrica e porque assim os coeficientes se leem como variação
percentual. Em salários mínimos porque o hiato é calculado dentro de um ano —
o deflator não muda nada aqui, mas mantém a medida comparável se alguém rodar
outro ano.

Uso:
    python -m ciencia_dados.hiato_salarial
    python -m ciencia_dados.hiato_salarial --ano 2023 --dimensao raca
"""
import argparse
import sys
import warnings

import numpy as np
import pandas as pd

from extracao_ftp.config_extracao import BUCKET_SILVER_TI, conectar_duckdb
from gold_caged import escopo_tecnologia as esc

warnings.simplefilter("ignore")

# Só o estoque de 31/12: misturar quem saiu no meio do ano traria salário de
# período parcial e pessoas que não estavam no mercado no mesmo momento.
ATIVO = "vinculo_ativo_3112 = '1'"

# Teto de plausibilidade — a fonte inverte a conversão para salário mínimo em
# alguns registros a partir de 2023 (ver gold_rais.construir_gold).
TETO_SM = 500

COVARIAVEIS = ["escolaridade", "area", "porte", "regiao", "setor"]
NUMERICAS = ["tempo_emprego", "idade", "idade2", "horas"]


def _sql(ano: int) -> str:
    return f"""
        SELECT
            vl_remun_media_sm                 AS remuneracao_sm,
            sexo_trabalhador_descricao        AS sexo,
            raca_cor_descricao                AS raca,
            escolaridade_apos_2005_descricao  AS escolaridade,
            tamanho_estabelecimento_descricao AS porte,
            tempo_emprego,
            idade,
            qtd_hora_contr                    AS horas,
            {esc.sql_area_ti('cbo_ocupacao_2002')} AS area,
            CASE WHEN {esc.sql_filtro_cnae('cnae_20_subclasse')}
                 THEN 'Empresa de TI' ELSE 'Fora do setor de TI' END AS setor,
            upper(split_part(municipio_descricao, '-', 1)) AS regiao
        FROM read_parquet('s3://{BUCKET_SILVER_TI}/rais_vinc/ano_particao={ano}/**/*.parquet')
        WHERE {ATIVO}
          AND {esc.sql_filtro_cbo('cbo_ocupacao_2002')}
          AND vl_remun_media_sm > 0 AND vl_remun_media_sm <= {TETO_SM}
          AND tempo_emprego IS NOT NULL AND idade IS NOT NULL
          AND qtd_hora_contr > 0
          AND municipio_descricao IS NOT NULL
    """


def carregar(ano: int, con=None) -> pd.DataFrame:
    con = con or conectar_duckdb()
    con.execute("SET enable_progress_bar=false")
    con.execute("SET threads=4")
    df = con.execute(_sql(ano)).df()
    df["ln_salario"] = np.log(df["remuneracao_sm"])
    # Idade ao quadrado: o retorno da experiência é côncavo — cresce e depois
    # achata. Sem o termo quadrático o modelo forçaria uma reta e jogaria a
    # curvatura para dentro da parte não explicada.
    df["idade2"] = df["idade"] ** 2
    return df.dropna(subset=["ln_salario"] + NUMERICAS + COVARIAVEIS)


def _desenho(df: pd.DataFrame, colunas: list[str]) -> pd.DataFrame:
    """Matriz de covariáveis alinhada entre os grupos."""
    X = pd.get_dummies(df[COVARIAVEIS].astype(str), drop_first=True, dtype=float)
    for c in NUMERICAS:
        X[c] = df[c].astype(float)
    X = X.reindex(columns=colunas, fill_value=0.0)
    X.insert(0, "const", 1.0)
    return X


def decompor(df: pd.DataFrame, dimensao: str, grupo_a: str, grupo_b: str) -> dict:
    """
    Decomposição de duas partes com referência conjunta (Neumark).

    `grupo_a` é o de MAIOR salário médio por convenção, para que o hiato saia
    positivo e a leitura não dependa da ordem dos argumentos.
    """
    import statsmodels.api as sm

    a = df[df[dimensao] == grupo_a]
    b = df[df[dimensao] == grupo_b]
    if len(a) < 500 or len(b) < 500:
        return {}

    # Colunas do desenho vindas dos DOIS grupos: se uma categoria só existe em
    # um deles, ela precisa existir (com zero) no outro, senão os vetores não
    # se subtraem.
    base = pd.get_dummies(df[COVARIAVEIS].astype(str), drop_first=True, dtype=float)
    colunas = list(base.columns) + NUMERICAS

    Xa, Xb = _desenho(a, colunas), _desenho(b, colunas)
    Xp = _desenho(df[df[dimensao].isin([grupo_a, grupo_b])], colunas)
    ya = a["ln_salario"].to_numpy()
    yb = b["ln_salario"].to_numpy()
    yp = df[df[dimensao].isin([grupo_a, grupo_b])]["ln_salario"].to_numpy()

    ma = sm.OLS(ya, Xa.to_numpy()).fit()
    mb = sm.OLS(yb, Xb.to_numpy()).fit()
    mp = sm.OLS(yp, Xp.to_numpy()).fit()

    media_a, media_b = Xa.to_numpy().mean(axis=0), Xb.to_numpy().mean(axis=0)
    hiato = float(ya.mean() - yb.mean())

    explicada = float((media_a - media_b) @ mp.params)
    nao_explicada = float(media_a @ (ma.params - mp.params)
                          + media_b @ (mp.params - mb.params))

    # Conferência: as duas partes têm de somar o hiato. Se não somarem, o
    # desenho está desalinhado entre os grupos e o resultado é lixo — melhor
    # descobrir aqui que num gráfico.
    residuo = hiato - (explicada + nao_explicada)

    # Contribuição de cada bloco de variáveis para a parte explicada, que é o
    # que responde "o que exatamente explica".
    detalhe = {}
    for bloco in COVARIAVEIS + ["tempo_emprego", "idade", "horas"]:
        if bloco in NUMERICAS:
            idx = [Xa.columns.get_loc(bloco)]
            if bloco == "idade":
                idx.append(Xa.columns.get_loc("idade2"))
        else:
            idx = [i for i, c in enumerate(Xa.columns) if c.startswith(bloco + "_")]
        if idx:
            detalhe[bloco] = float((media_a[idx] - media_b[idx]) @ mp.params[idx])

    return {
        "dimensao": dimensao,
        "grupo_a": grupo_a, "n_a": len(a), "salario_a": float(np.exp(ya.mean())),
        "grupo_b": grupo_b, "n_b": len(b), "salario_b": float(np.exp(yb.mean())),
        "hiato_log": hiato,
        "hiato_pct": (np.exp(hiato) - 1) * 100,
        "explicada": explicada,
        "nao_explicada": nao_explicada,
        "residuo": residuo,
        "r2_a": ma.rsquared, "r2_b": mb.rsquared,
        "detalhe": detalhe,
    }


def _relatar(r: dict):
    if not r:
        print("   grupos pequenos demais para decompor.")
        return
    print(f"\n   {r['grupo_a']} ({r['n_a']:,}): {r['salario_a']:.2f} SM na média geométrica")
    print(f"   {r['grupo_b']} ({r['n_b']:,}): {r['salario_b']:.2f} SM")
    print(f"   hiato: {r['hiato_log']:.4f} log ({r['hiato_pct']:+.1f}%)")

    if abs(r["residuo"]) > 1e-6:
        print(f"   ⚠️  resíduo da decomposição {r['residuo']:.2e} — deveria ser zero")

    pct_exp = r["explicada"] / r["hiato_log"] * 100 if r["hiato_log"] else 0
    print(f"\n   EXPLICADA pelas características : {r['explicada']:.4f} "
          f"({pct_exp:.0f}% do hiato)")
    print(f"   NÃO EXPLICADA                   : {r['nao_explicada']:.4f} "
          f"({100 - pct_exp:.0f}% do hiato)")

    print(f"\n   O que compõe a parte explicada:")
    for bloco, valor in sorted(r["detalhe"].items(), key=lambda kv: -abs(kv[1])):
        share = valor / r["hiato_log"] * 100 if r["hiato_log"] else 0
        print(f"      {bloco:<16} {valor:>8.4f}  ({share:>5.1f}% do hiato)")

    print(f"\n   R² das equações: {r['r2_a']:.3f} e {r['r2_b']:.3f}")


def serie(anos: range, con=None) -> pd.DataFrame:
    """
    A decomposição repetida ano a ano.

    É o que responde a pergunta que um ano só não responde: a parte NÃO
    EXPLICADA está encolhendo? Se ela cai ao longo de 19 anos, a diferença de
    retorno está diminuindo mesmo que o hiato bruto não mude — e o contrário
    também: hiato bruto estável pode esconder composição melhorando e retorno
    piorando ao mesmo tempo.

    Cada ano é ajustado do zero, com seus próprios coeficientes. Impor uma
    estrutura comum a 2007 e 2025 suporia que o retorno da escolaridade não
    mudou em duas décadas, que é justamente uma das coisas em teste.
    """
    con = con or conectar_duckdb()
    linhas = []
    for ano in anos:
        try:
            df = carregar(ano, con)
        except Exception as e:
            print(f"   ⚠️  {ano}: {str(e)[:70]}")
            continue
        for dimensao, a, b in (("sexo", "MASCULINO", "FEMININO"),
                               ("raca", "BRANCA", "PARDA"),
                               ("raca", "BRANCA", "PRETA")):
            r = decompor(df, dimensao, a, b)
            if not r:
                continue
            linhas.append({
                "ano": ano, "dimensao": dimensao,
                "comparacao": f"{a} vs {b}",
                "n_a": r["n_a"], "n_b": r["n_b"],
                "salario_a": r["salario_a"], "salario_b": r["salario_b"],
                "hiato_log": r["hiato_log"], "hiato_pct": r["hiato_pct"],
                "explicada": r["explicada"],
                "nao_explicada": r["nao_explicada"],
                # A participação só significa algo quando há hiato a repartir.
                # Em 2011-2015 o hiato bruto de sexo passa perto de zero e a
                # razão dispara para 4307% — número correto e inútil, que num
                # gráfico viraria um pico sem sentido. Abaixo de 0,02 log
                # (2% de diferença salarial) a divisão é suprimida.
                "explicada_share": (r["explicada"] / r["hiato_log"] * 100
                                    if abs(r["hiato_log"]) >= 0.02 else np.nan),
            })
        print(f"   ✅ {ano}: {len(df):,} vínculos")
    return pd.DataFrame(linhas)


def main() -> int:
    p = argparse.ArgumentParser(description="Decompõe o hiato salarial em TI.")
    p.add_argument("--ano", type=int, default=2024)
    p.add_argument("--dimensao", choices=("sexo", "raca", "ambos"), default="ambos")
    p.add_argument("--serie", action="store_true",
                   help="Roda a decomposição para toda a série e grava na gold.")
    p.add_argument("--ano-inicio", type=int, default=2007)
    p.add_argument("--ano-fim", type=int, default=2025)
    args = p.parse_args()

    if args.serie:
        from extracao_ftp.config_extracao import (
            BUCKET_GOLD, PARQUET_COMPRESSION, PARQUET_COMPRESSION_LEVEL)

        print("=" * 78)
        print("  HIATO SALARIAL ANO A ANO — OAXACA-BLINDER")
        print("=" * 78)
        con = conectar_duckdb()
        con.execute("SET enable_progress_bar=false")
        con.execute("SET threads=4")
        tabela = serie(range(args.ano_inicio, args.ano_fim + 1), con)
        if tabela.empty:
            print("\n❌ nada decomposto.")
            return 1
        con.register("hiato", tabela)
        destino = f"s3://{BUCKET_GOLD}/hiato_serie.parquet"
        con.execute(f"""
            COPY (SELECT * FROM hiato) TO '{destino}' (
                FORMAT PARQUET,
                COMPRESSION '{PARQUET_COMPRESSION.upper()}',
                COMPRESSION_LEVEL {PARQUET_COMPRESSION_LEVEL})
        """)
        print(f"\n📈 Evolução (parte não explicada, em log)")
        for comp, sub in tabela.groupby("comparacao"):
            sub = sub.sort_values("ano")
            print(f"\n   {comp}")
            print(f"      {'ano':<6}{'hiato':>9}{'explicada':>12}{'não expl.':>12}"
                  f"{'% expl.':>10}")
            for _, r in sub.iterrows():
                print(f"      {int(r['ano']):<6}{r['hiato_log']:>9.4f}"
                      f"{r['explicada']:>12.4f}{r['nao_explicada']:>12.4f}"
                      f"{r['explicada_share']:>9.0f}%")
        print(f"\n🏁 {len(tabela)} decomposições em {destino}")
        return 0

    print("=" * 78)
    print(f"  HIATO SALARIAL EM TECNOLOGIA — OAXACA-BLINDER, RAIS {args.ano}")
    print("=" * 78)

    df = carregar(args.ano)
    print(f"\n📊 {len(df):,} vínculos ativos com ocupação de TI e salário válido")

    pares = []
    if args.dimensao in ("sexo", "ambos"):
        pares.append(("sexo", "MASCULINO", "FEMININO"))
    if args.dimensao in ("raca", "ambos"):
        # Branca contra parda/preta, os dois maiores grupos não brancos.
        for outro in ("PARDA", "PRETA"):
            if (df["raca"] == outro).sum() >= 500:
                pares.append(("raca", "BRANCA", outro))

    for dimensao, a, b in pares:
        print(f"\n{'-' * 78}")
        print(f"  {dimensao.upper()}: {a} vs {b}")
        print(f"{'-' * 78}")
        _relatar(decompor(df, dimensao, a, b))

    print(f"\n{'=' * 78}")
    print("  A parte NÃO EXPLICADA não é medida de discriminação. Ela contém")
    print("  tudo que afeta salário e não está no modelo — experiência anterior,")
    print("  interrupções de carreira, senioridade no cargo, empresa específica.")
    print("  O que se afirma com honestidade é o LIMITE SUPERIOR da diferença")
    print("  de retorno, não a sua causa.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
