"""
Agrupamento de municípios por TRAJETÓRIA do mercado de TI.

O QUE UM RANKING NÃO RESPONDE
-----------------------------
Ordenar municípios por estoque devolve sempre a mesma lista — São Paulo, Rio,
Belo Horizonte —, que é a lista das cidades grandes. O tamanho da cidade
explica quase tudo, e o ranking acaba medindo população.

A pergunta interessante é outra: quais municípios se PARECEM entre si na forma
como o mercado de TI se comporta neles. Uma cidade média que cresce rápido e
paga bem tem mais em comum com outra cidade média assim do que com a capital
ao lado, mesmo estando a mil quilômetros dela.

AS VARIÁVEIS, E POR QUE CADA UMA
--------------------------------
    estoque (log)        tamanho, em log porque a distribuição é muito
                         assimétrica — sem log, São Paulo sozinho definiria
                         um agrupamento e o resto viraria uma nuvem só
    vínculos por mil     ESPECIALIZAÇÃO. É o que separa polo de tecnologia de
      habitantes         cidade grande qualquer, e só existe porque a
                         população vem do IBGE
    crescimento          trajetória: variação do estoque nos últimos anos
    intensidade de       fluxo do CAGED sobre o estoque da RAIS — quanto o
      fluxo              mercado se mexe em relação ao seu tamanho
    rotatividade         desligamentos sobre estoque
    remuneração          mediana em salários mínimos

As três primeiras vêm da RAIS, a quarta e a quinta do CAGED. Usar as duas
bases é o que permite distinguir "grande e parado" de "grande e fervendo".

COMO O K É ESCOLHIDO
--------------------
Por silhueta, não por chute nem por cotovelo lido a olho. A silhueta mede se
cada ponto está mais próximo do próprio grupo que do vizinho mais próximo, e
tem um número — dá para comparar candidatos em vez de discutir gráfico.

O DOMICÍLIO FISCAL, QUE NÃO DÁ PARA CONSERTAR
---------------------------------------------
A RAIS localiza o vínculo pelo ESTABELECIMENTO, e no Brasil a empresa de TI
escolhe onde registrar. O ISS sobre serviço de software varia por município, e
há cidades que o usam como política de atração. O efeito aparece cru no dado:

    Mg-Guaraciaba (10.307 habitantes): 1 vínculo de TI em 2022, 1.428 em 2023
    Sp-Barueri: 200 vínculos de TI por mil habitantes

Barueri, Jaguariúna, Eusébio e Eldorado do Sul aparecem juntos no topo da
concentração por habitante — todos conhecidos por incentivo fiscal a serviços.
Não é erro: é onde a empresa está registrada, que não é onde as pessoas
trabalham.

A RAIS tem a coluna que resolveria isso, `mun_trab`. Ela é inútil: vale
'999999' (não informado) em 1.817.418 dos 1.817.421 vínculos de 2024. Foi
conferido antes de desistir dela.

Portanto: as medidas territoriais aqui são de DOMICÍLIO DO ESTABELECIMENTO, e
qualquer leitura de "onde está o emprego de TI" precisa dizer isso. O
agrupamento exige um estoque mínimo TAMBÉM no ano-base, o que remove o caso
mais grosseiro — a cidade que salta de 1 para mil —, mas não corrige o
fenômeno, que é estrutural.

Uso:
    python -m ciencia_dados.clusters_municipios
    python -m ciencia_dados.clusters_municipios --ano 2024 --minimo 50
"""
import argparse
import sys
import warnings

import numpy as np
import pandas as pd

from extracao_ftp.config_extracao import (
    BUCKET_GOLD,
    PARQUET_COMPRESSION,
    PARQUET_COMPRESSION_LEVEL,
    conectar_duckdb,
)

warnings.simplefilter("ignore")

VARIAVEIS = ["log_estoque", "por_mil_hab", "crescimento",
             "intensidade_fluxo", "rotatividade", "remuneracao_sm_mediana"]


def montar(ano: int, janela: int, minimo: int, minimo_base: int = 10,
           con=None) -> pd.DataFrame:
    """Uma linha por município, com as variáveis da trajetória."""
    con = con or conectar_duckdb()
    con.execute("SET enable_progress_bar=false")

    mapa = f"read_parquet('s3://{BUCKET_GOLD}/mapa_municipio.parquet')"
    geo = f"read_parquet('s3://{BUCKET_GOLD}/geo_municipios.parquet')"
    base = ano - janela

    return con.execute(f"""
        WITH atual AS (
            SELECT cod_municipio, municipio, uf, estoque,
                   remuneracao_sm_mediana, saldo, desligamentos
            FROM {mapa} WHERE ano = {ano} AND cod_municipio IS NOT NULL
        ),
        anterior AS (
            SELECT cod_municipio, estoque AS estoque_antes
            FROM {mapa} WHERE ano = {base}
        ),
        fluxo AS (
            -- Fluxo acumulado da janela, não só do último ano: um ano isolado
            -- é ruidoso demais em município pequeno.
            SELECT cod_municipio, sum(saldo) AS saldo_janela
            FROM {mapa} WHERE ano > {base} AND ano <= {ano}
            GROUP BY 1
        )
        SELECT a.cod_municipio, a.municipio, a.uf,
               a.estoque,
               coalesce(b.estoque_antes, 0) AS estoque_antes,
               g.latitude, g.longitude, g.populacao,
               ln(a.estoque)                                        AS log_estoque,
               a.estoque * 1000.0 / nullif(g.populacao, 0)          AS por_mil_hab,
               -- Crescimento relativo na janela. `nullif` evita divisão por
               -- zero em município que não tinha estoque no ano-base.
               (a.estoque - coalesce(b.estoque_antes, 0))
                   / nullif(coalesce(b.estoque_antes, 0), 0)        AS crescimento,
               coalesce(f.saldo_janela, 0) * 1.0 / nullif(a.estoque, 0)
                                                                    AS intensidade_fluxo,
               a.desligamentos * 1.0 / nullif(a.estoque, 0)         AS rotatividade,
               a.remuneracao_sm_mediana
        FROM atual a
        LEFT JOIN anterior b USING (cod_municipio)
        LEFT JOIN fluxo f   USING (cod_municipio)
        LEFT JOIN {geo} g   ON g.cod6 = a.cod_municipio
        WHERE a.estoque >= {minimo}
          -- Estoque mínimo TAMBÉM no ano-base. Sem isto, a cidade que saiu de
          -- 1 para 1.428 vínculos entra com crescimento de 142.700% e define
          -- um grupo sozinha — ver o bloco sobre domicílio fiscal.
          AND coalesce(b.estoque_antes, 0) >= {minimo_base}
          AND g.latitude IS NOT NULL
          AND a.remuneracao_sm_mediana IS NOT NULL
        ORDER BY a.estoque DESC
    """).df()


def agrupar(df: pd.DataFrame, candidatos=range(3, 9)) -> tuple[pd.DataFrame, pd.DataFrame]:
    """K-means com k escolhido por silhueta."""
    from sklearn.cluster import KMeans
    from sklearn.metrics import silhouette_score
    from sklearn.preprocessing import StandardScaler

    d = df.dropna(subset=VARIAVEIS).copy()
    # Aparar as caudas antes de padronizar: um município com crescimento de
    # 4000% (saiu de 2 vínculos para 82) puxaria o centroide sozinho e criaria
    # um grupo de um elemento só. O corte é nos percentis, não em valor fixo.
    for v in VARIAVEIS:
        baixo, alto = d[v].quantile([0.01, 0.99])
        d[v] = d[v].clip(baixo, alto)

    X = StandardScaler().fit_transform(d[VARIAVEIS].to_numpy())
    # Silhueta sobre amostra: em 2 mil municípios a matriz de distâncias
    # completa é cara e o valor estabiliza bem antes disso.
    placar = []
    for k in candidatos:
        modelo = KMeans(n_clusters=k, n_init=10, random_state=42).fit(X)
        placar.append({
            "k": k,
            "silhueta": float(silhouette_score(X, modelo.labels_,
                                               sample_size=min(2000, len(X)),
                                               random_state=42)),
            "inercia": float(modelo.inertia_),
        })
    placar = pd.DataFrame(placar).sort_values("silhueta", ascending=False)

    melhor = int(placar.iloc[0]["k"])
    modelo = KMeans(n_clusters=melhor, n_init=10, random_state=42).fit(X)
    d["grupo"] = modelo.labels_
    return d, placar


def descrever(d: pd.DataFrame) -> pd.DataFrame:
    """Perfil médio de cada grupo — é o que dá nome ao agrupamento."""
    perfil = d.groupby("grupo").agg(
        municipios=("cod_municipio", "count"),
        estoque_total=("estoque", "sum"),
        estoque_mediano=("estoque", "median"),
        por_mil_hab=("por_mil_hab", "median"),
        crescimento=("crescimento", "median"),
        intensidade_fluxo=("intensidade_fluxo", "median"),
        rotatividade=("rotatividade", "median"),
        remuneracao=("remuneracao_sm_mediana", "median"),
    ).reset_index()
    return perfil.sort_values("estoque_total", ascending=False)


def rotular(perfil: pd.DataFrame) -> dict:
    """
    Nome legível para cada grupo, derivado do próprio perfil.

    Rotular à mão significaria reescrever os nomes toda vez que o dado mudar —
    e, pior, correr o risco de o nome deixar de descrever o grupo sem ninguém
    perceber. Aqui o rótulo é função dos números.
    """
    mediana_esp = perfil["por_mil_hab"].median()
    mediana_cres = perfil["crescimento"].median()
    mediana_rem = perfil["remuneracao"].median()

    nomes = {}
    for _, r in perfil.iterrows():
        especializado = r["por_mil_hab"] > mediana_esp
        crescendo = r["crescimento"] > mediana_cres
        paga_bem = r["remuneracao"] > mediana_rem
        if especializado and crescendo:
            nome = "Polo em expansão"
        elif especializado and not crescendo:
            nome = "Polo maduro"
        elif crescendo and paga_bem:
            nome = "Emergente qualificado"
        elif crescendo:
            nome = "Emergente"
        elif paga_bem:
            nome = "Estável e bem pago"
        else:
            nome = "Mercado incipiente"
        # Grupos podem empatar no rótulo; o sufixo mantém a chave única sem
        # inventar distinção que os números não sustentam.
        if nome in nomes.values():
            nome = f"{nome} ({int(r['grupo'])})"
        nomes[int(r["grupo"])] = nome
    return nomes


def main() -> int:
    p = argparse.ArgumentParser(description="Agrupa municípios por trajetória de TI.")
    p.add_argument("--ano", type=int, default=2025)
    p.add_argument("--janela", type=int, default=5, help="Anos para medir crescimento.")
    p.add_argument("--minimo", type=int, default=30,
                   # O %% é escape do argparse, que interpola o texto de ajuda.
                   help="Estoque mínimo. Abaixo disso a taxa vira ruído: sair "
                        "de 3 para 6 vínculos é 100%% de crescimento.")
    p.add_argument("--minimo-base", type=int, default=10,
                   help="Estoque mínimo no ano-base, para o crescimento "
                        "significar alguma coisa.")
    args = p.parse_args()

    print("=" * 78)
    print(f"  MUNICÍPIOS POR TRAJETÓRIA DO MERCADO DE TI — {args.ano}")
    print("=" * 78)

    df = montar(args.ano, args.janela, args.minimo, args.minimo_base)
    print(f"\n📊 {len(df):,} municípios com {args.minimo}+ vínculos de TI em "
          f"{args.ano} e {args.minimo_base}+ em {args.ano - args.janela}")
    print(f"   janela de crescimento: {args.ano - args.janela} a {args.ano}")
    print(f"   ⚠️  localização é a do ESTABELECIMENTO — ver o bloco sobre "
          f"domicílio fiscal no módulo")

    d, placar = agrupar(df)
    print(f"\n🧪 Escolha de k por silhueta")
    print(placar.to_string(index=False, float_format=lambda v: f"{v:,.3f}"))

    perfil = descrever(d)
    nomes = rotular(perfil)
    perfil["nome"] = perfil["grupo"].map(nomes)
    d["nome"] = d["grupo"].map(nomes)

    print(f"\n🏷️  Perfil dos grupos (medianas)")
    print(perfil[["nome", "municipios", "estoque_total", "estoque_mediano",
                  "por_mil_hab", "crescimento", "intensidade_fluxo",
                  "rotatividade", "remuneracao"]]
          .to_string(index=False, float_format=lambda v: f"{v:,.2f}"))

    print(f"\n📍 Exemplos de cada grupo (maiores de cada um)")
    for grupo, nome in nomes.items():
        exemplos = (d[d["grupo"] == grupo]
                    .nlargest(4, "estoque")[["municipio", "estoque"]])
        lista = ", ".join(f"{r.municipio} ({r.estoque:,.0f})"
                          for r in exemplos.itertuples())
        print(f"   {nome:<24} {lista}")

    con = conectar_duckdb()
    con.execute("SET enable_progress_bar=false")
    con.register("clusters", d)
    destino = f"s3://{BUCKET_GOLD}/municipios_cluster.parquet"
    con.execute(f"""
        COPY (SELECT * FROM clusters) TO '{destino}' (
            FORMAT PARQUET,
            COMPRESSION '{PARQUET_COMPRESSION.upper()}',
            COMPRESSION_LEVEL {PARQUET_COMPRESSION_LEVEL})
    """)
    print(f"\n🏁 {len(d):,} municípios gravados em {destino}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
