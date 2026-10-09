"""
Coordenadas dos municípios, para o mapa — enriquecimento externo.

POR QUE NÃO NOMINATIM
---------------------
A escolha óbvia seria geocodificar. Não é a certa aqui: são 5.570 municípios,
o Nominatim pede no máximo uma requisição por segundo para uso automatizado, e
geocodificação em massa é explicitamente desencorajada na política de uso
deles. Seriam horas de requisições contra um serviço voluntário para obter,
com erro de correspondência de nome, algo que já existe consolidado e oficial.

A fonte usada é a tabela do IBGE de municípios com centroide, publicada como
CSV único. Uma requisição, dado oficial, sem ambiguidade de nome.

O MESMO VALE PARA OSRM E APIS DE CEP
------------------------------------
Fazem sentido para roteamento e endereço individual, que não é o que este
trabalho pergunta. O dado aqui é agregado por município — não há endereço a
resolver nem rota a calcular. Chamar essas APIs acrescentaria dependência de
rede e limite de taxa sem responder nada que o dado já não responda.

A JUNÇÃO É POR CÓDIGO, NÃO POR NOME
-----------------------------------
A RAIS grava o código IBGE de 6 dígitos ('110001'); o CSV traz o de 7
('1100015'), que é o mesmo código mais o dígito verificador. Casar pelos 6
primeiros é exato.

Casar por NOME seria frágil: a base do MTE grava sem acento e com a UF colada
('Sp-Sao Paulo'), e existem municípios homônimos em estados diferentes — são
21 "Bom Jesus" no Brasil. O nome fica como conferência, não como chave.

Uso:
    python -m gold_unificado.geo_municipios
"""
import argparse
import io
import sys
import unicodedata
import urllib.request

import pandas as pd

from extracao_ftp.config_extracao import (
    BUCKET_GOLD,
    BUCKET_SILVER_TI,
    PARQUET_COMPRESSION,
    PARQUET_COMPRESSION_LEVEL,
    conectar_duckdb,
)

FONTE = ("https://raw.githubusercontent.com/kelvins/municipios-brasileiros/"
         "main/csv/municipios.csv")

# Agregado 6579 do IBGE = população residente estimada; variável 9324, nível
# N6 = município. Uma requisição devolve os 5.571.
URL_POPULACAO = ("https://servicodados.ibge.gov.br/api/v3/agregados/6579/"
                 "periodos/2021/variaveis/9324?localidades=N6[all]")

# Código numérico da UF no IBGE -> sigla. Fixo porque não muda desde 1988.
UF_POR_CODIGO = {
    11: "RO", 12: "AC", 13: "AM", 14: "RR", 15: "PA", 16: "AP", 17: "TO",
    21: "MA", 22: "PI", 23: "CE", 24: "RN", 25: "PB", 26: "PE", 27: "AL",
    28: "SE", 29: "BA", 31: "MG", 32: "ES", 33: "RJ", 35: "SP", 41: "PR",
    42: "SC", 43: "RS", 50: "MS", 51: "MT", 52: "GO", 53: "DF",
}


def sem_acento(texto: str) -> str:
    return "".join(c for c in unicodedata.normalize("NFKD", str(texto))
                   if not unicodedata.combining(c)).upper().strip()


def populacao() -> pd.DataFrame:
    """
    População municipal, da API de agregados do IBGE.

    Entra porque tamanho absoluto não distingue polo de tecnologia: São Paulo
    lidera qualquer contagem por ser São Paulo. Vínculos de TI POR HABITANTE é
    o que revela a cidade que se especializou — e é a variável que faz um
    agrupamento por trajetória dizer algo além de "grande, médio, pequeno".
    """
    import gzip
    import json

    # O IBGE às vezes responde comprimido mesmo sem `Accept-Encoding: gzip`, e
    # de forma inconsistente entre chamadas — a mesma URL veio em texto puro
    # numa requisição e gzipada na seguinte. Detectar pela assinatura do
    # conteúdo (1f 8b) é mais confiável que confiar no cabeçalho.
    req = urllib.request.Request(URL_POPULACAO, headers={"User-Agent": "tcc-caged"})
    with urllib.request.urlopen(req, timeout=180) as r:
        bruto = r.read()
    if bruto[:2] == b"\x1f\x8b":
        bruto = gzip.decompress(bruto)
    dados = json.loads(bruto.decode("utf-8"))
    series = dados[0]["resultados"][0]["series"]
    linhas = []
    for s in series:
        valor = next(iter(s["serie"].values()), None)
        try:
            habitantes = int(valor)
        except (TypeError, ValueError):
            continue
        linhas.append({"cod6": str(s["localidade"]["id"])[:6],
                       "populacao": habitantes})
    return pd.DataFrame(linhas)


def baixar() -> pd.DataFrame:
    req = urllib.request.Request(FONTE, headers={"User-Agent": "tcc-caged"})
    with urllib.request.urlopen(req, timeout=120) as r:
        df = pd.read_csv(io.StringIO(r.read().decode("utf-8")))
    df["uf"] = df["codigo_uf"].map(UF_POR_CODIGO)
    df["cod6"] = df["codigo_ibge"].astype(str).str[:6]
    df["nome_norm"] = df["nome"].map(sem_acento)
    df = df[["cod6", "codigo_ibge", "nome", "nome_norm", "uf",
             "latitude", "longitude", "capital"]]
    try:
        df = df.merge(populacao(), on="cod6", how="left")
    except Exception as e:
        # Coordenada sem população ainda serve para o mapa; população sem
        # coordenada não serviria para nada. Por isso a falha aqui é tolerada
        # e reportada, em vez de derrubar o enriquecimento inteiro.
        print(f"   ⚠️  população indisponível ({str(e)[:70]}); seguindo sem ela")
        df["populacao"] = pd.NA
    return df


def main() -> int:
    p = argparse.ArgumentParser(description="Coordenadas dos municípios para o mapa.")
    p.add_argument("--ano", type=int, default=2024,
                   help="Ano usado só para conferir a taxa de casamento.")
    args = p.parse_args()

    print("=" * 72)
    print("  GEO — coordenadas dos municípios (IBGE)")
    print("=" * 72)

    geo = baixar()
    print(f"\n📥 {len(geo):,} municípios com centroide")

    con = conectar_duckdb()
    con.execute("SET enable_progress_bar=false")
    con.execute("SET threads=4")
    con.register("geo", geo)

    # Conferência da chave ANTES de gravar: sem isso, um mapa sairia com
    # metade dos municípios faltando e ninguém notaria — as bolhas ausentes
    # não deixam buraco visível, só encolhem o Brasil em silêncio.
    nossos = con.execute(f"""
        SELECT DISTINCT municipio AS cod6, municipio_descricao
        FROM read_parquet('s3://{BUCKET_SILVER_TI}/rais_vinc/ano_particao={args.ano}/**/*.parquet')
        WHERE municipio IS NOT NULL
    """).df()
    casaram = nossos.merge(geo, on="cod6", how="left")
    faltam = casaram[casaram["latitude"].isna()]
    print(f"\n🔗 Junção por código IBGE de 6 dígitos, RAIS {args.ano}")
    print(f"   {len(nossos):,} municípios na base")
    print(f"   {len(casaram) - len(faltam):,} casaram "
          f"({(1 - len(faltam) / len(nossos)) * 100:.1f}%)")
    if len(faltam):
        print(f"   sem coordenada: {faltam['municipio_descricao'].head(8).tolist()}")

    destino = f"s3://{BUCKET_GOLD}/geo_municipios.parquet"
    con.execute(f"""
        COPY (SELECT * FROM geo) TO '{destino}' (
            FORMAT PARQUET,
            COMPRESSION '{PARQUET_COMPRESSION.upper()}',
            COMPRESSION_LEVEL {PARQUET_COMPRESSION_LEVEL})
    """)
    print(f"\n🏁 gravado em {destino}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
