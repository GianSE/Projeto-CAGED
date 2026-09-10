"""
Publica a camada gold no Hugging Face, para o dashboard hospedado ler.

POR QUE A GOLD E NÃO A SILVER
-----------------------------
O dashboard em produção não tem MinIO. Ele precisa ler de algum lugar público,
e a escolha entre silver e gold decide se a página abre em um segundo ou em
trinta.

A silver de TI publicada tem 110 MB por arquivo consolidado; a gold inteira tem
3,2 MB em 24 tabelas. São os mesmos números — a gold é a silver já agregada nas
dimensões que o dashboard oferece. Publicar a gold é publicar a resposta em vez
do insumo.

O QUE VAI JUNTO
---------------
Tudo que as abas leem: agregados da RAIS, territoriais das duas bases,
coordenadas do IBGE, e os resultados dos modelos (previsão, nowcast,
sobrevivência, hiato, agrupamento). Nada de modelo serializado — só resultado,
pelo motivo explicado em `ciencia_dados.materializar`.

Uso:
    python -m gold_unificado.publicar_gold
    python -m gold_unificado.publicar_gold --repo Gianpedro/outro
"""
import argparse
import os
import shutil
import sys
from pathlib import Path

from extracao_ftp.config_extracao import BUCKET_GOLD

REPO = "Gianpedro/mercado-ti-gold"
RAIZ = Path(__file__).resolve().parents[2]
DIR_LOCAL = RAIZ / "publicacao" / "gold"

CARTAO = """---
license: odc-by
language: [pt]
tags: [brazil, labor-market, technology, caged, rais, gold]
---

# Mercado de trabalho em tecnologia — camada gold

Agregados prontos para consumo, derivados dos microdados públicos do **CAGED**
e da **RAIS** (Ministério do Trabalho e Emprego), recortados em tecnologia por
setor (CNAE) **ou** ocupação (CBO).

É a camada que alimenta o dashboard. São os mesmos números da camada silver, já
agregados: 3 MB no lugar de 110 MB por arquivo, com a mesma resposta.

## O que tem aqui

| grupo | tabelas |
|---|---|
| **Estoque (RAIS)** | `rais_estoque_anual`, `rais_estoque_uf`, `rais_estoque_area`, `rais_estoque_perfil`, `rais_remuneracao_ocupacao`, `rais_estoque_municipio`, `rais_setor_vs_ocupacao`, `rais_estabelecimentos` |
| **Território (as duas bases)** | `mapa_uf`, `mapa_municipio`, `geo_municipios` |
| **Modelos** | `previsao_saldo`, `previsao_placar`, `serie_mensal`, `nowcast_*`, `sobrevivencia_*`, `hiato_serie`, `municipios_cluster` |

## Como ler

```python
import duckdb
base = "https://huggingface.co/datasets/{repo}/resolve/main"
duckdb.sql(f"SELECT * FROM read_parquet('{{base}}/rais_estoque_anual.parquet')")
```

## Ressalvas que acompanham o dado

**Localização é a do estabelecimento, não a do trabalho.** A empresa de TI
escolhe onde se registrar por causa do ISS. Barueri aparece com 200 vínculos de
TI por mil habitantes; Guaraciaba (MG), com 10 mil habitantes, saiu de 1 vínculo
em 2022 para 1.428 em 2023. A coluna da RAIS que resolveria isso, `mun_trab`,
vem como "não informado" em praticamente todos os registros.

**A remuneração tem teto de plausibilidade.** A partir de 2023 a fonte traz
registros com a conversão para salário mínimo invertida — multiplicada em vez de
dividida. São 0,018% dos vínculos, e eles sozinhos respondiam por 35% da massa
salarial. Sem o corte em 500 SM, a média saltaria de 6,17 para 8,84 entre 2022 e
2023, o que se leria como valorização de 43% que não houve.

**A parte não explicada do hiato salarial não é medida de discriminação.** Ela
contém tudo que afeta salário e não está no modelo. O que se afirma é o limite
superior da diferença de retorno.

**O estoque é `vinculo_ativo_3112 = SIM`** — vínculos ativos em 31/12. Os que
existiram e terminaram durante o ano entram como `desligados_no_ano`, não no
estoque.

## Fontes

- CAGED e RAIS: FTP público do MTE (`ftp.mtps.gov.br/pdet/microdados`)
- Coordenadas e população dos municípios: IBGE
"""


def _fs():
    import s3fs
    from extracao_ftp.config_extracao import (
        MINIO_ACCESS_KEY, MINIO_ENDPOINT, MINIO_REGION, MINIO_SECRET_KEY)

    return s3fs.S3FileSystem(
        key=MINIO_ACCESS_KEY, secret=MINIO_SECRET_KEY,
        client_kwargs={"endpoint_url": f"http://{MINIO_ENDPOINT}",
                       "region_name": MINIO_REGION})


def _credencial() -> str | None:
    from dotenv import load_dotenv

    load_dotenv(RAIZ / ".env")
    return os.getenv("HF_TOKEN")


def main() -> int:
    p = argparse.ArgumentParser(description="Publica a gold no Hugging Face.")
    p.add_argument("--repo", default=REPO)
    p.add_argument("--so-card", action="store_true")
    args = p.parse_args()

    from huggingface_hub import HfApi

    token = _credencial()
    if not token:
        print("❌ HF_TOKEN não encontrado no .env")
        return 1
    api = HfApi(token=token)
    api.create_repo(repo_id=args.repo, repo_type="dataset", exist_ok=True)

    print("=" * 72)
    print(f"  PUBLICANDO A GOLD -> {args.repo}")
    print("=" * 72)

    if DIR_LOCAL.exists():
        shutil.rmtree(DIR_LOCAL)
    DIR_LOCAL.mkdir(parents=True, exist_ok=True)
    (DIR_LOCAL / "README.md").write_text(
        CARTAO.replace("{repo}", args.repo), encoding="utf-8")

    if not args.so_card:
        fs = _fs()
        arquivos = sorted(fs.glob(f"{BUCKET_GOLD}/*.parquet"))
        if not arquivos:
            print("❌ Nenhuma tabela na gold. Rode os construtores antes.")
            return 1
        total = 0
        for caminho in arquivos:
            nome = caminho.split("/")[-1]
            destino = DIR_LOCAL / nome
            fs.get(caminho, str(destino))
            total += destino.stat().st_size
            print(f"   📥 {nome:<36} {destino.stat().st_size / 1024:>8,.0f} KB")
        print(f"\n   {len(arquivos)} tabela(s), {total / 1e6:.1f} MB")

    api.upload_folder(folder_path=str(DIR_LOCAL), repo_id=args.repo,
                      repo_type="dataset")
    print(f"\n🏁 https://huggingface.co/datasets/{args.repo}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
