"""
Repõe a silver de TI a partir do que já está publicado, renomeando a partição.

POR QUE ISTO EXISTE
-------------------
A silver de TI do CAGED foi gravada espelhando o caminho do bronze
(`ano=`/`mes=`), enquanto o resto do lake usa `ano_particao=`. Alinhar os dois
NÃO exige reprocessar: o conteúdo é idêntico, só o nome da pasta muda.

Reprocessar o caged_old custaria ~12 horas (156 arquivos de 2,5 milhões de
linhas com 33 dicionários). Baixar os mesmos 118 MB já publicados e regravar
com o caminho novo leva minutos — e produz exatamente os mesmos bytes, porque
o construtor não mudou desde a publicação.

A ideia é do usuário, e é a correção certa: quando a diferença é de FORMA e
não de CONTEÚDO, mover é melhor que refazer.

Uso:
    python -m auditoria.repor_do_hub --tabela caged_old caged_ajustes
"""
import argparse
import re
import sys
import time
import urllib.request
import json

from extracao_ftp.config_extracao import BUCKET_SILVER_TI

REPO = "Gianpedro/caged-tecnologia"
BASE = f"https://huggingface.co/datasets/{REPO}/resolve/main"


def _fs():
    import s3fs
    from extracao_ftp.config_extracao import (
        MINIO_ACCESS_KEY, MINIO_ENDPOINT, MINIO_REGION, MINIO_SECRET_KEY)

    return s3fs.S3FileSystem(
        key=MINIO_ACCESS_KEY, secret=MINIO_SECRET_KEY,
        client_kwargs={"endpoint_url": f"http://{MINIO_ENDPOINT}",
                       "region_name": MINIO_REGION})


def listar(repo: str) -> list[str]:
    """Todos os parquets publicados — seguindo a paginação da API do Hub."""
    url = f"https://huggingface.co/api/datasets/{repo}/tree/main?recursive=true"
    caminhos = []
    while True:
        req = urllib.request.Request(f"{url}&cb={time.time()}",
                                     headers={"User-Agent": "repor"})
        with urllib.request.urlopen(req, timeout=60) as r:
            caminhos += [i["path"] for i in json.load(r)
                         if i["type"] == "file" and i["path"].endswith(".parquet")]
            link = r.headers.get("Link")
        m = re.search(r'<([^>]+)>;\s*rel="next"', link or "")
        if not m:
            break
        url = m.group(1)
    return caminhos


def destino_hive(caminho: str) -> str:
    """`caged_old/ano=2007/mes=1/x.parquet` -> `.../ano_particao=2007/mes_particao=1/x.parquet`."""
    novo = re.sub(r"(^|/)ano=", r"\1ano_particao=", caminho)
    return re.sub(r"(^|/)mes=", r"\1mes_particao=", novo)


def main() -> int:
    p = argparse.ArgumentParser(description="Repõe a silver de TI do Hub, renomeando a partição.")
    p.add_argument("--tabela", nargs="+", required=True)
    p.add_argument("--repo", default=REPO)
    args = p.parse_args()

    fs = _fs()
    todos = listar(args.repo)
    alvos = [c for c in todos if c.split("/")[0] in args.tabela and "/" in c]
    if not alvos:
        print(f"❌ Nada publicado para {args.tabela} em {args.repo}")
        return 1

    print(f"📥 {len(alvos)} arquivo(s) de {', '.join(args.tabela)}\n")
    repostos = pulados = falhas = 0

    for n, caminho in enumerate(sorted(alvos), start=1):
        destino = f"{BUCKET_SILVER_TI}/{destino_hive(caminho)}"
        if fs.exists(destino):
            pulados += 1
            continue
        try:
            url = f"{BASE}/{urllib.request.quote(caminho)}"
            with urllib.request.urlopen(
                    urllib.request.Request(url, headers={"User-Agent": "repor"}),
                    timeout=180) as r:
                dados = r.read()
            with fs.open(destino, "wb") as f:
                f.write(dados)
            repostos += 1
            print(f"   [{n}/{len(alvos)}] ✅ {destino.split('/')[-1]}  ({len(dados) / 1e6:.1f} MB)")
        except Exception as e:
            falhas += 1
            print(f"   [{n}/{len(alvos)}] ❌ {caminho}: {str(e)[:120]}")

    print(f"\n🏁 {repostos} reposto(s), {pulados} já existente(s), {falhas} falha(s)")
    return 0 if not falhas else 2


if __name__ == "__main__":
    sys.exit(main())
