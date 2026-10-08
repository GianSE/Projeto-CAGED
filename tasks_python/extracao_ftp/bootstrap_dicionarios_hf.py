"""
Publica os dicionários do MTE (CBO, CNAE, escolaridade, ...) no Hugging
Face — passo ÚNICO E MANUAL, rodado da sua máquina com o MinIO local, antes
do pipeline em modo nuvem conseguir traduzir qualquer coisa.

POR QUE ISSO NÃO É AUTOMÁTICO NO CI
-------------------------------------
Os dicionários vêm de planilhas de layout que o MTE publica raramente (não
por competência) e a extração delas (`extrair_dicionarios`, em
extracao_ftp/dicionarios.py) ainda só sabe escrever no MinIO. Reescrever
isso para rodar no runner teria o mesmo custo da silver, para um ganho que
não se repete — uma vez publicado, dura até o MTE mudar o layout, o que é
raro. Dicionário errado também é o jeito mais silencioso de quebrar a
tradução (ver nota em memória: "o join pode sair vazio sem erro nenhum"),
então mais vale publicar manualmente, uma vez, com você olhando o resultado.

USO
---
Com o MinIO local no ar e os dicionários já extraídos (ou extraia agora
com `python -m extracao_ftp.run_extracao --so-dicionarios`):

    python -m extracao_ftp.bootstrap_dicionarios_hf

Reexecute sempre que extrair um dicionário novo localmente.
"""
import sys
from pathlib import Path

from extracao_ftp import nuvem
from extracao_ftp.config_extracao import (
    MINIO_ACCESS_KEY,
    MINIO_ENDPOINT,
    MINIO_REGION,
    MINIO_SECRET_KEY,
)
from silver_caged.dicionarios import PREFIXO_DICIONARIOS

DIR_LOCAL = Path(__file__).resolve().parents[2] / "publicacao" / "_dicionarios_espelho"


def _fs_minio():
    import s3fs

    return s3fs.S3FileSystem(
        key=MINIO_ACCESS_KEY, secret=MINIO_SECRET_KEY,
        client_kwargs={"endpoint_url": f"http://{MINIO_ENDPOINT}", "region_name": MINIO_REGION},
    )


def main() -> int:
    fs = _fs_minio()
    arquivos = sorted(fs.glob(f"{PREFIXO_DICIONARIOS}/**/*.parquet"))
    if not arquivos:
        print(f"❌ Nenhum dicionário em s3://{PREFIXO_DICIONARIOS}/. "
              "Rode antes: python -m extracao_ftp.run_extracao --so-dicionarios")
        return 1

    if DIR_LOCAL.exists():
        import shutil
        shutil.rmtree(DIR_LOCAL)
    DIR_LOCAL.mkdir(parents=True, exist_ok=True)

    print(f"📥 Espelhando {len(arquivos)} dicionário(s) do MinIO...")
    for caminho in arquivos:
        rel = caminho.removeprefix(f"{PREFIXO_DICIONARIOS}/")
        destino = DIR_LOCAL / rel
        destino.parent.mkdir(parents=True, exist_ok=True)
        fs.get(caminho, str(destino))

    print(f"📤 Publicando em {nuvem.REPO_DICIONARIOS}...")
    from huggingface_hub import HfApi

    api = HfApi()
    api.create_repo(repo_id=nuvem.REPO_DICIONARIOS, repo_type="dataset", exist_ok=True)
    api.upload_folder(folder_path=str(DIR_LOCAL), repo_id=nuvem.REPO_DICIONARIOS,
                      repo_type="dataset")
    print(f"\n🏁 https://huggingface.co/datasets/{nuvem.REPO_DICIONARIOS}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
