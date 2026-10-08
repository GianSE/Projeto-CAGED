"""
Controle de estado da extração.

Duas responsabilidades:
  - Idempotência: saber se um item já virou parquet no MinIO, para poder
    interromper e retomar a carga sem refazer trabalho.
  - Manifesto: registrar em CSV o que foi processado (linhas, tempo, status),
    que serve de trilha de auditoria da ingestão.
"""
import csv
from datetime import datetime
from pathlib import Path

import boto3
from botocore.client import Config
from botocore.exceptions import ClientError

from extracao_ftp.catalogo import ItemTrabalho
from extracao_ftp.config_extracao import (
    DIR_LOGS,
    MINIO_ACCESS_KEY,
    MINIO_ENDPOINT,
    MINIO_REGION,
    MINIO_SECRET_KEY,
)

CAMPOS_MANIFESTO = [
    "data_hora",
    "tabela",
    "ano",
    "mes",
    "recorte",
    "arquivo_fonte",
    "destino_s3",
    "linhas",
    "bytes_compactado",
    "segundos",
    "status",
    "observacao",
]


def _cliente_s3():
    return boto3.client(
        "s3",
        endpoint_url=f"http://{MINIO_ENDPOINT}",
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        region_name=MINIO_REGION,
        config=Config(signature_version="s3v4", s3={"addressing_style": "path"}),
    )


class EstadoLake:
    """
    Sabe o que já foi ingerido — no MinIO local, ou no Hugging Face no modo
    nuvem (MODO_NUVEM=1, ver extracao_ftp/nuvem.py). A pergunta é a mesma
    nos dois modos ("esse destino já existe?"); só a fonte consultada muda.
    """

    def __init__(self):
        from extracao_ftp import nuvem

        self.nuvem = nuvem.MODO_NUVEM
        self.s3 = None if self.nuvem else _cliente_s3()

    @staticmethod
    def _partes(destino_s3: str) -> tuple[str, str]:
        sem_esquema = destino_s3.replace("s3://", "", 1)
        bucket, _, chave = sem_esquema.partition("/")
        return bucket, chave

    def ja_existe(self, item: ItemTrabalho) -> bool:
        if self.nuvem:
            from extracao_ftp import nuvem

            # Por caminho_fonte, não pelo nome esperado do destino: arquivo
            # grande é quebrado em _parteNN só na publicação pro Hub (ver
            # caminhos_fonte_ingeridos) — o nome de destino nunca existe
            # sozinho para esses casos.
            return item.caminho_remoto in nuvem.caminhos_fonte_ingeridos(item.tabela, item.ano)
        bucket, chave = self._partes(item.destino_s3)
        try:
            self.s3.head_object(Bucket=bucket, Key=chave)
            return True
        except ClientError:
            return False

    def tamanho_destino(self, item: ItemTrabalho) -> int:
        if self.nuvem:
            # Metadado de tamanho não é consultado hoje no modo nuvem: a
            # listagem do Hub (`list_repo_files`) não traz tamanho sem uma
            # chamada por arquivo, e nada do pipeline depende deste valor
            # para decidir o que reprocessar — só `ja_existe` decide isso.
            return 0
        bucket, chave = self._partes(item.destino_s3)
        try:
            return self.s3.head_object(Bucket=bucket, Key=chave)["ContentLength"]
        except ClientError:
            return 0

    def testar_conexao(self) -> bool:
        if self.nuvem:
            from extracao_ftp import nuvem

            try:
                nuvem.arquivos_remotos(nuvem.REPO_BRONZE_CAGED)
                return True
            except Exception as e:
                print(f"❌ Não consegui falar com o Hugging Face: {e}")
                return False
        try:
            self.s3.list_buckets()
            return True
        except Exception as e:
            print(f"❌ Não consegui falar com o MinIO em {MINIO_ENDPOINT}: {e}")
            return False


class Manifesto:
    """Log CSV append-only de tudo que a extração processou."""

    def __init__(self, caminho: Path | None = None):
        DIR_LOGS.mkdir(parents=True, exist_ok=True)
        self.caminho = caminho or (DIR_LOGS / "manifesto_extracao.csv")
        if not self.caminho.exists():
            with open(self.caminho, "w", newline="", encoding="utf-8") as f:
                csv.DictWriter(f, fieldnames=CAMPOS_MANIFESTO).writeheader()

    def registrar(self, item: ItemTrabalho, linhas: int, segundos: float,
                  status: str, observacao: str = "") -> None:
        with open(self.caminho, "a", newline="", encoding="utf-8") as f:
            csv.DictWriter(f, fieldnames=CAMPOS_MANIFESTO).writerow(
                {
                    "data_hora": datetime.now().isoformat(timespec="seconds"),
                    "tabela": item.tabela,
                    "ano": item.ano,
                    "mes": item.mes if item.mes is not None else "",
                    "recorte": item.recorte or "",
                    "arquivo_fonte": item.nome_arquivo,
                    "destino_s3": item.destino_s3,
                    "linhas": linhas,
                    "bytes_compactado": item.tamanho,
                    "segundos": round(segundos, 1),
                    "status": status,
                    "observacao": observacao,
                }
            )
