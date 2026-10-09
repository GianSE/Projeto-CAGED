"""
Controle de estado da extração.

Duas responsabilidades:
  - Idempotência: saber se um item já foi publicado no Hugging Face, para
    poder interromper e retomar a carga sem refazer trabalho.
  - Manifesto: registrar em CSV o que foi processado (linhas, tempo,
    status), que serve de trilha de auditoria da ingestão.
"""
import csv
from datetime import datetime
from pathlib import Path

from extracao_ftp.catalogo import ItemTrabalho
from extracao_ftp.config_extracao import DIR_LOGS

CAMPOS_MANIFESTO = [
    "data_hora", "tabela", "ano", "mes", "recorte", "arquivo_fonte",
    "destino", "linhas", "bytes_compactado", "segundos", "status", "observacao",
]


class EstadoLake:
    """Sabe o que já foi publicado no Hugging Face — ver _utils/nuvem.py."""

    def ja_existe(self, item: ItemTrabalho) -> bool:
        from _utils import nuvem

        # Por caminho_fonte, não pelo nome esperado do destino: arquivo
        # grande é quebrado em _parteNN só na publicação pro Hub (ver
        # caminhos_fonte_ingeridos) — o nome de destino nunca existe
        # sozinho para esses casos.
        try:
            fontes = nuvem.caminhos_fonte_ingeridos(item.tabela, item.ano)
        except Exception as e:
            # A consulta falhou de verdade (não "vazio") — assume que JÁ
            # EXISTE. Pular um item que precisava reprocessar é barato (a
            # próxima verificação pega de novo); reprocessar ou duplicar um
            # item que já existia não é.
            print(f"⚠️  não consegui confirmar {item.rotulo}, assumindo que já "
                  f"existe (mais seguro que reprocessar à toa): {e}")
            return True
        return item.caminho_remoto in fontes

    def testar_conexao(self) -> bool:
        from _utils import nuvem

        try:
            nuvem.arquivos_remotos(nuvem.REPO_BRONZE_CAGED)
            return True
        except Exception as e:
            print(f"❌ Não consegui falar com o Hugging Face: {e}")
            return False


class Manifesto:
    """Log CSV append-only de tudo que a extração processou nesta execução."""

    def __init__(self, caminho: Path | None = None):
        DIR_LOGS.mkdir(parents=True, exist_ok=True)
        self.caminho = caminho or (DIR_LOGS / "manifesto_extracao.csv")
        if not self.caminho.exists():
            with open(self.caminho, "w", newline="", encoding="utf-8") as f:
                csv.DictWriter(f, fieldnames=CAMPOS_MANIFESTO).writeheader()

    def registrar(self, item: ItemTrabalho, linhas: int, segundos: float,
                  status: str, observacao: str = "") -> None:
        with open(self.caminho, "a", newline="", encoding="utf-8") as f:
            csv.DictWriter(f, fieldnames=CAMPOS_MANIFESTO).writerow({
                "data_hora": datetime.now().isoformat(timespec="seconds"),
                "tabela": item.tabela,
                "ano": item.ano,
                "mes": item.mes if item.mes is not None else "",
                "recorte": item.recorte or "",
                "arquivo_fonte": item.nome_arquivo,
                "destino": item.destino_rel,
                "linhas": linhas,
                "bytes_compactado": item.tamanho,
                "segundos": round(segundos, 1),
                "status": status,
                "observacao": observacao,
            })
