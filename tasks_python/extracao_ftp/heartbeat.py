"""
Heartbeat da extração: um JSON pequeno dizendo "o que está rodando agora".

O manifesto (estado.py) só registra itens JÁ concluídos — bom para auditoria,
inútil para "está travado ou só é lento?". Este módulo escreve
{STAGING_DIR}/logs/status_atual.json a cada item que começa e termina, para o
painel web ler sem precisar tocar no DuckDB (que já está ocupado com a carga).

Escrita atômica (arquivo temporário + rename) para o painel nunca ler um JSON
pela metade.
"""
import json
import os
import time
from pathlib import Path

from extracao_ftp.config_extracao import DIR_LOGS

CAMINHO_HEARTBEAT = DIR_LOGS / "status_atual.json"

# Acima disso, o painel considera o processo morto/parado (e não só "lento
# processando um arquivo grande") — a RAIS_VINC_PUB_SP passa de 1h em disco
# lento, então a margem é generosa.
SEGUNDOS_PARA_CONSIDERAR_MORTO = 60 * 30


def escrever(**campos) -> None:
    """Atualiza o heartbeat. Falha silenciosa: isso nunca deve derrubar a extração."""
    try:
        DIR_LOGS.mkdir(parents=True, exist_ok=True)
        dados = {**campos, "atualizado_em": time.time()}
        temp = CAMINHO_HEARTBEAT.with_suffix(".tmp")
        temp.write_text(json.dumps(dados, ensure_ascii=False), encoding="utf-8")
        os.replace(temp, CAMINHO_HEARTBEAT)
    except Exception:
        pass


def limpar() -> None:
    """Remove o heartbeat ao final da execução (processo não está mais rodando)."""
    try:
        CAMINHO_HEARTBEAT.unlink(missing_ok=True)
    except Exception:
        pass


def ler() -> dict | None:
    """
    Lê o heartbeat, se existir e for recente.

    Devolve None se não há extração rodando (arquivo ausente ou muito velho —
    processo provavelmente morreu sem limpar).
    """
    try:
        dados = json.loads(CAMINHO_HEARTBEAT.read_text(encoding="utf-8"))
    except Exception:
        return None

    idade = time.time() - dados.get("atualizado_em", 0)
    if idade > SEGUNDOS_PARA_CONSIDERAR_MORTO:
        return None

    dados["idade_segundos"] = round(idade, 1)
    return dados
