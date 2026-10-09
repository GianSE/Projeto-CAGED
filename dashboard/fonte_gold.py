"""
De onde vem a camada gold — a gold publicada no Hugging Face, sempre.

Ver gold_unificado/publicar_gold.py (o publicador) e _utils/nuvem.py (o
mesmo REPO_GOLD que o resto do pipeline usa, para que uma renomeação de
repositório não precise ser replicada aqui).
"""
from _utils import nuvem


def caminho(nome: str) -> str:
    return nuvem.fonte_leitura(nuvem.REPO_GOLD, f"{nome}.parquet")


def rotulo() -> str:
    """De onde os dados vieram, para o rodapé."""
    return f"gold publicada no Hugging Face ({nuvem.REPO_GOLD})"
