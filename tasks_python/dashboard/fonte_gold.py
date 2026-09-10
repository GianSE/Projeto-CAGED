"""
De onde vem a camada gold — resolvido sozinho, sem configuração.

O PROBLEMA
----------
O mesmo código roda em dois lugares muito diferentes:

    máquina local          -> MinIO ao lado, dado sempre fresco
    Streamlit Cloud        -> nenhum MinIO, só o que estiver público

Amarrar isso a uma variável de ambiente funciona, mas transfere para quem
publica a obrigação de lembrar de configurá-la — e o modo de falha é ruim: sem
a variável, o app sobe, as abas aparecem, e cada uma diz "modelo ainda não
materializado". Página inteira funcionando e vazia, sem erro nenhum.

A ORDEM DE PREFERÊNCIA
----------------------
    1. DADOS_URL_BASE_RAIS, se alguém definiu   (controle explícito vence)
    2. MinIO local, se responder                 (desenvolvimento: dado fresco)
    3. a gold publicada no Hugging Face          (produção: sempre existe)

O passo 2 é testado de verdade, com uma leitura. Checar se a variável de
ambiente do MinIO existe não serviria: ela tem valor padrão e existe sempre,
inclusive onde não há MinIO algum.

O passo 3 é o que faz o deploy funcionar sem ninguém configurar nada. O custo é
que a produção lê uma cópia — publicada por `gold_unificado.publicar_gold` —, e
não o dado vivo. Para uma camada agregada que muda quando a base anual sai, é o
compromisso certo.
"""
import os

import streamlit as st

from dashboard.dados import conectar
from extracao_ftp.config_extracao import BUCKET_GOLD

# A gold publicada. 3 MB em 24 tabelas — os mesmos números da silver, já
# agregados. Ver gold_unificado/publicar_gold.py.
GOLD_PUBLICADA = ("https://huggingface.co/datasets/Gianpedro/"
                  "mercado-ti-gold/resolve/main")

# Tabela usada para testar se o MinIO responde. Qualquer uma serve; esta é
# pequena e existe desde o primeiro build territorial.
SENTINELA = "mapa_uf"


@st.cache_resource(show_spinner=False)
def base() -> str:
    """A raiz da gold, decidida uma vez por sessão."""
    explicito = os.getenv("DADOS_URL_BASE_RAIS", "").rstrip("/")
    if explicito:
        return explicito
    try:
        conectar().execute(
            f"SELECT 1 FROM read_parquet('s3://{BUCKET_GOLD}/{SENTINELA}.parquet') "
            f"LIMIT 1").fetchone()
        return f"s3://{BUCKET_GOLD}"
    except Exception:
        return GOLD_PUBLICADA


def caminho(nome: str) -> str:
    return f"{base()}/{nome}.parquet"


def rotulo() -> str:
    """De onde os dados vieram, para o rodapé — sem expor credencial."""
    atual = base()
    if atual.startswith("s3://"):
        return "data lake local (MinIO)"
    if "huggingface" in atual:
        return "gold publicada no Hugging Face"
    return "fonte configurada por variável de ambiente"
