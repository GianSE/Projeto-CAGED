# Use a versão que você preferir (3.6.13 ou a nova 3.6.16)
FROM python:3.12-slim

WORKDIR /app

# Evita que o Python gere arquivos .pyc e permite logs em tempo real
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

# --- Dependências de sistema: 7-Zip (descompactação dos .7z da RAIS/CAGED) ---
USER root
RUN apt-get update && \
    apt-get install -y p7zip-full && \
    rm -rf /var/lib/apt/lists/*
# -----------------------------------------------------------------

COPY worker/requirements.txt .

# O cache do BuildKit preserva os wheels já baixados entre tentativas de build:
# duckdb, pyarrow e polars passam de 100 MB cada e o PyPI costuma cair no meio.
# Sem --no-cache-dir, justamente para o cache montado ser aproveitado.
RUN --mount=type=cache,target=/root/.cache/pip \
    pip install --timeout 300 --retries 20 -r requirements.txt

CMD ["tail", "-f", "/dev/null"]
