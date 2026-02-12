# Use a versão que você preferir (3.6.13 ou a nova 3.6.16)
FROM python:3.12-slim

WORKDIR /app

# Evita que o Python gere arquivos .pyc e permite logs em tempo real
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

# --- NOVO BLOCO: Instala dependências de sistema para o MariaDB ---
USER root
RUN apt-get update && \
    apt-get install -y libmariadb-dev gcc pkg-config && \
    rm -rf /var/lib/apt/lists/*
# -----------------------------------------------------------------

COPY worker/requirements.txt .

# Agora o pip vai conseguir compilar o mariadb
RUN pip install --no-cache-dir -r requirements.txt

CMD ["tail", "-f", "/dev/null"]