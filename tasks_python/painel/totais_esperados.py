"""
Snapshot de quantos arquivos cada tabela deve ter no bronze quando completa.

Vem de uma varredura --listar no FTP (ver extracao_ftp/catalogo.py). Não é
recalculado a cada request do painel de propósito: escanear o FTP inteiro
demora minutos e o painel precisa responder em milissegundos. Atualize aqui
manualmente depois de rodar `--listar` de novo (ex.: quando a RAIS entrar).

Sem entrada aqui, o painel ainda funciona — só não mostra a barra de progresso
percentual, apenas a contagem absoluta.
"""

# python -m extracao_ftp.run_extracao --listar --ano-inicio 1985
TOTAIS_BRONZE = {
    "caged_ajustes": 128,
    "caged_exc": 75,
    "caged_for": 77,
    "caged_mov": 78,
    "caged_old": 156,
    # RAIS ainda não extraída — preencher quando a carga começar:
    # "rais_estab": ...,
    # "rais_vinc": ...,
}
