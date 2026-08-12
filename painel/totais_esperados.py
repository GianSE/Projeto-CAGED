"""
Snapshot de quantos arquivos cada tabela deve ter no bronze quando completa.

Vem de uma varredura `--listar` no FTP (ver extracao_ftp/catalogo.py). Não é
recalculado a cada request do painel de propósito: escanear o FTP inteiro
demora minutos e o painel precisa responder em milissegundos.

ATENÇÃO — os totais dependem do ESCOPO extraído
-----------------------------------------------
Os números da RAIS abaixo são do recorte 2007+, que é o do estudo. A série
completa da RAIS (1985+) teria 41 arquivos de estabelecimento e 950 de
vínculos — usar aqueles totais faria o painel reportar 15% quando o
progresso real é 42%, porque o denominador seria de anos que nunca serão
baixados.

Se mudar o escopo da extração, rode o `--listar` correspondente e atualize:

    python -m extracao_ftp.run_extracao --listar --dataset rais --ano-inicio 2007

Sem entrada aqui o painel continua funcionando — só mostra a contagem
absoluta, sem barra percentual.
"""

# CAGED: escopo completo (1985+ na prática começa em 2002/2007 conforme a base)
# RAIS:  escopo 2007+ — ver aviso acima
TOTAIS_BRONZE = {
    "caged_ajustes": 128,
    "caged_exc": 75,
    "caged_for": 77,
    "caged_mov": 78,
    "caged_old": 156,
    "rais_estab": 19,
    "rais_vinc": 349,
}
