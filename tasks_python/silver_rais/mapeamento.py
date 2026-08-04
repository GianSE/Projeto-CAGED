"""
Configuração de como o bronze da RAIS vira silver.

Diferente do CAGED, aqui ainda NÃO temos o bronze real pra conferir os nomes
de coluna (a extração da RAIS — 950+ arquivos, ~56 GB — ainda não rodou).
Por isso o mapeamento fica só no modo automático: para cada coluna do bronze,
se existir uma aba com o MESMO nome em bronze/dicionarios/rais_layouts/, ela
é traduzida (mesmo truque que funcionou para o Novo CAGED, onde o nome da
coluna bate com o nome da aba do dicionário).

QUANDO O BRONZE DA RAIS EXISTIR: rode
    python -m silver_rais.construir_silver --listar
para ver quais colunas casaram e quais não. Não casar não é erro — só quer
dizer que o nome da coluna no bronze não é idêntico ao nome da aba (ex.: a
aba se chama "escolaridade_ou_g_instrucao" mas a coluna pode se chamar
"grau_instrucao_apos_2005"). Nesse caso, acrescente a entrada em MAPA_MANUAL
abaixo — o mesmo padrão usado em silver_caged/mapeamento.py para o CAGED
antigo — depois de CONFERIR o nome de verdade, não adivinhar.
"""

NAMESPACE_DICIONARIO = "rais_layouts"

# Abas do dicionário que são texto de apoio (layout completo, planilha crua),
# não uma tabela de código -> descrição — nunca tentar casar com uma coluna.
ABAS_NAO_TRADUZIVEIS = {"plan1", "rais_layout", "raisd_layout", "estb_layout"}

# Preenchido manualmente depois de rodar --listar contra o bronze real (ver
# docstring). Formato idêntico ao MAPA_CAGED_ANTIGO de silver_caged/mapeamento.py.
MAPA_MANUAL: dict[str, dict] = {
    # "grau_instrucao_apos_2005": {"estilo": "titulo_codigo", "aba": "escolaridade_ou_g_instrucao",
    #                              "col_desc": "col_00", "col_cod": "col_02"},
    # "faixa_etaria": {"estilo": "titulo_codigo", "aba": "faixas", "col_desc": "col_00", "col_cod": "col_01"},
    # "faixa_remun_media_sm": {"estilo": "titulo_codigo", "aba": "faixas", "col_desc": "col_02", "col_cod": "col_03"},
}

# Campos numéricos e de competência: mesma ideia do CAGED (vírgula decimal,
# AAAAMM em texto), mas os nomes reais só serão conhecidos com o bronze em
# mãos. Deixado vazio de propósito — preencher depois de inspecionar o schema
# real, do jeito que foi feito para caged_old em silver_caged/mapeamento.py.
NUMERICOS: dict[str, str] = {}
DATAS_AAAAMM: list[str] = []

TABELAS_RAIS = ("rais_estab", "rais_vinc")
