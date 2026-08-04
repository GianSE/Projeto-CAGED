"""
Configuração de como cada tabela bronze do CAGED vira silver.

Duas gerações de dados, dois dicionários diferentes:

  Novo CAGED (caged_mov, caged_for, caged_exc; 2020+)
    Dicionário limpo (bronze/dicionarios/novo_caged/*), 2 colunas
    (Código/Descrição), e o nome da coluna no bronze é IDÊNTICO ao nome da
    aba — dá para casar automaticamente por nome, sem mapa manual (ver
    `colunas_traduziveis` em construir_silver.py).

  CAGED antigo / ajustes (caged_old, caged_ajustes; 2002-2019)
    Dicionário único no formato "código:descrição" numa coluna só
    (bronze/dicionarios/caged/*), e os nomes de coluna do bronze não batem
    com o nome da aba (cnae_20_subclas -> aba "subclasse") — precisa de mapa
    explícito, montado abaixo a partir da inspeção real do bronze.

    Nem todo código do CAGED antigo tem dicionário publicado em formato
    tabular (grau_instrucao, sexo, raca_cor, tipo_mov_desagregado, ... só
    aparecem dentro da planilha de layout, em texto livre). Esses ficam
    codificados na silver por enquanto — documentado no README, não é
    esquecimento.
"""

BUCKET_SILVER_NAMESPACE_NOVO = "novo_caged"
BUCKET_DICT_NAMESPACE_ANTIGO = "caged"

# Abas do dicionário do Novo CAGED que NÃO são tabela de código (são texto de
# apoio) — não tentar casar com nenhuma coluna mesmo que o nome bata.
ABAS_NAO_TRADUZIVEIS = {"layout"}

# --- CAGED antigo / ajustes: coluna bronze -> fonte do dicionário ---------
#
# Três fontes dentro do mesmo namespace "caged":
#   "colon"        -> uma aba dedicada, código:descrição (bronze/dicionarios/caged/{aba}.parquet)
#   "colon_coluna" -> uma coluna específica da aba "outros" (11 listas regionais empilhadas)
#   "cagestid"     -> um campo da aba "cagestid_layout" (códigos curtos embutidos no layout;
#                     ver aviso de compatibilidade em dicionarios.py — os códigos NÃO
#                     são os mesmos do Novo CAGED)
from silver_caged.dicionarios import CAMPOS_CAGESTID, COLUNAS_ABA_OUTROS

MAPA_CAGED_ANTIGO = {
    # --- geografia / atividade econômica: aba própria ---
    "municipio": {"estilo": "colon", "aba": "municipio"},
    "cbo_2002_ocupacao": {"estilo": "colon", "aba": "cbo2002"},
    "cbo_94_ocupacao": {"estilo": "colon", "aba": "cbo_94"},
    "cnae_10_classe": {"estilo": "colon", "aba": "classe_10"},
    "cnae_20_classe": {"estilo": "colon", "aba": "classe_20"},
    "cnae_20_subclas": {"estilo": "colon", "aba": "subclasse"},
    "bairros_sp": {"estilo": "colon", "aba": "bairro_sp"},
    "bairros_fortaleza": {"estilo": "colon", "aba": "bairro_fort"},
    "bairros_rj": {"estilo": "colon", "aba": "bairro_rj"},
    "distritos_sp": {"estilo": "colon", "aba": "distrito_sp"},
    "regioes_adm_df": {"estilo": "colon", "aba": "reg_adm_df"},
}

# --- geografia regional: as 11 colunas da aba "outros" ---------------------
for _coluna, _campo_bronze in COLUNAS_ABA_OUTROS.items():
    MAPA_CAGED_ANTIGO[_campo_bronze] = {"estilo": "colon_coluna", "aba": "outros", "coluna": _coluna}

# --- códigos curtos embutidos no layout (sexo, raça/cor, grau instr, ...) --
for _campo_layout, _campo_bronze in CAMPOS_CAGESTID.items():
    MAPA_CAGED_ANTIGO[_campo_bronze] = {
        "estilo": "cagestid", "aba": "cagestid_layout", "campo": _campo_layout,
    }

# --- Campos numéricos: vêm com vírgula decimal e zero à esquerda ----------
# CAST direto falha ("000005,10"); precisa REPLACE(',', '.') antes.
NUMERICOS_NOVO_CAGED = {
    "idade": "SMALLINT",
    "horascontratuais": "DOUBLE",
    "salario": "DOUBLE",
    "valorsalariofixo": "DOUBLE",
    "saldomovimentacao": "TINYINT",
}

NUMERICOS_CAGED_ANTIGO = {
    "idade": "SMALLINT",
    "qtd_hora_contrat": "DOUBLE",
    "salario_mensal": "DOUBLE",
    "tempo_emprego": "DOUBLE",
    "saldo_mov": "TINYINT",
}

# --- Campos de competência (AAAAMM em texto) -> DATE (primeiro dia do mês) -
DATAS_AAAAMM_NOVO_CAGED = ["competenciamov", "competenciadec"]
DATAS_AAAAMM_CAGED_ANTIGO = [
    "competencia_movimentacao",
    "competencia_declarada",
    "competencia_dec",
]

# Tabelas de cada geração
TABELAS_NOVO_CAGED = ("caged_mov", "caged_for", "caged_exc")
TABELAS_CAGED_ANTIGO = ("caged_old", "caged_ajustes")
TODAS_TABELAS = TABELAS_NOVO_CAGED + TABELAS_CAGED_ANTIGO


def geracao(tabela: str) -> str:
    if tabela in TABELAS_NOVO_CAGED:
        return "novo"
    if tabela in TABELAS_CAGED_ANTIGO:
        return "antigo"
    raise ValueError(f"Tabela desconhecida para a silver do CAGED: {tabela}")
