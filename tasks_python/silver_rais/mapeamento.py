"""
Configuração de como o bronze da RAIS vira silver.

POR QUE AQUI É TUDO EXPLÍCITO (e no CAGED não é)
------------------------------------------------
No Novo CAGED o nome da coluna do bronze é igual ao nome da aba do dicionário,
então o casamento automático resolve quase tudo. Na RAIS não: a coluna se chama
`sexo_trabalhador` e o campo do layout se chama `SEXO`; `motivo_desligamento`
aparece como `CAUSA DESLI ou Motivo de Desligamento`. Das 52 colunas do
`rais_vinc`, exatamente UMA (`municipio`) casava sozinha.

Pior: o dicionário da RAIS está espalhado por 21 planilhas de layout, uma por
período, e várias têm abas de mesmo nome. Deixar o casamento automático varrer
tudo fundiria tabelas de código de anos diferentes. Por isso cada entrada aqui
diz de QUAL planilha vem o de/para — o parâmetro `planilha`.

DE QUAL ANO É O DICIONÁRIO
--------------------------
Vínculos: layout de 2020. Estabelecimentos: layout de 2018/2019 (o mais
recente publicado). O recorte do estudo começa em 2007, e nesse intervalo as
taxonomias usadas são estáveis — CBO 2002, CNAE 2.0 e a escolaridade "após
2005" valem para a série inteira. É justamente por isso que o estudo começa em
2007: antes disso vigoravam CNAE 1.0 e CBO 1994, e um dicionário só não
serviria.

O LAYOUT DA RAIS TEM O MESMO FORMATO DO CAGEDEST
------------------------------------------------
A aba `raisd_layout` traz `Nome | Descrição | Tamanho | Categorias | Valor na
Fonte`, com o Nome preenchido só na primeira linha de cada campo — idêntico ao
`cagestid_layout` do CAGED antigo, nas mesmas posições de coluna. Por isso o
estilo "cagestid" que já existia traduz a maior parte da RAIS sem código novo.
"""

NAMESPACE_DICIONARIO = "rais_layouts"

# Planilhas de onde sai o de/para de cada tabela.
PLANILHA_VINC = "rais_vinculos_layout2020"
PLANILHA_ESTAB = "rais_estabelecimento_layout2018e2019"

# Abas que são texto de apoio (o layout inteiro), não uma tabela código ->
# descrição. Continuam listadas porque o motor genérico as leria como dicionário.
ABAS_NAO_TRADUZIVEIS = {"plan1", "rais_layout", "raisd_layout", "estb_layout"}


def _cagestid(campo: str, planilha: str) -> dict:
    """Campo cujo de/para está embutido na aba de layout."""
    return {"estilo": "cagestid", "aba": _ABA_LAYOUT[planilha], "campo": campo,
            "planilha": planilha}


def _colon(aba: str, planilha: str) -> dict:
    """Aba dedicada no formato "codigo:descrição" numa coluna só."""
    return {"estilo": "colon", "aba": aba, "planilha": planilha}


def _par(aba: str, planilha: str, cod: str, desc: str) -> dict:
    """Aba dedicada com código e descrição em colunas separadas."""
    return {"estilo": "titulo_codigo", "aba": aba, "col_cod": cod,
            "col_desc": desc, "planilha": planilha}


_ABA_LAYOUT = {PLANILHA_VINC: "raisd_layout", PLANILHA_ESTAB: "estb_layout"}

_V = PLANILHA_VINC
_E = PLANILHA_ESTAB

# --- VÍNCULOS --------------------------------------------------------------
_MAPA_VINC = {
    # Códigos embutidos na aba de layout.
    "causa_afastamento_1": _cagestid("CAUS AFAST 1", _V),
    "causa_afastamento_2": _cagestid("CAUS AFAST 2", _V),
    "causa_afastamento_3": _cagestid("CAUS AFAST 3", _V),
    "motivo_desligamento": _cagestid("CAUSA DESLI ou Motivo de Desligamento", _V),
    "vinculo_ativo_3112": _cagestid("EMP EM 31/12", _V),
    "ind_cei_vinculado": _cagestid("IND DE CEI VINC", _V),
    "ind_simples": _cagestid("IND SIMPLES", _V),
    "nacionalidade": _cagestid("NACIONALIDAD", _V),
    "natureza_juridica": _cagestid("NAT JURIDICA", _V),
    "ind_portador_defic": _cagestid("PORT DEFIC", _V),
    "raca_cor": _cagestid("RACA_COR", _V),
    "sexo_trabalhador": _cagestid("SEXO", _V),
    "tamanho_estabelecimento": _cagestid("TAMESTAB", _V),
    "tipo_admissao": _cagestid("TIPO ADM", _V),
    "tipo_estab": _cagestid("TIPO ESTBL", _V),
    "tipo_estab_1": _cagestid("TIPO ESTBL", _V),
    "tipo_defic": _cagestid("TP DEFIC", _V),
    "tipo_vinculo": _cagestid("TP VINCULO", _V),

    # Abas dedicadas, formato "codigo:descrição".
    "municipio": _colon("municipio", _V),
    "mun_trab": _colon("municipio", _V),
    "cbo_ocupacao_2002": _colon("ocupacao", _V),
    "cnae_20_subclasse": _colon("subclasse_2_0", _V),
    "cnae_95_classe": _colon("classe_1_0_ou_95", _V),

    # Abas dedicadas, código e descrição em colunas separadas.
    "escolaridade_apos_2005": _par("escolaridade_ou_g_instrucao", _V, "col_02", "col_00"),
    "mes_admissao": _par("mes_adm_ou_desl", _V, "col_00", "col_01"),
    "mes_desligamento": _par("mes_adm_ou_desl", _V, "col_00", "col_01"),
    "regioes_adm_df": _par("reg_adm_df", _V, "col_00", "col_01"),
    "distritos_sp": _par("distrito_sp", _V, "col_00", "col_01"),
    "bairros_sp": _par("bairro_sp", _V, "col_02", "col_01"),
    "bairros_rj": _par("bairro_rj", _V, "col_02", "col_01"),
    "bairros_fortaleza": _par("bairro_fort", _V, "col_02", "col_01"),

    # A aba "faixas" empilha DOIS de/para lado a lado na mesma planilha:
    # col_00/col_01 = faixa etária, col_02/col_03 = faixa de remuneração média.
    "faixa_etaria": _par("faixas", _V, "col_00", "col_01"),
    "faixa_remun_media_sm": _par("faixas", _V, "col_02", "col_03"),

    # SEM DICIONÁRIO PUBLICADO: faixa_hora_contrat, faixa_remun_dezem_sm e
    # faixa_tempo_emprego aparecem no layout como campo, mas o MTE não publica
    # a lista de faixas de nenhum deles em aba alguma das 21 planilhas.
    # Ficam com o código cru. Não é perda relevante: as três têm equivalente
    # contínuo na mesma linha (qtd_hora_contr, vl_remun_dezembro_sm,
    # tempo_emprego), que é informação melhor do que a faixa.
    #
    # cnae_20_classe também fica sem de/para — o MTE só publica a aba da
    # subclasse. Como a classe são os 5 primeiros dígitos da subclasse, que
    # ESTÁ traduzida, nada se perde.
}

# --- ESTABELECIMENTOS ------------------------------------------------------
# O layout do estabelecimento grafa os nomes de campo de outro jeito
# ("Natureza Jurídica" contra "NAT JURIDICA" no de vínculos) — conferido, não
# suposto. Por isso um mapa separado em vez de reaproveitar o de cima.
_MAPA_ESTAB = {
    "natureza_juridica": _cagestid("Natureza Jurídica", _E),
    "tamanho_estabelecimento": _cagestid("Tamanho Estabelecimento", _E),
    "tipo_estab": _cagestid("Tipo Estab", _E),
    "tipo_estab_1": _cagestid("Tipo Estab", _E),
    "ind_rais_negativa": _cagestid("Ind Rais Negativa", _E),
    "ind_estab_participa_pat": _cagestid("Ind Estab Participa PAT", _E),
    "ind_simples": _cagestid("Ind Simples", _E),
    "ind_cei_vinculado": _cagestid("Ind CEI Vinculado", _E),
    "ind_atividade_ano": _cagestid("Ind Atividade Ano", _E),

    "municipio": _colon("municipio", _E),
    "cnae_20_subclasse": _colon("subclasse_2_0", _E),
    "cnae_95_classe": _colon("classe_1_0_ou_95", _E),

    # ATENÇÃO: as abas dedicadas do layout de ESTABELECIMENTO têm três colunas
    # (Categorias | Descrição | Valor na Fonte), com o código na TERCEIRA — ao
    # contrário das mesmas abas no layout de vínculos, onde o código é a
    # primeira coluna. Conferido aba a aba: usar aqui o formato de vínculos faz
    # o "código" virar a abreviação ("CANDANGOLAND" em vez de 1), e aí nenhuma
    # linha do fato casa.
    "regioes_adm_df": _par("reg_adm_df", _E, "col_02", "col_01"),
    "distritos_sp": _par("distrito_sp", _E, "col_02", "col_01"),
    "bairros_sp": _par("bairro_sp", _E, "col_02", "col_01"),
    "bairros_rj": _par("bairro_rj", _E, "col_02", "col_01"),
    "bairros_fortaleza": _par("bairro_fort", _E, "col_02", "col_01"),
}

MAPA_MANUAL: dict[str, dict[str, dict]] = {
    "rais_vinc": _MAPA_VINC,
    "rais_estab": _MAPA_ESTAB,
}

# Numéricos gravados como texto no bronze, com vírgula decimal (padrão BR).
# O construtor troca a vírgula por ponto antes do cast.
_NUMERICOS_COMUNS = {
    "qtd_vinculos_clt": "INTEGER",
    "qtd_vinculos_ativos": "INTEGER",
    "qtd_vinculos_estatutarios": "INTEGER",
}

NUMERICOS: dict[str, str] = {
    **_NUMERICOS_COMUNS,
    "idade": "SMALLINT",
    "qtd_hora_contr": "SMALLINT",
    "qtd_dias_afastamento": "SMALLINT",
    "tempo_emprego": "DOUBLE",
    "vl_remun_media_nom": "DOUBLE",
    "vl_remun_media_sm": "DOUBLE",
    "vl_remun_dezembro_nom": "DOUBLE",
    "vl_remun_dezembro_sm": "DOUBLE",
}

# A RAIS não tem competência AAAAMM: cada arquivo é um ano inteiro, e o mês de
# admissão/desligamento vem como código de 0 a 12 (traduzido acima), não como
# data. Nada a converter aqui.
DATAS_AAAAMM: list[str] = []

TABELAS_RAIS = ("rais_estab", "rais_vinc")
