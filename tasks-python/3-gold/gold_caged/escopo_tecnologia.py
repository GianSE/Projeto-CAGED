"""
Definição do recorte de TECNOLOGIA — a decisão metodológica central do estudo.

Existem DUAS formas legítimas de recortar "mercado de tecnologia" nos
microdados do CAGED, e elas descrevem populações diferentes:

  SETOR DE TI (por CNAE, atividade do estabelecimento)
      Quem trabalha em empresa cuja atividade econômica é tecnologia.
      Inclui a recepcionista de uma software house.
      Exclui o desenvolvedor de um banco.

  PROFISSIONAIS DE TI (por CBO, ocupação da pessoa)
      Quem exerce ocupação de tecnologia, em qualquer setor.
      Inclui o desenvolvedor do banco.
      Exclui a recepcionista da software house.

No Brasil a maior parte dos profissionais de TI trabalha FORA do setor de TI
(bancos, varejo, indústria, governo). Por isso as duas lentes dão respostas
diferentes para "o mercado de tecnologia cresceu?" — e a diferença entre elas
é um resultado do trabalho, não um detalhe de implementação. A gold constrói
as duas, mais o mercado geral como linha de base: sem baseline, "TI cresceu
8%" não significa nada.

POR QUE POR FAMÍLIA DE CÓDIGO, E NÃO POR PALAVRA-CHAVE
------------------------------------------------------
Filtrar a descrição por palavras como "sistemas" ou "dados" traz
"Trabalhador na Operação de Sistemas de Irrigação por Aspersão" e "Montador
de Sistemas de Combustível de Aeronaves" — verificado no dicionário do MTE.
Os códigos abaixo foram conferidos um a um contra
bronze/dicionarios/novo_caged/{subclasse,cbo2002ocupacao}.parquet.

Ajuste as listas conforme a metodologia que você for defender; todo o resto
do pipeline segue a partir daqui.
"""

# --- LENTE 1: setor de TI, por CNAE (subclasse de 7 dígitos) ---------------
# Divisão 62 = Serviços de tecnologia da informação (núcleo do setor).
# Divisão 63 = Serviços de informação; só as subclasses de dados/internet
# entram. Agências de notícias (6391700) ficam de fora: é mídia, não TI.
CNAE_TI = {
    "6201500": "Desenvolvimento de programas sob encomenda",
    "6201501": "Desenvolvimento de programas sob encomenda",
    "6201502": "Web design",
    "6202300": "Software customizável",
    "6203100": "Software não-customizável",
    "6204000": "Consultoria em TI",
    "6209100": "Suporte técnico e manutenção em TI",
    "6311900": "Tratamento de dados e hospedagem",
    "6319400": "Portais e provedores de conteúdo",
    "6399200": "Outros serviços de informação",
}

# --- LENTE 2: profissionais de TI, por família CBO (4 primeiros dígitos) ---
# Família é o nível certo de agregação: agrupa as ocupações de uma mesma
# natureza sem depender de o MTE criar/renomear códigos específicos ao longo
# dos anos (o que de fato aconteceu — "Arquiteto de Soluções de TI" e
# "Analista de Testes de TI" são códigos recentes dentro da família 2124).
CBO_FAMILIAS_TI = {
    "1236": "Direção de serviços de informática",
    "1425": "Gerência de TI",
    "2031": "Pesquisa em computação",
    "2112": "Estatística e ciência de dados",
    "2122": "Engenharia em computação",
    "2123": "Administração de TI (BD, redes, SO)",
    "2124": "Análise de sistemas e desenvolvimento",
    "3171": "Programação e desenvolvimento (técnico)",
    "3172": "Operação e suporte ao usuário",
}

# Ocupações de TI que não caem nas famílias acima e valem incluir
# explicitamente, por código completo.
CBO_AVULSOS_TI = {
    "313220": "Técnico em manutenção de equipamentos de informática",
    "313305": "Técnico de comunicação de dados",
    "142135": "Encarregado de proteção de dados (DPO)",
}

# Códigos que a seleção por FAMÍLIA trazia e que não são de tecnologia.
#
# A família é boa regra porque captura códigos que o MTE cria com o tempo — mas
# duas famílias misturam TI com outra coisa. A 2031 é "pesquisa em ciências
# naturais e exatas": dela só a computação (2031-05) é TI; física, química,
# matemática e ciências da terra entravam de carona. A 3171 inclui o
# programador de máquina CNC (3171-15), que é chão de fábrica. Somavam 1.921
# admissões/ano no CAGED 2025, com 1 a 6% delas em empresa de TI.
#
# A exclusão vale só para a LENTE DE OCUPAÇÃO: quem exerce essas ocupações
# dentro de uma empresa de TI continua no recorte pela lente de setor.
CBO_EXCLUIDOS = {
    "203110": "Pesquisador em ciências da terra e meio ambiente",
    "203115": "Pesquisador em física",
    "203120": "Pesquisador em matemática",
    "203125": "Pesquisador em química",
    "317115": "Programador de máquinas-ferramenta com comando numérico (CNC)",
}

# Famílias que entraram DEPOIS da primeira construção da silver de TI.
#
# A 2112 (estatísticos) é onde analistas e cientistas de dados são registrados
# — 2.324 admissões/ano no CAGED 2025, 72% delas FORA de empresa de TI, com
# salário mediano de R$ 7.500. A inclusão foi decidida com o orientador.
#
# Guardar a lista separada é o que permite acrescentar só o que falta à silver
# já construída, em vez de reprocessar a série inteira: ver
# `auditoria.ajustar_recorte`.
CBO_FAMILIAS_ACRESCENTADAS = {"2112"}


# As colunas de CNAE e CBO mudam de nome entre as gerações do CAGED e na
# RAIS. O recorte é o mesmo; só o nome do campo muda.
COLUNAS_POR_TABELA = {
    "caged_mov": ("subclasse", "cbo2002ocupacao"),
    "caged_for": ("subclasse", "cbo2002ocupacao"),
    "caged_exc": ("subclasse", "cbo2002ocupacao"),
    "caged_old": ("cnae_20_subclas", "cbo_2002_ocupacao"),
    "caged_ajustes": ("cnae_20_subclas", "cbo_2002_ocupacao"),
    # RAIS: nomes conferidos no bronze. Cobertura verificada em 100% desde
    # 2007 para as duas colunas — é o que sustenta o recorte começar nesse ano
    # (antes disso vigoravam CNAE 1.0 e CBO 1994, taxonomias diferentes).
    "rais_vinc": ("cnae_20_subclasse", "cbo_ocupacao_2002"),
    # O estabelecimento não tem ocupação: uma empresa não exerce CBO. Aqui o
    # recorte é necessariamente só pelo setor.
    "rais_estab": ("cnae_20_subclasse", None),
}


def colunas_da_tabela(tabela: str, colunas_existentes: list[str]) -> tuple[str | None, str | None]:
    """
    Descobre como CNAE e CBO se chamam nesta tabela.

    Devolve None para o que não existir no arquivo: o CAGED antigo dos
    primeiros anos não traz CNAE 2.0 (só a 1.0), e nesse caso o recorte cai
    para a ocupação apenas — melhor perder a lente de setor num ano do que
    derrubar a ingestão inteira.
    """
    cnae, cbo = COLUNAS_POR_TABELA.get(tabela, ("subclasse", "cbo2002ocupacao"))
    return (cnae if cnae in colunas_existentes else None,
            cbo if cbo in colunas_existentes else None)


def sql_filtro_tecnologia(col_cnae: str | None, col_cbo: str | None) -> str | None:
    """
    Predicado do recorte: fica a movimentação que for de SETOR de TI OU de
    OCUPAÇÃO de TI. A união das duas lentes é o que preserva, dentro do
    recorte, tanto o dev do banco quanto a recepcionista da software house —
    permitindo comparar as duas leituras depois, na gold.

    None quando nenhuma das colunas existe: sem elas não há como recortar, e
    a decisão de o que fazer cabe a quem chamou.
    """
    partes = []
    if col_cnae:
        partes.append(sql_filtro_cnae(col_cnae))
    if col_cbo:
        partes.append(sql_filtro_cbo(col_cbo))
    if not partes:
        return None
    return "(" + " OR ".join(partes) + ")"


# --- ÁREAS DE ATUAÇÃO EM TI -----------------------------------------------
# Agrupamento das famílias CBO em áreas analiticamente comparáveis.
#
# Por que agrupar em vez de usar a família crua: medido no CAGED, duas
# famílias respondem por quase nada isoladamente — "Pesquisa em computação"
# (0,3%) e "Direção de serviços de informática" (0,1%). Como categoria
# própria virariam ruído em qualquer gráfico ou recorte; somadas ao seu
# parente natural, viram série legível.
#
# A ordem aqui não importa: cada família pertence a exatamente uma área.
AREAS_TI = {
    "Desenvolvimento": ["2124", "3171"],          # análise/desenvolvimento + programação
    "Suporte e Operação": ["3172"],               # helpdesk, operação
    "Infraestrutura e Dados": ["2123"],           # banco de dados, redes, SO
    "Engenharia e Pesquisa": ["2122", "2031"],    # engenharia de computação + pesquisa
    "Estatística e Ciência de Dados": ["2112"],   # estatísticos, analistas e cientistas de dados
    "Gestão e Direção": ["1425", "1236"],         # gerência e direção de TI
}

# Famílias -> área, invertido para consulta direta.
_FAMILIA_PARA_AREA = {fam: area for area, fams in AREAS_TI.items() for fam in fams}

# Os códigos avulsos também precisam de área.
AREA_DOS_AVULSOS = {
    "313220": "Suporte e Operação",   # manutenção de equipamentos
    "313305": "Infraestrutura e Dados",  # comunicação de dados
    "142135": "Gestão e Direção",     # encarregado de proteção de dados (DPO)
}

# Quem entrou no recorte só pela lente do SETOR (trabalha em empresa de TI mas
# não exerce ocupação de TI). São 39% do recorte no CAGED — não é resíduo, é
# uma categoria de pleno direito, e separá-la é o que permite responder
# "quanto do emprego em empresas de tecnologia não é trabalho técnico".
AREA_NAO_TI = "Outra ocupação (empresa de TI)"


def sql_area_ti(coluna: str = "cbo2002ocupacao") -> str:
    """
    Expressão SQL que classifica a ocupação numa área de TI.

    Normaliza o código antes de ler a família: o CAGED antigo grava com
    tamanho variável e às vezes com hífen, e sem o lpad os quatro primeiros
    dígitos não seriam a família.
    """
    codigo = sql_codigo_cbo(coluna)
    familia = f"substr({codigo}, 1, 4)"
    excluidos = ", ".join(f"'{c}'" for c in CBO_EXCLUIDOS)

    casos = "\n".join(
        f"            WHEN {familia} = '{fam}' THEN '{area}'"
        for fam, area in _FAMILIA_PARA_AREA.items()
    )
    casos_avulsos = "\n".join(
        f"            WHEN {codigo} = '{cod}' THEN '{area}'"
        for cod, area in AREA_DOS_AVULSOS.items()
    )

    # Excluído vem PRIMEIRO: sem isso o pesquisador de física cairia na área
    # "Engenharia e Pesquisa" pela família 2031 antes de chegar ao ELSE.
    return f"""CASE
            WHEN {codigo} IN ({excluidos}) THEN '{AREA_NAO_TI}'
{casos_avulsos}
{casos}
            ELSE '{AREA_NAO_TI}'
        END"""


def sql_filtro_cnae(coluna: str = "subclasse") -> str:
    """Predicado SQL do setor de TI."""
    lista = ", ".join(f"'{c}'" for c in CNAE_TI)
    return f"{coluna} IN ({lista})"


def sql_codigo_cbo(coluna: str = "cbo2002ocupacao") -> str:
    """
    O CBO normalizado em 6 dígitos: sem hífen, sem espaço, com zero à esquerda.

    Uma regra só para o filtro e para a classificação por área. Antes cada um
    normalizava de um jeito — a área removia hífen, o filtro não —, e um código
    gravado como "3132-20" passava pela área e escapava do filtro.
    """
    return f"lpad(regexp_replace(trim({coluna}), '[^0-9]', '', 'g'), 6, '0')"


def sql_filtro_cbo(coluna: str = "cbo2002ocupacao") -> str:
    """
    Predicado SQL dos profissionais de TI.

    Compara os 4 primeiros dígitos com a família, soma os códigos avulsos e
    tira os excluídos. O zero à esquerda importa: "21110" é o sargento da
    Polícia Militar (021110), não a família 2111 dos matemáticos.
    """
    familias = ", ".join(f"'{f}'" for f in CBO_FAMILIAS_TI)
    avulsos = ", ".join(f"'{c}'" for c in CBO_AVULSOS_TI)
    excluidos = ", ".join(f"'{c}'" for c in CBO_EXCLUIDOS)
    codigo = sql_codigo_cbo(coluna)
    return (f"((substr({codigo}, 1, 4) IN ({familias}) OR {codigo} IN ({avulsos})) "
            f"AND {codigo} NOT IN ({excluidos}))")


def sql_complemento_cbo(col_cnae: str | None, col_cbo: str) -> str:
    """
    As linhas que ENTRAM no recorte com as famílias acrescentadas.

    São as da família nova que ainda não estavam lá: as que já trabalham em
    empresa de TI entraram desde o começo pela lente de setor, e trazê-las de
    novo as duplicaria.
    """
    codigo = sql_codigo_cbo(col_cbo)
    novas = ", ".join(f"'{f}'" for f in CBO_FAMILIAS_ACRESCENTADAS)
    fora_do_setor = f"NOT coalesce({sql_filtro_cnae(col_cnae)}, false)" if col_cnae else "true"
    return f"(substr({codigo}, 1, 4) IN ({novas}) AND {fora_do_setor})"


def sql_saida_cbo(col_cnae: str | None, col_cbo: str) -> str:
    """
    As linhas que SAEM do recorte com os códigos excluídos.

    Só as que estavam lá exclusivamente pela ocupação: dentro de empresa de TI,
    a lente de setor as mantém.
    """
    codigo = sql_codigo_cbo(col_cbo)
    excluidos = ", ".join(f"'{c}'" for c in CBO_EXCLUIDOS)
    fora_do_setor = f"NOT coalesce({sql_filtro_cnae(col_cnae)}, false)" if col_cnae else "true"
    return f"({codigo} IN ({excluidos}) AND {fora_do_setor})"


def sql_classificacao(col_cnae: str = "subclasse",
                      col_cbo: str | None = "cbo2002ocupacao") -> str:
    """
    Expressão que rotula cada registro nas duas lentes de uma vez.

    Guardar os dois rótulos na mesma tabela permite responder, sem novo
    processamento, a pergunta mais interessante: quantos profissionais de TI
    estão fora do setor de TI.

    Os nomes de coluna são parâmetro porque a mesma classificação vale para o
    CAGED e para a RAIS, que chamam CNAE e CBO de outro jeito. `col_cbo=None`
    atende o estabelecimento da RAIS, que não tem ocupação — uma empresa não
    exerce CBO — e nesse caso a lente de ocupação sai sempre falsa em vez de
    quebrar a consulta.
    """
    ocupacao = (f"CASE WHEN {sql_filtro_cbo(col_cbo)} THEN true ELSE false END"
                if col_cbo else "false")
    return f"""
        CASE WHEN {sql_filtro_cnae(col_cnae)} THEN true ELSE false END AS setor_ti,
        {ocupacao} AS ocupacao_ti
    """
