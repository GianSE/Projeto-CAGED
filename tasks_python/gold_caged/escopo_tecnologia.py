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


def sql_filtro_cnae(coluna: str = "subclasse") -> str:
    """Predicado SQL do setor de TI."""
    lista = ", ".join(f"'{c}'" for c in CNAE_TI)
    return f"{coluna} IN ({lista})"


def sql_filtro_cbo(coluna: str = "cbo2002ocupacao") -> str:
    """
    Predicado SQL dos profissionais de TI.

    Compara os 4 primeiros dígitos com a família; o CBO no CAGED vem com 6
    dígitos, e alguns registros trazem zero à esquerda, daí o lpad.
    """
    familias = ", ".join(f"'{f}'" for f in CBO_FAMILIAS_TI)
    avulsos = ", ".join(f"'{c}'" for c in CBO_AVULSOS_TI)
    codigo = f"lpad(trim({coluna}), 6, '0')"
    return f"(substr({codigo}, 1, 4) IN ({familias}) OR {codigo} IN ({avulsos}))"


def sql_classificacao() -> str:
    """
    Expressão que rotula cada movimentação nas duas lentes de uma vez.

    Guardar os dois rótulos na mesma tabela permite responder, sem novo
    processamento, a pergunta mais interessante: quantos profissionais de TI
    estão fora do setor de TI.
    """
    return f"""
        CASE WHEN {sql_filtro_cnae()} THEN true ELSE false END AS setor_ti,
        CASE WHEN {sql_filtro_cbo()} THEN true ELSE false END AS ocupacao_ti
    """
