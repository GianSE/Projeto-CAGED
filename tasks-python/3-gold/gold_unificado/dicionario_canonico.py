"""
Vocabulário único para CAGED e RAIS.

O PROBLEMA QUE ISTO RESOLVE
--------------------------
As duas bases codificam os mesmos conceitos de formas incompatíveis. Medido nos
dicionários oficiais publicados:

    código   Novo CAGED        RAIS
      1      Branca            INDÍGENA
      2      Preta             BRANCA
      4      Amarela           PRETA
      6      Não informada     AMARELA

Em sexo, `2` é Feminino na RAIS e não existe no Novo CAGED (que usa `3` para
Mulher). A RAIS segue a codificação do CAGED antigo, não a do Novo.

Um filtro de dashboard que mande "sexo = 1" para as duas bases devolve homens de
uma e uma mistura da outra — sem erro, sem aviso, com o gráfico parecendo certo.

O QUE É SEGURO E O QUE NÃO É
----------------------------
Conferido código a código contra os dois dicionários publicados:

    CBO 2002       2.571 códigos em comum, 2 divergências (só acento)  -> OK
    CNAE 2.0       1.160 em comum, zero divergências                   -> OK
    Município      5.658 em comum, zero divergências                   -> OK
    Escolaridade   8 de 11 divergem (grafia: "MEDIO INCOMP")           -> não
    Sexo, raça     significados trocados                               -> não

CBO, CNAE e município são taxonomias oficiais (MTE e IBGE), idênticas nas duas
bases — juntar por código é seguro. As demográficas são codificação interna de
cada sistema.

A SOLUÇÃO
---------
Para os conceitos que divergem, a ponte é a DESCRIÇÃO normalizada, não o
código: acento removido, maiúsculas, espaços colapsados. "MEDIO INCOMP" e
"Médio Incompleto" não colapsam sozinhos, então os casos que a normalização não
resolve entram em SINONIMOS, escritos à mão e conferidos.

O resultado é um rótulo canônico único por conceito, gravado JUNTO do fato na
gold. O dashboard filtra por esse rótulo e não precisa saber de qual base a
linha veio.
"""
import re
import unicodedata

# Conceitos que exigem ponte por descrição, com a coluna de cada base.
# (conceito, coluna no CAGED novo, coluna no CAGED antigo, coluna na RAIS)
CONCEITOS = {
    "sexo": ("sexo_descricao", "sexo_descricao", "sexo_trabalhador_descricao"),
    "raca_cor": ("racacor_descricao", "raca_cor_descricao", "raca_cor_descricao"),
    "escolaridade": ("graudeinstrucao_descricao", "grau_instrucao_descricao",
                     "escolaridade_apos_2005_descricao"),
}

# Rótulos canônicos por conceito, na ordem em que devem aparecer num filtro.
CANONICOS = {
    "sexo": ["Homem", "Mulher", "Não informado"],
    "raca_cor": ["Branca", "Preta", "Parda", "Amarela", "Indígena", "Não informado"],
    # Ordinal de propósito: escolaridade é escala, e um filtro que a mostre
    # fora de ordem convida à leitura errada.
    "escolaridade": [
        "Analfabeto", "Fundamental incompleto", "Fundamental completo",
        "Médio incompleto", "Médio completo", "Superior incompleto",
        "Superior completo", "Pós-graduação", "Não informado",
    ],
}

# Descrições que a normalização não junta sozinha, mapeadas à mão.
#
# A RAIS abrevia ("MEDIO INCOMP", "ATE 5.A INC") e o CAGED escreve por extenso.
# Também unifico faixas que uma base separa e a outra não: o CAGED antigo
# divide o fundamental em "Até 5ª", "5ª completo" e "6ª a 9ª"; agregar em
# "Fundamental incompleto" é o único jeito de a série de 2007 a 2026 ter uma
# categoria comparável de ponta a ponta.
SINONIMOS = {
    "sexo": {
        "MASCULINO": "Homem", "HOMEM": "Homem",
        "FEMININO": "Mulher", "MULHER": "Mulher",
        "IGNORADO": "Não informado", "NAO IDENTIFICADO": "Não informado",
        "NAO IDENT": "Não informado",
    },
    "raca_cor": {
        "BRANCA": "Branca", "PRETA": "Preta", "PARDA": "Parda",
        "AMARELA": "Amarela", "INDIGENA": "Indígena",
        "IGNORADO": "Não informado", "NAO INFORMADA": "Não informado",
        "NAO IDENTIFICADO": "Não informado", "NAO IDENT": "Não informado",
    },
    "escolaridade": {
        "ANALFABETO": "Analfabeto",
        "ATE 5.A INC": "Fundamental incompleto",
        "ATE 5A INCOMPLETO": "Fundamental incompleto",
        "5.A CO FUND": "Fundamental incompleto",
        "5A COMPLETO FUNDAMENTAL": "Fundamental incompleto",
        "6. A 9. FUND": "Fundamental incompleto",
        "6A A 9A FUNDAMENTAL": "Fundamental incompleto",
        "FUND COMPL": "Fundamental completo",
        "FUNDAMENTAL COMPLETO": "Fundamental completo",
        "MEDIO INCOMP": "Médio incompleto",
        "MEDIO INCOMPLETO": "Médio incompleto",
        "MEDIO COMPL": "Médio completo",
        "MEDIO COMPLETO": "Médio completo",
        "SUP. INCOMP": "Superior incompleto",
        "SUPERIOR INCOMPLETO": "Superior incompleto",
        "SUP. COMP": "Superior completo",
        "SUPERIOR COMPLETO": "Superior completo",
        "MESTRADO": "Pós-graduação",
        "DOUTORADO": "Pós-graduação",
        "POS-GRADUACAO COMPLETA": "Pós-graduação",
        "IGNORADO": "Não informado",
        "NAO IDENTIFICADO": "Não informado",
    },
}


def normalizar(texto: str | None) -> str:
    """Sem acento, maiúsculo, espaços colapsados — a chave de comparação."""
    if not texto:
        return ""
    t = unicodedata.normalize("NFKD", str(texto))
    t = "".join(c for c in t if not unicodedata.combining(c))
    return re.sub(r"\s+", " ", t).strip().upper()


def sql_canonico(conceito: str, coluna: str) -> str:
    """
    Expressão SQL que traduz a descrição daquela base para o rótulo canônico.

    Faz a normalização dentro do SQL para não depender de a base ter gravado
    com acento, caixa ou espaçamento iguais — que é justamente onde as duas
    divergem.
    """
    mapa = SINONIMOS[conceito]
    # `ª` e `º` precisam virar letra ANTES do strip_accents: ele trata acento
    # combinante, não indicador ordinal, e deixava "Até 5ª Incompleto" como
    # "ATE 5ª INCOMPLETO" — sem casar com nenhuma chave. Eram exatamente as
    # três faixas de fundamental do CAGED que ficavam de fora.
    limpo = f"replace(replace(trim({coluna}), 'ª', 'a'), 'º', 'o')"
    chave = f"regexp_replace(upper(strip_accents({limpo})), '\\s+', ' ', 'g')"
    casos = "\n".join(
        f"            WHEN {chave} = '{normalizar(k)}' THEN '{v}'"
        for k, v in mapa.items()
    )
    return f"""CASE
{casos}
            ELSE 'Não informado'
        END"""
