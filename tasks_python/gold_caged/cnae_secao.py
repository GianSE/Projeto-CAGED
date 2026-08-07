"""
Derivação da seção CNAE 2.0 a partir do código da subclasse.

POR QUE ISTO EXISTE
-------------------
As duas gerações do CAGED trazem a atividade econômica de formas diferentes:
o Novo CAGED já entrega a seção pronta (`secao_descricao`), o CAGED antigo
não — ele traz o subsetor IBGE, que é outra taxonomia e não é comparável.

Mas o CAGED antigo TAMBÉM traz `cnae_20_subclas`, com os mesmos códigos
CNAE 2.0 do Novo CAGED (verificado: 6201501, 6209100, 6204000... aparecem
idênticos nas duas bases). Como a seção é determinada pela divisão — os dois
primeiros dígitos do código —, dá para derivá-la e obter a série setorial
contínua de 2007 a 2026, em vez de restringir a análise de setor ao período
recente.

A tabela abaixo é a estrutura oficial da CNAE 2.0 (IBGE/CONCLA): cada seção
cobre uma faixa de divisões.
"""

# (divisão inicial, divisão final, letra da seção, nome)
FAIXAS_SECAO = [
    # Grafia idêntica à do dicionário do MTE, inclusive onde ela destoa da
    # norma ("AqÜIcultura", vírgula no lugar do ponto-e-vírgula em Comércio).
    # Corrigir a ortografia faria a seção derivada virar um grupo separado da
    # oficial nas consultas que unem as duas gerações.
    (1, 3, "A", "Agricultura, Pecuária, Produção Florestal, Pesca e AqÜIcultura"),
    (5, 9, "B", "Indústrias Extrativas"),
    (10, 33, "C", "Indústrias de Transformação"),
    (35, 35, "D", "Eletricidade e Gás"),
    (36, 39, "E", "Água, Esgoto, Atividades de Gestão de Resíduos e Descontaminação"),
    (41, 43, "F", "Construção"),
    (45, 47, "G", "Comércio, Reparação de Veículos Automotores e Motocicletas"),
    (49, 53, "H", "Transporte, Armazenagem e Correio"),
    (55, 56, "I", "Alojamento e Alimentação"),
    (58, 63, "J", "Informação e Comunicação"),
    (64, 66, "K", "Atividades Financeiras, de Seguros e Serviços Relacionados"),
    (68, 68, "L", "Atividades Imobiliárias"),
    (69, 75, "M", "Atividades Profissionais, Científicas e Técnicas"),
    (77, 82, "N", "Atividades Administrativas e Serviços Complementares"),
    (84, 84, "O", "Administração Pública, Defesa e Seguridade Social"),
    (85, 85, "P", "Educação"),
    (86, 88, "Q", "Saúde Humana e Serviços Sociais"),
    (90, 93, "R", "Artes, Cultura, Esporte e Recreação"),
    (94, 96, "S", "Outras Atividades de Serviços"),
    (97, 97, "T", "Serviços Domésticos"),
    (99, 99, "U", "Organismos Internacionais e Outras Instituições Extraterritoriais"),
]


def sql_secao(coluna: str) -> str:
    """
    Expressão SQL que devolve o nome da seção a partir do código da subclasse.

    Normaliza antes de ler a divisão: o CAGED antigo grava alguns códigos com
    hífen ("00000-1") e o comprimento varia, então extrai só os dígitos e
    completa com zero à esquerda para os dois primeiros serem sempre a
    divisão. Código não informado (divisão 0) cai em NULL em vez de virar
    uma seção errada.
    """
    digitos = f"regexp_replace(trim({coluna}), '[^0-9]', '', 'g')"
    divisao = f"try_cast(substr(lpad({digitos}, 7, '0'), 1, 2) AS INTEGER)"

    casos = "\n".join(
        f"            WHEN {divisao} BETWEEN {ini} AND {fim} "
        f"THEN '{nome.replace(chr(39), chr(39) * 2)}'"
        for ini, fim, _letra, nome in FAIXAS_SECAO
    )
    return f"""CASE
{casos}
            ELSE NULL
        END"""
