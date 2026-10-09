"""
Consultas da RAIS para o dashboard — lendo a camada gold.

POR QUE ESTA COM GOLD E O CAGED SEM
-----------------------------------
`dados.py` consulta a silver do CAGED direto, e explica por quê: 4,5 milhões
de movimentações agregam em menos de um segundo, então uma camada intermediária
só acrescentaria um passo de build.

A RAIS não cabe nessa conta. São 23,8 milhões de vínculos, e as métricas de
estoque precisam de `median()` — que é ordenação, não soma, e não se resolve em
streaming. Medido: o agregado anual leva ~55 segundos sobre a silver. Por clique
de filtro isso é inaceitável; pré-calculado, a mesma resposta vem de uma tabela
de 57 linhas.

É exatamente o cenário que o comentário de `dados.py` previa: "se a silver
voltar a crescer muito (mercado completo, ou RAIS inteira), o caminho é
reintroduzir a gold".

O QUE A RAIS ACRESCENTA
-----------------------
O CAGED conta FLUXO — quantos empregos foram criados. A RAIS conta ESTOQUE —
quantos existem em 31/12. As duas leituras juntas evitam os dois erros comuns:
concluir que o mercado é grande porque cresceu, ou que está parado porque é
grande.

Além disso a RAIS traz remuneração em salários mínimos (comparável ao longo de
19 anos sem escolher deflator), tempo de emprego e cadastro de estabelecimentos
— nada disso existe no CAGED.
"""
import pandas as pd
import streamlit as st

from dashboard.dados import _consultar, conectar  # noqa: F401  (mesma conexão e cache)

# A raiz da gold vem de `fonte_gold`, que decide entre MinIO local e a cópia
# publicada. Reaproveitar `DADOS_URL_BASE` seria errado de qualquer forma: ela
# aponta para o dataset do CAGED, e os agregados da RAIS não estão lá — o
# resultado era 404 disfarçado de "gold ainda não construída", mandando
# reconstruir o que já existia.
from dashboard.fonte_gold import caminho as _caminho  # noqa: E402


def _caminho_gold(nome: str) -> str:
    return _caminho(nome)


# O recorte de TI é a união das duas lentes — setor OU ocupação —, igual ao
# CAGED. Manter os dois rótulos na gold permite ligar e desligar cada lente no
# dashboard sem reprocessar.
TI = "(setor_ti OR ocupacao_ti)"


@st.cache_resource
def tem_dados() -> bool:
    """A aba da RAIS só aparece se a gold existir — sem ela, nada a mostrar."""
    try:
        conectar().execute(
            f"SELECT 1 FROM read_parquet('{_caminho_gold('rais_estoque_anual')}') LIMIT 1"
        ).fetchone()
        return True
    except Exception:
        return False


def _media_ponderada(coluna: str, peso: str = "estoque_3112") -> str:
    """
    Média das médias tem que ser PONDERADA pelo estoque.

    Somar `avg` de grupos com tamanhos diferentes e dividir pela quantidade de
    grupos daria o mesmo peso a São Paulo e ao Acre. O erro é silencioso: o
    número sai plausível e está errado.
    """
    return (f"round(sum({peso} * {coluna}) / nullif(sum({peso}), 0), 2)")


def estoque_anual(lente: str = "ti") -> pd.DataFrame:
    """
    Estoque de vínculos em 31/12, ano a ano.

    `lente` escolhe o recorte: 'ti' (setor ou ocupação), 'setor', 'ocupacao'.
    """
    onde = {"ti": TI, "setor": "setor_ti", "ocupacao": "ocupacao_ti"}[lente]
    return _consultar(f"""
        SELECT ano,
               sum(estoque_3112)      AS estoque,
               sum(vinculos_no_ano)   AS vinculos_no_ano,
               sum(desligados_no_ano) AS desligados,
               sum(remun_implausivel) AS remun_descartada,
               {_media_ponderada('remuneracao_sm')}         AS remuneracao_sm,
               {_media_ponderada('remuneracao_sm_mediana')} AS remuneracao_sm_mediana,
               {_media_ponderada('remuneracao_nominal')}    AS remuneracao_nominal,
               {_media_ponderada('tempo_emprego_meses')}    AS tempo_emprego_meses,
               {_media_ponderada('idade_media')}            AS idade_media,
               {_media_ponderada('horas_semanais')}         AS horas_semanais
        FROM read_parquet('{_caminho_gold('rais_estoque_anual')}')
        WHERE {onde}
        GROUP BY 1 ORDER BY 1
    """)


def estoque_por_uf() -> pd.DataFrame:
    return _consultar(f"""
        SELECT ano, uf,
               sum(estoque_3112) AS estoque,
               {_media_ponderada('remuneracao_sm')}         AS remuneracao_sm,
               {_media_ponderada('remuneracao_sm_mediana')} AS remuneracao_sm_mediana
        FROM read_parquet('{_caminho_gold('rais_estoque_uf')}')
        WHERE {TI} AND uf IS NOT NULL AND uf <> ''
        GROUP BY 1, 2 ORDER BY 1, 3 DESC
    """)


def estoque_por_area() -> pd.DataFrame:
    """Estoque por área de atuação — só quem tem ocupação de TI."""
    return _consultar(f"""
        SELECT ano, area_ti AS area,
               sum(estoque_3112) AS estoque,
               {_media_ponderada('remuneracao_sm')}         AS remuneracao_sm,
               {_media_ponderada('remuneracao_sm_mediana')} AS remuneracao_sm_mediana,
               {_media_ponderada('tempo_emprego_meses')}    AS tempo_emprego_meses
        FROM read_parquet('{_caminho_gold('rais_estoque_area')}')
        WHERE area_ti IS NOT NULL
        GROUP BY 1, 2 ORDER BY 1, 3 DESC
    """)


def perfil(dimensao: str) -> pd.DataFrame:
    """
    Estoque e remuneração por sexo, raça/cor ou escolaridade.

    Uma função para as três porque a pergunta é a mesma — quem está no estoque
    e quanto recebe — e a única diferença é a coluna do GROUP BY.
    """
    coluna = {"sexo": "sexo", "raca": "raca_cor", "escolaridade": "escolaridade"}[dimensao]
    return _consultar(f"""
        SELECT ano, "{coluna}" AS categoria,
               sum(estoque_3112) AS estoque,
               {_media_ponderada('remuneracao_sm')}         AS remuneracao_sm,
               {_media_ponderada('remuneracao_sm_mediana')} AS remuneracao_sm_mediana,
               {_media_ponderada('tempo_emprego_meses')}    AS tempo_emprego_meses
        FROM read_parquet('{_caminho_gold('rais_estoque_perfil')}')
        WHERE {TI} AND "{coluna}" IS NOT NULL
        GROUP BY 1, 2 ORDER BY 1, 3 DESC
    """)


def hiato_por_escolaridade(ano: int) -> pd.DataFrame:
    """
    Remuneração por sexo DENTRO de cada nível de escolaridade.

    É a comparação que separa "ganham menos porque estudaram menos" de "ganham
    menos no mesmo nível de formação". Sem controlar por escolaridade, a
    diferença bruta mistura as duas explicações e não sustenta nenhuma.
    """
    return _consultar(f"""
        SELECT escolaridade, sexo,
               sum(estoque_3112) AS estoque,
               {_media_ponderada('remuneracao_sm_mediana')} AS remuneracao_sm_mediana
        FROM read_parquet('{_caminho_gold('rais_estoque_perfil')}')
        WHERE {TI} AND ano = {int(ano)}
          AND escolaridade IS NOT NULL AND sexo IS NOT NULL
        GROUP BY 1, 2
        HAVING sum(estoque_3112) >= 500
        ORDER BY 3 DESC
    """)


def remuneracao_por_ocupacao(ano: int, limite: int = 20) -> pd.DataFrame:
    return _consultar(f"""
        SELECT ocupacao,
               sum(estoque_3112) AS estoque,
               {_media_ponderada('remuneracao_sm_mediana')} AS remuneracao_sm_mediana,
               {_media_ponderada('remuneracao_nominal')}    AS remuneracao_nominal,
               {_media_ponderada('tempo_emprego_meses')}    AS tempo_emprego_meses
        FROM read_parquet('{_caminho_gold('rais_remuneracao_ocupacao')}')
        WHERE ano = {int(ano)}
        GROUP BY 1 ORDER BY 2 DESC LIMIT {int(limite)}
    """)


def municipios(ano: int, limite: int = 20) -> pd.DataFrame:
    return _consultar(f"""
        SELECT municipio, uf,
               sum(estoque_3112) AS estoque,
               {_media_ponderada('remuneracao_sm_mediana')} AS remuneracao_sm_mediana
        FROM read_parquet('{_caminho_gold('rais_estoque_municipio')}')
        WHERE ano = {int(ano)} AND municipio IS NOT NULL
        GROUP BY 1, 2 ORDER BY 3 DESC LIMIT {int(limite)}
    """)


def lentes() -> pd.DataFrame:
    """
    Cruzamento das duas lentes sobre o ESTOQUE.

    O mesmo achado que o CAGED mostra no fluxo, agora em nível: quanto do
    trabalho de TI acontece fora do setor de TI — e quanto cada lado paga.
    """
    return _consultar(f"""
        SELECT ano,
               CASE
                 WHEN setor_ti AND ocupacao_ti THEN 'Profissional de TI em empresa de TI'
                 WHEN ocupacao_ti              THEN 'Profissional de TI fora do setor de TI'
                 ELSE                               'Outra ocupação em empresa de TI'
               END AS categoria,
               sum(estoque_3112) AS estoque,
               {_media_ponderada('remuneracao_sm_mediana')} AS remuneracao_sm_mediana,
               {_media_ponderada('tempo_emprego_meses')}    AS tempo_emprego_meses
        FROM read_parquet('{_caminho_gold('rais_setor_vs_ocupacao')}')
        GROUP BY 1, 2 ORDER BY 1, 3 DESC
    """)


def estabelecimentos_por_ano() -> pd.DataFrame:
    """Quantas empresas de TI existem — o que o CAGED não tem."""
    return _consultar(f"""
        SELECT ano,
               sum(estabelecimentos) AS estabelecimentos,
               sum(vinculos_ativos)  AS vinculos_ativos,
               round(sum(vinculos_ativos) / nullif(sum(estabelecimentos), 0), 1)
                                     AS media_vinculos_por_estab
        FROM read_parquet('{_caminho_gold('rais_estabelecimentos')}')
        WHERE setor_ti
        GROUP BY 1 ORDER BY 1
    """)


def estabelecimentos_por_porte(ano: int) -> pd.DataFrame:
    return _consultar(f"""
        SELECT porte,
               sum(estabelecimentos) AS estabelecimentos,
               sum(vinculos_ativos)  AS vinculos_ativos
        FROM read_parquet('{_caminho_gold('rais_estabelecimentos')}')
        WHERE setor_ti AND ano = {int(ano)} AND porte IS NOT NULL
        GROUP BY 1 ORDER BY 3 DESC
    """)


def anos_disponiveis() -> list[int]:
    df = _consultar(f"""
        SELECT DISTINCT ano FROM read_parquet('{_caminho_gold('rais_estoque_anual')}')
        ORDER BY ano
    """)
    return [] if df.empty else [int(a) for a in df["ano"]]
