"""
Consultas do dashboard — DuckDB lendo a silver direto.

POR QUE SEM CAMADA GOLD
-----------------------
A silver é recortada em tecnologia: ~4,5 milhões de movimentações, não os 725
milhões do mercado inteiro. Nessa escala o DuckDB agrega em menos de um
segundo, então uma camada intermediária de agregados só acrescentaria um
passo de build e mais uma cópia para manter sincronizada.

As agregações abaixo fazem o papel da gold, e o cache do Streamlit as
materializa em memória na primeira execução — o efeito prático é o mesmo,
sem bucket extra.

Se a silver voltar a crescer muito (mercado completo, ou RAIS inteira), o
caminho é reintroduzir a gold: as consultas daqui viram os agregados de lá
praticamente sem alteração.
"""
import duckdb
import pandas as pd
import streamlit as st

from _utils import nuvem
from gold_unificado.dicionario_canonico import sql_canonico

# Fonte única: a silver de TI publicada no Hugging Face, lida direto por
# hf:// via DuckDB (httpfs) — sem MinIO, sem URL/ano configurado à mão.
# glob_silver_ti() já resolve pra "caged_mov" ou "caged_old" o mesmo
# repositório (ver _utils/nuvem.py), particionado por ano_particao.
FONTE = f"'{nuvem.glob_silver_ti('caged_mov')}'"

# Arquivo único com as duas gerações harmonizadas (ver gold_caged/unificar.py),
# publicado como "caged_ti_<ano>.parquet" (um arquivo por ano, SEM
# particionamento hive) no mesmo repositório da silver: nomes de coluna
# estáveis e setor comparável em toda a série 2007–2026. Quando existe, é a
# fonte preferida — evita repetir a lógica de união em cada consulta e cobre
# 20 anos em vez de 6.
FONTE_UNIF = f"'{nuvem.fonte_leitura(nuvem.REPO_SILVER_TI_CAGED, 'caged_ti_*.parquet')}'"

# As duas gerações do CAGED têm nomes de coluna diferentes para os MESMOS
# conceitos — o Novo CAGED reescreveu o layout em 2020. Unificar em uma view
# é o que permite a série contínua 2007–2026; sem isso o dashboard começaria
# em 2020 e perderia a crise de 2015-16 e o ciclo pré-pandemia.
#
# Fica de fora, de propósito, o setor da empresa: o CAGED antigo classifica
# por subsetor IBGE e o novo por seção CNAE. São taxonomias distintas, e
# empilhá-las produziria uma série falsa.
FONTE_UNIFICADA = f"""
    SELECT competenciamov_data AS competencia, uf_descricao, municipio_descricao,
           cbo2002ocupacao_descricao,
           {sql_canonico("sexo", "sexo_descricao")} AS sexo_descricao,
           {sql_canonico("raca_cor", "racacor_descricao")} AS racacor_descricao,
           {sql_canonico("escolaridade", "graudeinstrucao_descricao")} AS escolaridade_descricao,
           saldomovimentacao AS saldo_mov, salario AS salario_valor, idade,
           ano_particao, 'Novo CAGED' AS geracao
    FROM read_parquet({_caminho("caged_mov")})
    UNION ALL
    SELECT competencia_declarada_data, uf_descricao, municipio_descricao,
           cbo_2002_ocupacao_descricao,
           {sql_canonico("sexo", "sexo_descricao")},
           {sql_canonico("raca_cor", "raca_cor_descricao")},
           {sql_canonico("escolaridade", "grau_instrucao_descricao")},
           saldo_mov, salario_mensal, idade,
           ano_particao, 'CAGED antigo'
    FROM read_parquet({_caminho("caged_old")})
"""

# saldomovimentacao vale +1 na admissão e -1 no desligamento: é a definição
# oficial do saldo do CAGED (geração líquida de emprego formal).
# O salário médio considera só admissões com valor informado — no
# desligamento o salário reflete o histórico do vínculo, não o mercado atual,
# e os zeros (sem informação) afundariam a média.
METRICAS = """
    count(*) FILTER (WHERE saldomovimentacao = 1)  AS admissoes,
    count(*) FILTER (WHERE saldomovimentacao = -1) AS desligamentos,
    sum(saldomovimentacao)                          AS saldo,
    round(avg(CASE WHEN saldomovimentacao = 1 AND salario > 0
                   THEN salario END), 2)            AS salario_medio,
    round(avg(CASE WHEN saldomovimentacao = 1 THEN idade END), 1) AS idade_media
"""

# Mesmas métricas sobre a view unificada, que renomeia as colunas.
METRICAS_UNIF = """
    count(*) FILTER (WHERE saldo_mov = 1)  AS admissoes,
    count(*) FILTER (WHERE saldo_mov = -1) AS desligamentos,
    sum(saldo_mov)                          AS saldo,
    round(avg(CASE WHEN saldo_mov = 1 AND salario_valor > 0
                   THEN salario_valor END), 2)      AS salario_medio,
    round(avg(CASE WHEN saldo_mov = 1 THEN idade END), 1) AS idade_media
"""


@st.cache_resource
def conectar():
    con = duckdb.connect()
    con.execute("INSTALL httpfs; LOAD httpfs; SET enable_progress_bar=false;")
    nuvem.aplicar_secret_hf(con)
    return con


@st.cache_data(ttl=900, show_spinner="Consultando os dados…")
def _consultar(sql: str) -> pd.DataFrame:
    try:
        return conectar().execute(sql).df()
    except Exception as e:
        st.error(f"Falha na consulta: {str(e)[:300]}")
        return pd.DataFrame()


def tem_dados() -> bool:
    df = _consultar(f"SELECT count(*) AS n FROM read_parquet({FONTE})")
    return not df.empty and df["n"].iloc[0] > 0


def _sql_lentes() -> str:
    """
    Cruzamento das duas lentes do recorte.

    Responde quanto do trabalho de TI acontece FORA das empresas de
    tecnologia — o desenvolvedor do banco, da rede de varejo, do hospital.
    Classificar em SQL (e não em Python) mantém a lógica junto da definição
    do recorte e evita trazer as linhas cruas para a memória.
    """
    from gold_caged import escopo_tecnologia as esc

    return f"""
        SELECT ano_particao AS ano,
               CASE
                 WHEN {esc.sql_filtro_cnae()} AND {esc.sql_filtro_cbo()}
                   THEN 'Profissional de TI em empresa de TI'
                 WHEN {esc.sql_filtro_cbo()}
                   THEN 'Profissional de TI fora do setor de TI'
                 ELSE 'Outra ocupação em empresa de TI'
               END AS categoria,
               secao_descricao AS setor_empresa,
               {METRICAS}
        FROM read_parquet({FONTE})
        GROUP BY 1, 2, 3 ORDER BY 1
    """


# nome do agregado publicado -> SQL equivalente sobre a silver. As duas
# fontes produzem exatamente as mesmas colunas: publicar.py usa estas mesmas
# consultas, então o que é publicado nunca diverge do que o app calcula.
def _sql(nome: str) -> str:
    return {
        "mensal": f"""
            SELECT competenciamov_data AS competencia, {METRICAS}
            FROM read_parquet({FONTE})
            WHERE competenciamov_data IS NOT NULL GROUP BY 1 ORDER BY 1
        """,
        "mensal_uf": f"""
            SELECT competenciamov_data AS competencia, uf_descricao AS uf,
                   regiao_descricao AS regiao, {METRICAS}
            FROM read_parquet({FONTE})
            WHERE competenciamov_data IS NOT NULL AND uf_descricao IS NOT NULL
            GROUP BY 1, 2, 3 ORDER BY 1
        """,
        "setor": f"""
            SELECT ano_particao AS ano, secao_descricao AS setor, {METRICAS}
            FROM read_parquet({FONTE})
            WHERE secao_descricao IS NOT NULL GROUP BY 1, 2 ORDER BY 1
        """,
        "ocupacao": f"""
            SELECT ano_particao AS ano, cbo2002ocupacao_descricao AS ocupacao, {METRICAS}
            FROM read_parquet({FONTE})
            WHERE cbo2002ocupacao_descricao IS NOT NULL
            GROUP BY 1, 2 HAVING count(*) >= 50 ORDER BY 1
        """,
        "demografia": f"""
            SELECT ano_particao AS ano, {sql_canonico("sexo", "sexo_descricao")} AS sexo,
                   {sql_canonico("raca_cor", "racacor_descricao")} AS raca_cor,
                   {sql_canonico("escolaridade", "graudeinstrucao_descricao")} AS escolaridade, {METRICAS}
            FROM read_parquet({FONTE}) GROUP BY 1, 2, 3, 4 ORDER BY 1
        """,
        "lentes": _sql_lentes(),
    }[nome]


def _obter(nome: str) -> pd.DataFrame:
    return _consultar(_sql(nome))


def mensal() -> pd.DataFrame:
    return _obter("mensal")


def mensal_por_uf() -> pd.DataFrame:
    return _obter("mensal_uf")


def por_setor() -> pd.DataFrame:
    """Setor da EMPRESA que contrata — mostra onde o profissional de TI trabalha."""
    return _obter("setor")


def por_ocupacao() -> pd.DataFrame:
    return _obter("ocupacao")


def demografia() -> pd.DataFrame:
    return _obter("demografia")


def setor_ti_vs_ocupacao_ti() -> pd.DataFrame:
    return _obter("lentes")


# Métricas sobre o arquivo unificado, cujas colunas já têm nomes estáveis.
METRICAS_UNI = """
    count(*) FILTER (WHERE saldo = 1)  AS admissoes,
    count(*) FILTER (WHERE saldo = -1) AS desligamentos,
    sum(saldo)                          AS saldo,
    round(avg(CASE WHEN saldo = 1 AND salario > 0 THEN salario END), 2) AS salario_medio,
    round(avg(CASE WHEN saldo = 1 THEN idade END), 1) AS idade_media
"""


@st.cache_resource
def tem_serie_longa() -> bool:
    """A série de 20 anos depende do arquivo unificado estar publicado."""
    try:
        conectar().execute(f"SELECT 1 FROM read_parquet({FONTE_UNIF}) LIMIT 1").fetchone()
        return True
    except Exception:
        return False


def serie_longa_anual() -> pd.DataFrame:
    """Agregado anual 2007–2026 — o esqueleto da narrativa histórica."""
    return _consultar(f"""
        SELECT ano, geracao, {METRICAS_UNI}
        FROM read_parquet({FONTE_UNIF})
        GROUP BY 1, 2 ORDER BY 1
    """)


def serie_longa_mensal() -> pd.DataFrame:
    return _consultar(f"""
        SELECT competencia, {METRICAS_UNI}
        FROM read_parquet({FONTE_UNIF})
        WHERE competencia IS NOT NULL GROUP BY 1 ORDER BY 1
    """)


def setor_longo() -> pd.DataFrame:
    """Série setorial dos 20 anos — possível porque a seção do CAGED antigo
    é derivada do CNAE 2.0 (ver gold_caged/cnae_secao.py)."""
    return _consultar(f"""
        SELECT ano, setor, {METRICAS_UNI}
        FROM read_parquet({FONTE_UNIF})
        WHERE setor IS NOT NULL GROUP BY 1, 2 ORDER BY 1
    """)


def uf_longo() -> pd.DataFrame:
    return _consultar(f"""
        SELECT ano, uf, {METRICAS_UNI}
        FROM read_parquet({FONTE_UNIF})
        WHERE uf IS NOT NULL GROUP BY 1, 2 ORDER BY 1
    """)


def ocupacao_longa() -> pd.DataFrame:
    return _consultar(f"""
        SELECT ano, ocupacao, {METRICAS_UNI}
        FROM read_parquet({FONTE_UNIF})
        WHERE ocupacao IS NOT NULL
        GROUP BY 1, 2 HAVING count(*) >= 50 ORDER BY 1
    """)


def demografia_longa() -> pd.DataFrame:
    return _consultar(f"""
        SELECT ano, sexo, raca_cor, escolaridade, {METRICAS_UNI}
        FROM read_parquet({FONTE_UNIF})
        GROUP BY 1, 2, 3, 4 ORDER BY 1
    """)


def fonte_atual() -> str:
    """Rótulo da origem dos dados, para o rodapé do dashboard."""
    return f"silver publicada no Hugging Face ({nuvem.REPO_SILVER_TI_CAGED})"
