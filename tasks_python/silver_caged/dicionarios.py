"""
Leitura das planilhas de dicionário (bronze/dicionarios/...) como lookups SQL.

As planilhas convertidas em bronze/dicionarios/{namespace}/{aba}.parquet vêm em
quatro formatos, dependendo da fonte:

  1. "2col" — Novo CAGED (bronze/dicionarios/novo_caged/*):
     col_00=Código, col_01=Descrição, uma linha por código, primeira linha é o
     cabeçalho literal ("Código"/"Descrição") repetido como dado.

  2. "colon" — CAGED antigo, dicionários de uma coluna só
     (bronze/dicionarios/caged/municipio.parquet, cbo2002.parquet, ...):
     "codigo:descrição" numa coluna (ex.: "110001:Ro-Alta Floresta D Oeste"),
     primeira linha é o título da planilha (sem ":"), sem valor de código.

  3. "colon_coluna" — CAGED antigo, aba "outros": onze listas regionais
     (mesorregião, microrregião, regiões administrativas/SENAI/SENAC/COREDE)
     empilhadas lado a lado, uma por coluna (col_00..col_10), mesmo formato
     "codigo:descrição" de cada uma.

  4. "cagestid" — CAGED antigo, aba "cagestid_layout": o layout oficial do
     CAGEDEST published pelo MTE traz, para os campos de código curto (sexo,
     raça/cor, grau de instrução, tipo de movimento, ...), a lista de
     categorias embutida na própria descrição do layout, no formato:

         Nome        | ... | Categorias  | Valor na Fonte
         GRAU INSTR   |     | Analfabeto  | 1
         (nulo)       |     | Até 5ª...   | 2
         ...

     "Nome" só aparece na primeira linha de cada campo (precisa de
     forward-fill). IMPORTANTE: os códigos daqui são os do CAGED ANTIGO, que
     não são os mesmos do Novo CAGED — sexo é 1=Masculino/2=Feminino/-1 aqui,
     mas 1=Homem/3=Mulher/9 no Novo CAGED; raça/cor é 1,2,4,6,8,9 aqui contra
     1-6,9 no Novo CAGED. Por isso não dá para reaproveitar o dicionário do
     Novo CAGED nessas colunas.

  5. "titulo_codigo" — RAIS (bronze/dicionarios/rais_layouts/*): descrição
     numa coluna, código em outra, com a linha de título da lista (sem
     código, só o nome do campo) descartada automaticamente por não ter
     valor na coluna de código:

         col_00                              col_02
         "grau de instrução após 2005"       NULL   <- título, cai fora
         "ANALFABETO"                        "1"
         "ATE 5.A INC"                       "2"

     Uma variante do mesmo formato ("faixas") empilha DOIS desses pares lado
     a lado na mesma aba (col_00/col_01 = faixa etária, col_02/col_03 =
     faixa de remuneração) — por isso as colunas de descrição/código são
     parâmetros, não fixas.

Apesar do nome do módulo (herdado do CAGED, que foi construído primeiro), o
motor aqui é genérico — silver_rais importa e reusa as mesmas funções.

Este módulo cria, dentro de uma conexão DuckDB já aberta, uma VIEW temporária
por dicionário com colunas (codigo VARCHAR, descricao VARCHAR) — pronta para
LEFT JOIN na construção da silver.
"""
from extracao_ftp.config_extracao import BUCKET_BRONZE

PREFIXO_DICIONARIOS = f"{BUCKET_BRONZE}/dicionarios"

# Nomes de campo (coluna "Nome" da aba cagestid_layout, já forward-filled) que
# de fato contêm uma lista de códigos enumerável. Os demais campos da mesma
# aba são placeholders de formato (ex.: "<99999999>" para salário) e não
# viram dicionário.
CAMPOS_CAGESTID = {
    "ADMITIDOS/DESLIGADOS": "admitidosdesligados",
    "FX EMP JAN": "faixa_empr_inicio_jan",
    "GRAU INSTR": "grau_instrucao",
    "IBGE SUBSETOR": "ibge_subsetor",
    "IND APRENDIZ": "ind_aprendiz",
    "PORT DEFIC": "ind_portador_defic",
    "RACACOR": "raca_cor",
    "SEXO": "sexo",
    "TIPO ESTBL": "tipo_estab",
    "TP DEFIC": "tipo_defic",
    "TP MOV DESAG": "tipo_mov_desagregado",
    "UF": "uf",
    "IND TRAB PARCIAL": "ind_trab_parcial",
    "IND TRAB INTERMITENTE": "ind_trab_intermitente",
}

# Colunas da aba "outros" (CAGED antigo) -> nome de campo do bronze.
COLUNAS_ABA_OUTROS = {
    "col_00": "mesorregiao",
    "col_01": "microrregiao",
    "col_02": "regiao_adm_rj",
    "col_03": "regiao_adm_sp",
    "col_04": "regiao_gov_sp",
    "col_05": "regiao_senai_sp",
    "col_06": "regiao_senac_pr",
    "col_07": "regiao_senai_pr",
    "col_08": "subregiao_senai_pr",
    "col_09": "regiao_corede_04",
    "col_10": "regiao_corede",
}


def _caminho(namespace: str, aba: str) -> str:
    return f"s3://{PREFIXO_DICIONARIOS}/{namespace}/{aba}.parquet"


def existe(fs, namespace: str, aba: str) -> bool:
    """Verifica no MinIO se um dicionário existe, sem tentar lê-lo."""
    return fs.exists(f"{PREFIXO_DICIONARIOS}/{namespace}/{aba}.parquet")


def _sql_2col(caminho: str) -> str:
    return f"""
        SELECT DISTINCT
            trim(col_00) AS codigo,
            trim(col_01) AS descricao
        FROM read_parquet('{caminho}')
        WHERE trim(col_00) NOT IN ('Código', 'CÓDIGO', 'codigo')
          AND col_00 IS NOT NULL
          AND col_01 IS NOT NULL
    """


def _sql_colon(caminho: str, coluna: str = "col_00") -> str:
    # "110001:Ro-Alta Floresta D Oeste" -> codigo=110001, descricao=Ro-...
    # A primeira linha (título da lista, sem ":") cai fora sozinha porque
    # strpos devolve 0 e o WHERE exige > 0.
    return f"""
        SELECT DISTINCT
            trim(substr("{coluna}", 1, strpos("{coluna}", ':') - 1)) AS codigo,
            trim(substr("{coluna}", strpos("{coluna}", ':') + 1)) AS descricao
        FROM read_parquet('{caminho}')
        WHERE "{coluna}" IS NOT NULL AND strpos("{coluna}", ':') > 0
    """


def _sql_cagestid(caminho: str, campo: str) -> str:
    # col_00 (Nome do campo) só vem preenchido na 1ª linha de cada bloco;
    # as linhas seguintes (mesmo campo, categoria seguinte) vêm NaN — por
    # isso o forward-fill via window function antes de filtrar pelo campo.
    campo_escapado = campo.replace("'", "''")
    return f"""
        WITH preenchido AS (
            SELECT
                last_value(col_00 IGNORE NULLS) OVER (
                    ORDER BY rowid ROWS UNBOUNDED PRECEDING
                ) AS campo,
                col_03 AS categoria,
                col_04 AS valor_fonte
            FROM (SELECT *, row_number() OVER () AS rowid FROM read_parquet('{caminho}'))
        )
        SELECT DISTINCT
            trim(valor_fonte) AS codigo,
            trim(categoria) AS descricao
        FROM preenchido
        WHERE upper(trim(campo)) = upper('{campo_escapado}')
          AND valor_fonte IS NOT NULL
          AND categoria IS NOT NULL
    """


def _sql_titulo_codigo(caminho: str, col_desc: str = "col_00", col_cod: str = "col_01") -> str:
    # A linha de título não tem valor na coluna de código -> IS NOT NULL a descarta sozinha.
    return f"""
        SELECT DISTINCT
            trim("{col_cod}") AS codigo,
            trim("{col_desc}") AS descricao
        FROM read_parquet('{caminho}')
        WHERE "{col_cod}" IS NOT NULL AND "{col_desc}" IS NOT NULL
    """


def criar_view(con, namespace: str, aba: str, estilo: str, nome_view: str, **kwargs) -> bool:
    """
    Cria (ou substitui) uma VIEW temporária (codigo, descricao) no DuckDB.

    Devolve False sem lançar exceção se o parquet não existir ou vier vazio —
    a silver deve seguir sem tradução para essa coluna, não travar por isso.
    """
    caminho = _caminho(namespace, aba)

    if estilo == "2col":
        sql = _sql_2col(caminho)
    elif estilo == "colon":
        sql = _sql_colon(caminho, kwargs.get("coluna", "col_00"))
    elif estilo == "colon_coluna":
        sql = _sql_colon(caminho, kwargs["coluna"])
    elif estilo == "cagestid":
        sql = _sql_cagestid(caminho, kwargs["campo"])
    elif estilo == "titulo_codigo":
        sql = _sql_titulo_codigo(caminho, kwargs.get("col_desc", "col_00"), kwargs.get("col_cod", "col_01"))
    else:
        raise ValueError(f"estilo de dicionário desconhecido: {estilo}")

    try:
        con.execute(f"CREATE OR REPLACE TEMP VIEW {nome_view} AS {sql};")
        total = con.execute(f"SELECT count(*) FROM {nome_view}").fetchone()[0]
        return total > 0
    except Exception as e:
        print(f"      ⚠️  Dicionário {namespace}/{aba} ({estilo}) indisponível: {str(e)[:150]}")
        return False
