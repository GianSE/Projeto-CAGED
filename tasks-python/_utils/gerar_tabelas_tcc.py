"""
Gera as tabelas LaTeX do TCC a partir da camada Gold.

POR QUE GERAR, E NÃO DIGITAR
----------------------------
Todo número deste trabalho vem de um agregado da camada Gold. Digitá-los no
texto criaria duas versões da mesma informação, e a segunda envelheceria em
silêncio: bastaria uma revisão do recorte ou uma correção na camada para o
documento passar a afirmar algo que o dado não diz. Foi exatamente o que
aconteceu com a lista de códigos CBO, escrita à mão no início e corrigida depois.

Cada tabela sai num arquivo próprio em `TCC2_.../estrutura/tabelas-geradas/`,
incluído no capítulo correspondente com `\\input`. Regerar é um comando:

    python -m gerar_tabelas_tcc        (de dentro de tasks_python/)

O script mora AQUI, e não na pasta do TCC, por dois motivos: precisa das
credenciais e dos módulos do pipeline para ler a camada Gold, e a pasta do TCC é
empacotada para o Overleaf, que não executa Python — levá-lo junto aumentaria o
pacote sem servir para nada lá.

Requer acesso ao MinIO (o mesmo `.env` do pipeline).
"""
import sys
from pathlib import Path

RAIZ_PROJETO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(Path(__file__).resolve().parent))

from extracao_ftp.config_extracao import (BUCKET_BRONZE, BUCKET_SILVER_TI,
                                          conectar_duckdb)

DESTINO = (RAIZ_PROJETO / "TCC2_Gian_Pedro_Rodrigues" / "estrutura"
           / "tabelas-geradas")
G = "s3://gold"


def _fs():
    import s3fs
    from extracao_ftp.config_extracao import (MINIO_ACCESS_KEY, MINIO_ENDPOINT,
                                              MINIO_REGION, MINIO_SECRET_KEY)
    return s3fs.S3FileSystem(
        key=MINIO_ACCESS_KEY, secret=MINIO_SECRET_KEY,
        client_kwargs={"endpoint_url": f"http://{MINIO_ENDPOINT}",
                       "region_name": MINIO_REGION})


con, fs = conectar_duckdb(), _fs()
con.execute("SET enable_progress_bar=false")
DESTINO.mkdir(parents=True, exist_ok=True)


# ------------------------------------------------------------- formatação
def num(v, casas: int = 0) -> str:
    """Número no padrão brasileiro: ponto de milhar, vírgula decimal."""
    if v is None:
        return "---"
    texto = f"{float(v):,.{casas}f}"
    return texto.replace(",", "X").replace(".", ",").replace("X", ".")


def ano(v) -> str:
    """Ano é identificador, não quantidade: sem separador de milhar."""
    return str(int(v))


def pct(v, casas: int = 1) -> str:
    return f"{num(v, casas)}\\%"


def escrever(nome: str, corpo: str):
    (DESTINO / nome).write_text(corpo.rstrip() + "\n", encoding="utf-8")
    print(f"   {nome}")


def tabela(nome: str, caption: str, rotulo: str, colunas: str,
           cabecalho: list[str], linhas: list[list[str]],
           fonte: str = "Autoria própria, a partir da camada Gold.",
           nota: str = "", ambiente: str = "table") -> None:
    """Monta uma tabela ABNT com booktabs e grava o arquivo."""
    cab = " & ".join(f"\\textbf{{{c}}}" for c in cabecalho) + " \\\\"
    corpo = "\n".join("        " + " & ".join(l) + " \\\\" for l in linhas)
    escrever(nome, f"""% Gerado por scripts/gerar_tabelas.py --- não editar à mão.
\\begin{{{ambiente}}}[htbp]
    \\centering
    \\caption{{{caption}}}
    \\label{{{rotulo}}}
    \\footnotesize
    \\renewcommand{{\\arraystretch}}{{1.25}}
    \\begin{{tabular}}{{{colunas}}}
        \\toprule
        \\rowcolor{{black!8}}
        {cab}
        \\midrule
{corpo}
        \\bottomrule
    \\end{{tabular}}
    \\\\[4pt]
    {nota}Fonte: {fonte}
\\end{{{ambiente}}}""")


print("Gerando tabelas em", DESTINO)

# ------------------------------------------------- volumes das camadas
linhas = []
for bucket, rotulo in ((BUCKET_BRONZE, "Bronze"), ("silver", "Silver (mercado completo)"),
                       (BUCKET_SILVER_TI, "Silver de TI"), ("gold", "Gold")):
    infos = fs.find(bucket, detail=True)
    arqs = [v["size"] for k, v in infos.items() if k.endswith(".parquet")]
    tam = sum(arqs) / 2**30
    linhas.append([rotulo, num(len(arqs)), f"{num(tam, 2)} GB" if tam >= 0.01 else "< 10 MB"])
tabela("tab-volumes.tex", "Volume de cada camada do \\textit{data lake}",
       "tab:volumes", "lrr", ["Camada", "Arquivos", "Tamanho"], linhas)

# ------------------------------------------------- linhas por tabela
TABELAS_TI = ("caged_mov", "caged_old", "caged_for", "caged_exc", "caged_ajustes",
              "rais_vinc", "rais_estab")
linhas = []
for t in TABELAS_TI:
    n = con.execute(f"SELECT count(*) FROM read_parquet("
                    f"'s3://{BUCKET_SILVER_TI}/{t}/**/*.parquet', union_by_name=true)"
                    ).fetchone()[0]
    linhas.append([f"\\texttt{{{t.replace('_', chr(92) + '_')}}}", num(n)])
tabela("tab-linhas-camada.tex",
       "Registros no recorte de tecnologia, por tabela da camada tratada",
       "tab:linhas-camada", "lr", ["Tabela", "Registros"], linhas)

# ------------------------------------------------- estoque e qualidade
linhas = [[ano(r[0]), num(r[1]), num(r[2], 2), num(r[3])] for r in con.execute(f"""
    SELECT ano, sum(estoque_3112) AS estoque,
           sum(estoque_3112 * remuneracao_sm_mediana) / sum(estoque_3112) AS sm,
           sum(estoque_3112 * tempo_emprego_meses) / sum(estoque_3112) AS meses
    FROM read_parquet('{G}/rais_estoque_anual.parquet')
    GROUP BY 1 ORDER BY 1
""").fetchall()]
tabela("tab-estoque-serie.tex",
       "Estoque de vínculos formais de TI, remuneração mediana e tempo de emprego",
       "tab:estoque-serie", "lrrr",
       ["Ano", "Vínculos em 31/12", "Remuneração mediana (SM)", "Tempo de emprego (meses)"],
       linhas, nota="SM: salários mínimos do ano de referência. ")

# ------------------------------------------------- as duas lentes
ANO = con.execute(f"SELECT max(ano) FROM read_parquet('{G}/rais_estoque_anual.parquet')").fetchone()[0]
linhas = [[r[0], num(r[1]), num(r[2], 2), num(r[3])] for r in con.execute(f"""
    SELECT CASE WHEN ocupacao_ti AND NOT setor_ti THEN 'Profissional de TI fora de empresa de TI'
                WHEN ocupacao_ti AND setor_ti     THEN 'Profissional de TI em empresa de TI'
                ELSE 'Outra ocupação em empresa de TI' END AS lente,
           sum(estoque_3112) AS estoque,
           sum(estoque_3112 * remuneracao_sm_mediana) / sum(estoque_3112) AS sm,
           sum(estoque_3112 * tempo_emprego_meses) / sum(estoque_3112) AS meses
    FROM read_parquet('{G}/rais_setor_vs_ocupacao.parquet')
    WHERE ano = {ANO} GROUP BY 1 ORDER BY estoque DESC
""").fetchall()]
tabela("tab-lentes.tex",
       f"As duas lentes do recorte de tecnologia em {ANO}",
       "tab:lentes", "lrrr",
       ["Lente", "Vínculos", "Remuneração mediana (SM)", "Tempo de emprego (meses)"], linhas)

# ------------------------------------------------- areas de TI
linhas = [[r[0], num(r[1]), num(r[2], 2)] for r in con.execute(f"""
    SELECT area_ti, sum(estoque_3112) AS estoque,
           sum(estoque_3112 * remuneracao_sm_mediana) / sum(estoque_3112) AS sm
    FROM read_parquet('{G}/rais_estoque_area.parquet')
    WHERE ano = {ANO} GROUP BY 1 ORDER BY estoque DESC
""").fetchall()]
tabela("tab-areas.tex", f"Estoque e remuneração por área de TI em {ANO}",
       "tab:areas", "lrr", ["Área", "Vínculos", "Remuneração mediana (SM)"], linhas)

# ------------------------------------------------- concentracao por UF
linhas = [[r[0], num(r[1]), pct(r[2]), num(r[3], 2)] for r in con.execute(f"""
    SELECT uf, estoque, 100.0 * estoque / sum(estoque) OVER () AS parte,
           remuneracao_sm_mediana
    FROM read_parquet('{G}/mapa_uf.parquet') WHERE ano = {ANO}
    ORDER BY estoque DESC LIMIT 10
""").fetchall()]
tabela("tab-uf.tex", f"As dez unidades federativas com maior estoque de TI em {ANO}",
       "tab:uf", "lrrr",
       ["UF", "Vínculos", "Participação", "Remuneração mediana (SM)"], linhas)

# ------------------------------------------------- hiato salarial
linhas = [[ano(r[0]), r[1], pct(r[2]), num(r[3], 3), num(r[4], 3)] for r in con.execute(f"""
    SELECT ano, comparacao, hiato_pct, explicada, nao_explicada
    FROM read_parquet('{G}/hiato_serie.parquet')
    WHERE ano IN (2007, 2015, {ANO}) ORDER BY comparacao, ano
""").fetchall()]
tabela("tab-hiato.tex", "Decomposição de Oaxaca-Blinder do hiato salarial",
       "tab:hiato", "llrrr",
       ["Ano", "Comparação", "Hiato bruto", "Parcela explicada (log)",
        "Parcela não explicada (log)"], linhas,
       nota="Parcelas em logaritmo da remuneração; referência de Neumark. ")

# ------------------------------------------------- previsao
linhas = [[f"\\texttt{{{r[0]}}}", num(r[1]), num(r[2]), num(r[3]), pct(r[4])]
          for r in con.execute(f"""
    SELECT modelo, dobras, mae, rmse, \"ganho_vs_naive_%\"
    FROM read_parquet('{G}/previsao_placar.parquet') ORDER BY mae
""").fetchall()]
tabela("tab-previsao.tex",
       "Desempenho dos modelos na validação de origem móvel",
       "tab:previsao", "lrrrr",
       ["Modelo", "Dobras", "MAE", "RMSE", "Ganho sobre o ingênuo"], linhas)

# ------------------------------------------------- nowcast
linhas = [[r[0], num(r[1]), num(r[2]), num(r[3]), pct(r[4], 2)] for r in con.execute(f"""
    SELECT estimador, previsto, real, erro, erro_pct
    FROM read_parquet('{G}/nowcast_ano_retido.parquet') ORDER BY abs(erro_pct)
""").fetchall()]
ano_teste = con.execute(f"SELECT max(ano_teste) FROM read_parquet('{G}/nowcast_ano_retido.parquet')").fetchone()[0]
tabela("tab-nowcast.tex",
       f"\\textit{{Nowcast}} do estoque: estimadores ajustados sem o ano de {num(ano_teste)}",
       "tab:nowcast", "lrrrr",
       ["Estimador", "Estimado", "Observado", "Erro", "Erro relativo"], linhas)

# ------------------------------------------------- sobrevivencia
linhas = [[r[0], num(r[1]), num(r[2]), pct(r[3])] for r in con.execute(f"""
    SELECT tempo_de_casa, vinculos, desligamentos, \"risco_%\"
    FROM read_parquet('{G}/sobrevivencia_risco.parquet') ORDER BY vinculos DESC
""").fetchall()]
tabela("tab-risco.tex", "Risco de desligamento por tempo de casa",
       "tab:risco", "lrrr",
       ["Tempo de casa", "Vínculos", "Desligamentos", "Risco no ano"], linhas)

# ------------------------------------------------- perfis de municipio
linhas = [[r[0], num(r[1]), num(r[2]), num(r[3], 2), num(r[4], 2)] for r in con.execute(f"""
    SELECT nome, count(*) AS municipios, avg(estoque) AS estoque,
           avg(crescimento) AS cresc, avg(remuneracao_sm_mediana) AS sm
    FROM read_parquet('{G}/municipios_cluster.parquet')
    GROUP BY 1 ORDER BY 3 DESC
""").fetchall()]
tabela("tab-clusters.tex", "Perfis de município obtidos por $k$-médias",
       "tab:clusters", "lrrrr",
       ["Perfil", "Municípios", "Estoque médio", "Crescimento médio",
        "Remuneração mediana (SM)"], linhas)

# ------------------------------------------- apendice: codigos do recorte
linhas = [[f"\\texttt{{{r[0]}}}", f"\\texttt{{{r[1]}}}", r[2][:52], r[3], num(r[4])]
          for r in con.execute(f"""
    SELECT familia, cbo, ocupacao, area, estoque_3112
    FROM read_parquet('{G}/rais_recorte_cbo.parquet')
    WHERE ano = {ANO} ORDER BY familia, cbo
""").fetchall()]
tabela("tab-cbos.tex",
       f"Códigos CBO do recorte de tecnologia observados no estoque de {ANO}",
       "quadro:cbos", "llp{6.0cm}lr",
       ["Família", "CBO", "Ocupação", "Área", f"Vínculos {ANO}"], linhas,
       nota="Ocupações sem descrição são códigos em uso que não constam dos "
            "dicionários oficiais do MTE. ", ambiente="quadro")

print(f"\n{len(list(DESTINO.glob('*.tex')))} tabela(s) geradas.")
