"""
Publica os agregados do dashboard como arquivos pequenos, versionáveis no git.

POR QUE ISTO EXISTE
-------------------
O data lake fica no MinIO local — que não existe quando o dashboard roda na
nuvem. Mas o dashboard nunca consome o lake inteiro: só agregados, e eles são
minúsculos (~92 KB para o Novo CAGED; algumas centenas de KB com a série
histórica). Nessa escala os dados cabem no próprio repositório, e o deploy
não precisa de banco, storage externo nem credencial nenhuma.

É a camada gold de volta — agora justificada pelo DEPLOY, não pela
performance local: ela desacopla o app público do lake de 1 GB.

Uso:
    python -m gold_caged.publicar
    # gera dados_publicados/*.parquet na raiz do projeto
"""
import sys
from pathlib import Path

from extracao_ftp.config_extracao import (
    PARQUET_COMPRESSION,
    PARQUET_COMPRESSION_LEVEL,
    conectar_duckdb,
)

DIR_PUBLICADO = Path(__file__).resolve().parents[2] / "dados_publicados"


def publicar() -> int:
    # Importa aqui para reaproveitar exatamente as mesmas consultas que o
    # dashboard usa — assim o que é publicado nunca diverge do que ele mostra.
    from dashboard import dados as d

    DIR_PUBLICADO.mkdir(parents=True, exist_ok=True)
    con = conectar_duckdb()

    consultas = {
        "mensal": f"""
            SELECT competenciamov_data AS competencia, {d.METRICAS}
            FROM read_parquet('{d.FONTE}')
            WHERE competenciamov_data IS NOT NULL GROUP BY 1 ORDER BY 1
        """,
        "mensal_uf": f"""
            SELECT competenciamov_data AS competencia, uf_descricao AS uf,
                   regiao_descricao AS regiao, {d.METRICAS}
            FROM read_parquet('{d.FONTE}')
            WHERE competenciamov_data IS NOT NULL AND uf_descricao IS NOT NULL
            GROUP BY 1, 2, 3 ORDER BY 1
        """,
        "setor": f"""
            SELECT ano_particao AS ano, secao_descricao AS setor, {d.METRICAS}
            FROM read_parquet('{d.FONTE}')
            WHERE secao_descricao IS NOT NULL GROUP BY 1, 2 ORDER BY 1
        """,
        "ocupacao": f"""
            SELECT ano_particao AS ano, cbo2002ocupacao_descricao AS ocupacao, {d.METRICAS}
            FROM read_parquet('{d.FONTE}')
            WHERE cbo2002ocupacao_descricao IS NOT NULL
            GROUP BY 1, 2 HAVING count(*) >= 50 ORDER BY 1
        """,
        "demografia": f"""
            SELECT ano_particao AS ano, sexo_descricao AS sexo,
                   racacor_descricao AS raca_cor,
                   graudeinstrucao_descricao AS escolaridade, {d.METRICAS}
            FROM read_parquet('{d.FONTE}') GROUP BY 1, 2, 3, 4 ORDER BY 1
        """,
        "lentes": d._sql_lentes(),
    }

    print(f"📦 Publicando agregados em {DIR_PUBLICADO}\n")
    total = 0
    for nome, sql in consultas.items():
        destino = DIR_PUBLICADO / f"{nome}.parquet"
        try:
            con.execute(f"""
                COPY ({sql}) TO '{destino.as_posix()}' (
                    FORMAT PARQUET,
                    COMPRESSION '{PARQUET_COMPRESSION.upper()}',
                    COMPRESSION_LEVEL {PARQUET_COMPRESSION_LEVEL}
                );
            """)
            linhas = con.execute(
                f"SELECT count(*) FROM read_parquet('{destino.as_posix()}')"
            ).fetchone()[0]
            tam = destino.stat().st_size
            total += tam
            print(f"   ✅ {nome:<12} {linhas:>7,} linhas  {tam / 1024:>8.1f} KB")
        except Exception as e:
            print(f"   ❌ {nome}: {str(e)[:200]}")
            return 1

    print(f"\n🏁 {total / 1024:.1f} KB no total — cabe no repositório do GitHub.")
    print("   Faça commit de dados_publicados/ para o deploy enxergar os dados.")
    return 0


if __name__ == "__main__":
    sys.exit(publicar())
