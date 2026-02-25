import duckdb
import os

# --- CONFIGURAÇÕES DO MINIO ---
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "127.0.0.1:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")

# --- ARQUIVOS ---
ARQUIVO_ALVO = "s3://bronze/caged_ajustes/ano_hive=2010/mes_hive=10/cagedest_ajustes_102010.parquet"
ARQUIVO_LIMPO = "s3://bronze/caged_ajustes/ano_hive=2010/mes_hive=10/cagedest_ajustes_102010_limpo.parquet"

# --- CONEXÃO DUCKDB ---
con = duckdb.connect()
con.execute("INSTALL httpfs; LOAD httpfs;")
con.execute(f"""
    SET s3_endpoint='{MINIO_ENDPOINT}';
    SET s3_access_key_id='{MINIO_ACCESS_KEY}';
    SET s3_secret_access_key='{MINIO_SECRET_KEY}';
    SET s3_use_ssl=false;
    SET s3_url_style='path';
""")

print(f"🧹 Lendo o arquivo original: {ARQUIVO_ALVO}")

# --- A MÁGICA DA LIMPEZA ---
# O EXCLUDE agora tira as colunas fantasmas de 45 a 52
query_reprocessamento = f"""
    COPY (
        SELECT * EXCLUDE (
            ano_hive, mes_hive, column42, column43, column44, column45, column46, column47, column48, column49, column50, column51, column52, column53, column54, column55, column56, column57, column58, column59, column60, column61, column62, column63, column64, column65
        )
        FROM '{ARQUIVO_ALVO}'
    ) TO '{ARQUIVO_LIMPO}' (FORMAT PARQUET);
"""

try:
    print("⏳ Processando e gravando o novo Parquet no MinIO...")
    con.execute(query_reprocessamento)
    
    # Validação rápida
    colunas_finais = con.execute(f"DESCRIBE SELECT * FROM read_parquet('{ARQUIVO_LIMPO}')").fetchall()
    nomes_cols = [linha[0] for linha in colunas_finais]
    
    print(f"✅ Sucesso! O arquivo limpo foi salvo como: {ARQUIVO_LIMPO}")
    print(f"📊 Total de colunas agora: {len(nomes_cols)}")
    print("💡 Não se esqueça de deletar o arquivo original no painel do MinIO para não duplicar os dados na sua análise.")

except Exception as e:
    print(f"❌ Erro ao tentar reprocessar o arquivo: {e}")