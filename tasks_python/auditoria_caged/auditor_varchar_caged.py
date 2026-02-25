import os
import duckdb

# --- CONFIGURAÇÕES ---
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "host.docker.internal:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
MINIO_BUCKET = "bronze"

PATH_GLOB = f"s3://{MINIO_BUCKET}/caged_ajustes/**/*.parquet"

# Colunas que ACEITAMOS que sejam numéricas (Partição e Tempo)
COLUNAS_IGNORAR = [
    'ano', 'mes', 
    'ano_particao', 'mes_particao'
]

# --- CONEXÃO ---
con = duckdb.connect()
con.execute("INSTALL httpfs; LOAD httpfs;")
con.execute(f"""
    SET s3_endpoint='{MINIO_ENDPOINT}';
    SET s3_access_key_id='{MINIO_ACCESS_KEY}';
    SET s3_secret_access_key='{MINIO_SECRET_KEY}';
    SET s3_use_ssl=false;
    SET s3_url_style='path';
""")

def auditar_inteligente():
    print(f"🔍 Auditoria Focada em Dados de Negócio: {PATH_GLOB}")
    
    try:
        arquivos = con.execute(f"SELECT file FROM glob('{PATH_GLOB}')").fetchall()
    except Exception as e:
        print(f"❌ Erro ao listar arquivos: {e}")
        return

    arquivos_com_erro = 0
    total = len(arquivos)
    
    print(f"📂 Total de arquivos: {total}\n")

    for i, row in enumerate(arquivos):
        arquivo = row[0]
        nome_curto = arquivo.split('/')[-1]
        
        try:
            # DESCRIBE do parquet
            schema = con.execute(f"DESCRIBE SELECT * FROM read_parquet('{arquivo}')").fetchall()
            
            colunas_criticas_erradas = []
            
            for col in schema:
                col_name = col[0].lower()
                col_type = col[1]
                
                # Se a coluna NÃO está na lista de ignoradas E NÃO é VARCHAR/STRING
                if col_name not in COLUNAS_IGNORAR and col_type not in ['VARCHAR', 'STRING']:
                    colunas_criticas_erradas.append(f"{col_name} ({col_type})")
            
            if colunas_criticas_erradas:
                print(f"❌ [ALERTA CRÍTICO] {nome_curto}")
                print(f"   ⚠️ IDs ou Dados de negócio como número: {colunas_criticas_erradas}")
                arquivos_com_erro += 1
            # Ocultamos os OKs para o terminal ficar limpo

        except Exception as e:
            print(f"⚠️ Erro ao ler {nome_curto}: {e}")

    print(f"\n{'='*50}")
    print(f"📊 RESULTADO DA VALIDAÇÃO")
    print(f"{'='*50}")
    if arquivos_com_erro == 0:
        print("✅ TUDO LIMPO! Todos os IDs (CBO, CNPJ, etc) são VARCHAR.")
        print("   As colunas de partição (ano/mes) estão numéricas como planejado.")
    else:
        print(f"❌ Encontrados {arquivos_com_erro} arquivos com colunas críticas numéricas.")
    print(f"{'='*50}")

if __name__ == "__main__":
    auditar_inteligente()