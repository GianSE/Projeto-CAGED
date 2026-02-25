import duckdb
import os
import csv

# --- CONFIGURAÇÕES ---
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "localhost:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
MINIO_BUCKET = "bronze"

# Alterado para o prefixo do CAGED
CAGED_PREFIX = "caged_ajustes" 
OUTPUT_FILE = "ajustes_auditoria_caged_detalhada.csv"

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

def get_all_parquet_files():
    print(f"🔍 Listando arquivos em s3://{MINIO_BUCKET}/{CAGED_PREFIX}/...")
    try:
        query = f"SELECT file FROM glob('s3://{MINIO_BUCKET}/{CAGED_PREFIX}/**/*.parquet')"
        result = con.execute(query).fetchall()
        files = [row[0] for row in result]
        print(f"✅ Total de arquivos encontrados: {len(files)}")
        return sorted(files)
    except Exception as e:
        print(f"❌ Erro ao listar arquivos: {e}")
        return []

def check_file_content(s3_path):
    """
    Identifica colunas REAIS vs VIRTUAIS e valida campos do Novo CAGED.
    """
    try:
        # 1. PEGA O SCHEMA REAL (sem as pastas)
        query_real = f"DESCRIBE SELECT * FROM read_parquet('{s3_path}', hive_partitioning = 0)"
        schema_real = con.execute(query_real).fetchall()
        cols_reais = [row[0].lower() for row in schema_real]
        
        # 2. PEGA O SCHEMA HIVE (com as pastas)
        query_hive = f"DESCRIBE SELECT * FROM read_parquet('{s3_path}', hive_partitioning = 1)"
        schema_hive = con.execute(query_hive).fetchall()
        cols_hive = [row[0].lower() for row in schema_hive]

        cols_virtuais = list(set(cols_hive) - set(cols_reais))
        
        # --- VALIDAÇÕES ESPECÍFICAS CAGED ---
        # No CAGED, procuramos por saldo de movimentação e salário
        tem_saldo = any('saldomovimentacao' in c for c in cols_reais)
        tem_salario = any('valor_salario' in c or 'salario' in c for c in cols_reais)
        tem_cbo = any('cbo' in c for c in cols_reais)
        
        # Se tem saldo e CBO, é quase certeza que é a tabela de MOVIMENTAÇÕES
        eh_movimentacao = tem_saldo and tem_cbo

        str_reais = ", ".join(cols_reais)
        str_virtuais = ", ".join(cols_virtuais)
        
        return eh_movimentacao, tem_salario, len(cols_reais), len(cols_virtuais), str_reais, str_virtuais
        
    except Exception as e:
        print(f"⚠️ Erro ao ler {s3_path}: {e}")
        return False, False, 0, 0, "", ""

def main():
    files = get_all_parquet_files()
    if not files: return

    print(f"\n🚀 Iniciando auditoria em {len(files)} arquivos...")

    with open(OUTPUT_FILE, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.writer(csvfile, delimiter=';')
        writer.writerow([
            'ARQUIVO', 'COMPETENCIA_PASTA', 'EH_MOVIMENTACAO', 'TEM_SALARIO', 
            'QTD_COLS_REAIS', 'QTD_COLS_VIRTUAIS', 'STATUS', 'COLUNAS_REAIS', 'COLUNAS_VIRTUAIS'
        ])

        for i, f_path in enumerate(files):
            # Tenta extrair a competência/ano da pasta (ex: mes=202301 ou ano=2023)
            parts = f_path.split('/')
            competencia = "N/A"
            for p in parts:
                if "mes=" in p or "competencia=" in p or "ano=" in p:
                    competencia = p.split('=')[-1]
                    break

            is_mov, has_sal, q_real, q_virt, n_reais, n_virt = check_file_content(f_path)
            
            status = "✅ MOVIMENTACAO" if is_mov else "⚠️ OUTRA_TABELA"
            
            writer.writerow([f_path, competencia, is_mov, has_sal, q_real, q_virt, status, n_reais, n_virt])
            
            if i % 10 == 0:
                print(f"[{i+1}/{len(files)}] Auditando: {competencia} - {status}")

    print(f"\n✅ Auditoria concluída! Resultado em: {OUTPUT_FILE}")

if __name__ == "__main__":
    main()