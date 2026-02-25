import duckdb
import os
import csv
import time

# --- CONFIGURAÇÕES ---
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "localhost:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
MINIO_BUCKET = "bronze"

OUTPUT_FILE = "auditoria_arquivos_detalhada.csv"

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
    """Lista todos os parquets recursivamente"""
    print("🔍 Listando todos os arquivos no MinIO (isso pode levar alguns segundos)...")
    try:
        query = f"SELECT file FROM glob('s3://{MINIO_BUCKET}/rais/**/*.parquet')"
        result = con.execute(query).fetchall()
        files = [row[0] for row in result]
        print(f"✅ Total de arquivos encontrados: {len(files)}")
        return sorted(files)
    except Exception as e:
        print(f"❌ Erro ao listar arquivos: {e}")
        return []

def check_file_content(s3_path):
    """
    Identifica colunas REAIS vs VIRTUAIS (Hive)
    Retorna: (has_remun, has_cbo, total_real, total_virtual, lista_real, lista_virtual)
    """
    try:
        # 1. PEGA O SCHEMA REAL (SÓ O QUE ESTÁ DENTRO DO PARQUET)
        query_real = f"DESCRIBE SELECT * FROM read_parquet('{s3_path}', hive_partitioning = 0)"
        schema_real = con.execute(query_real).fetchall()
        cols_reais = [row[0].lower() for row in schema_real]
        
        # 2. PEGA O SCHEMA HIVE (O QUE O DUCKDB MONTA COM AS PASTAS)
        query_hive = f"DESCRIBE SELECT * FROM read_parquet('{s3_path}', hive_partitioning = 1)"
        schema_hive = con.execute(query_hive).fetchall()
        cols_hive = [row[0].lower() for row in schema_hive]

        # Identifica o que é puramente virtual (está no hive mas não no real)
        cols_virtuais = list(set(cols_hive) - set(cols_reais))
        
        # Validações de conteúdo (nas colunas REAIS para não ter falso positivo)
        tem_remun = any(('remun' in c or 'salario' in c) for c in cols_reais)
        tem_cbo = any('cbo' in c for c in cols_reais)
        
        # Formata strings para o CSV
        str_reais = ", ".join(cols_reais)
        str_virtuais = ", ".join(cols_virtuais)
        
        return tem_remun, tem_cbo, len(cols_reais), len(cols_virtuais), str_reais, str_virtuais
        
    except Exception as e:
        print(f"⚠️ Erro ao ler {s3_path}: {e}")
        return False, False, 0, 0, "", ""

def main():
    files = get_all_parquet_files()
    if not files:
        return

    print(f"\n🚀 Iniciando verificação PENTE FINO em {len(files)} arquivos...")
    print(f"💾 O resultado será salvo em: {OUTPUT_FILE}\n")

    files_good = 0
    files_bad = 0

    with open(OUTPUT_FILE, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.writer(csvfile, delimiter=';')
        
        # --- NOVO: Cabeçalho do CSV atualizado com a coluna LISTA_DE_COLUNAS ---
        writer.writerow(['ARQUIVO', 'ANO', 'TEM_REMUNERACAO', 'TEM_CBO', 'QTD_COLUNAS', 'STATUS_PROVAVEL', 'LISTA_DE_COLUNAS'])

        for i, f_path in enumerate(files):
            parts = f_path.split('/')
            ano = "Desc"
            for p in parts:
                if "ano=" in p or "ano_hive=" in p: # Ajustado para capturar ano_hive também
                    ano = p.replace("ano=", "").replace("ano_hive=", "")
                    break

            # AJUSTE AQUI: Agora recebemos 6 valores da função
            has_remun, has_cbo, qtd_real, qtd_virt, nomes_reais, nomes_virt = check_file_content(f_path)
            
            if has_remun or has_cbo:
                status = "✅ VINCULOS (OK)"
                files_good += 1
            else:
                status = "❌ ESTABELECIMENTOS (SEM DADOS)"
                files_bad += 1
            
            # ATUALIZE O writerow: Adicione as novas colunas no CSV
            writer.writerow([
                f_path, 
                ano, 
                "SIM" if has_remun else "NAO", 
                "SIM" if has_cbo else "NAO", 
                qtd_real,       # Nova coluna: quantidade real
                qtd_virt,       # Nova coluna: quantidade virtual
                status, 
                nomes_reais,    # Nova coluna: nomes reais
                nomes_virt      # Nova coluna: nomes virtuais
            ])
            
            if i % 10 == 0:
                print(f"[{i+1}/{len(files)}] Processando: {ano} - {status} ...")

    print(f"\n{'='*60}")
    print(f"📊 RELATÓRIO FINAL")
    print(f"{'='*60}")
    print(f"✅ Arquivos com Dados de Trabalhador (ÚTEIS): {files_good}")
    print(f"❌ Arquivos de Estabelecimento (LIXO/OUTROS): {files_bad}")
    print(f"📂 Detalhes salvos em: {os.path.abspath(OUTPUT_FILE)}")
    print(f"{'='*60}")
    print("DICA: Abra o CSV e filtre pela coluna 'STATUS_PROVAVEL' para pegar os arquivos '❌' e deletar.")

if __name__ == "__main__":
    main()