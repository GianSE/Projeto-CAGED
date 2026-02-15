import duckdb
import os
import csv
import time

# --- CONFIGURAÇÕES ---
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "host.docker.internal:9000")
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
    Abre o cabeçalho do arquivo e verifica palavras-chave.
    Retorna: (Tem_Remuneracao, Tem_CBO, Lista_Colunas_Chave)
    """
    try:
        # Lê apenas o schema (rápido)
        query = f"DESCRIBE SELECT * FROM read_parquet('{s3_path}')"
        schema = con.execute(query).fetchall()
        
        # Normaliza nomes das colunas para minúsculo
        cols = [row[0].lower() for row in schema]
        
        # Verifica palavras-chave
        # Procura por variações como: "vl remun", "remuneracao", "valor remuneracao"
        tem_remun = any(('remun' in c or 'salario' in c) for c in cols)
        
        # Procura por CBO (indicativo forte de dados de trabalhador)
        tem_cbo = any('cbo' in c for c in cols)
        
        # Procura colunas de identificação de empresa (indicativo de arquivo de Estabelecimento)
        # Geralmente arquivos de Estabelecimento SÓ têm "cnpj", "razao", "qtd vinculos"
        # Se tiver APENAS qtd vinculos e NÃO tiver remuneração, é Estabelecimento.
        
        return tem_remun, tem_cbo, len(cols)
        
    except Exception as e:
        print(f"⚠️ Erro ao ler {s3_path}: {e}")
        return False, False, 0

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
        # Cabeçalho do CSV
        writer.writerow(['ARQUIVO', 'ANO', 'TEM_REMUNERACAO', 'TEM_CBO', 'QTD_COLUNAS', 'STATUS_PROVAVEL'])

        for i, f_path in enumerate(files):
            # Extrai o ano do caminho (ex: .../ano=2024/...)
            parts = f_path.split('/')
            ano = "Desc"
            for p in parts:
                if "ano=" in p:
                    ano = p.replace("ano=", "")
                    break

            # Verifica o conteúdo
            has_remun, has_cbo, col_count = check_file_content(f_path)
            
            # Define o Status
            if has_remun or has_cbo:
                status = "✅ VINCULOS (OK)"
                files_good += 1
            else:
                status = "❌ ESTABELECIMENTOS (SEM DADOS)"
                files_bad += 1
            
            # Escreve no CSV
            writer.writerow([f_path, ano, "SIM" if has_remun else "NAO", "SIM" if has_cbo else "NAO", col_count, status])

            # Feedback visual no terminal a cada 10 arquivos
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