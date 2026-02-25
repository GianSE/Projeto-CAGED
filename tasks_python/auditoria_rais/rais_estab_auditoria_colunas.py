import duckdb
import os
import csv

# --- CONFIGURAÇÕES ---
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "localhost:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
MINIO_BUCKET = "bronze"

OUTPUT_FILE = "auditoria_estab_detalhada.csv"

con = duckdb.connect()
con.execute("INSTALL httpfs; LOAD httpfs;")
con.execute(f"""
    SET s3_endpoint='{MINIO_ENDPOINT}';
    SET s3_access_key_id='{MINIO_ACCESS_KEY}';
    SET s3_secret_access_key='{MINIO_SECRET_KEY}';
    SET s3_use_ssl=false;
    SET s3_url_style='path';
""")

def audit_columns(s3_path):
    try:
        # 1. Colunas REAIS (o que está gravado dentro do arquivo)
        res_real = con.execute(f"DESCRIBE SELECT * FROM read_parquet('{s3_path}', hive_partitioning = 0)").fetchall()
        cols_reais = {row[0].lower() for row in res_real}

        # 2. Colunas TOTAIS (Reais + o que o Hive "inventa" das pastas)
        res_hive = con.execute(f"DESCRIBE SELECT * FROM read_parquet('{s3_path}', hive_partitioning = 1)").fetchall()
        cols_hive = {row[0].lower() for row in res_hive}

        # Identificação
        cols_virtuais = cols_hive - cols_reais
        
        # Auditoria de conteúdo (apenas no que é REAL)
        # Se as colunas de remuneração forem REAIS, o arquivo está na pasta errada!
        tem_vinculo_real = any(('remun' in c or 'salario' in c or 'cbo' in c) for c in cols_reais)
        
        # Se o CNPJ for REAL, o arquivo está no lugar certo
        tem_estab_real = any(('cnpj' in c or 'cnae' in c) for c in cols_reais)

        return {
            "reais": ", ".join(sorted(list(cols_reais))),
            "virtuais": ", ".join(sorted(list(cols_virtuais))),
            "tem_vinculo_real": tem_vinculo_real,
            "tem_estab_real": tem_estab_real,
            "qtd_reais": len(cols_reais),
            "qtd_virtuais": len(cols_virtuais)
        }
    except Exception as e:
        return None

def main():
    print("🔍 Escaneando MinIO...")
    files = [row[0] for row in con.execute(f"SELECT file FROM glob('s3://{MINIO_BUCKET}/rais_estab/**/*.parquet')").fetchall()]
    
    if not files:
        print("❌ Nenhum arquivo encontrado.")
        return

    with open(OUTPUT_FILE, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.writer(csvfile, delimiter=';')
        writer.writerow(['ARQUIVO', 'STATUS', 'DADOS_VINCULO_NO_ARQUIVO', 'QTD_REAIS', 'QTD_VIRTUAIS', 'COLUNAS_VIRTUAIS', 'COLUNAS_REAIS'])

        for f_path in sorted(files):
            data = audit_columns(f_path)
            if not data: continue

            # Lógica de status:
            if data['tem_vinculo_real']:
                status = "🚨 ERRO: CONTÉM DADOS DE VÍNCULO"
            elif data['tem_estab_real']:
                status = "✅ OK: ESTABELECIMENTO PURO"
            else:
                status = "⚠️ ATENÇÃO: ARQUIVO VAZIO OU ESTRANHO"

            writer.writerow([
                f_path, 
                status, 
                "SIM" if data['tem_vinculo_real'] else "NAO",
                data['qtd_reais'],
                data['qtd_virtuais'],
                data['virtuais'], # Mostra o que veio da estrutura de pastas
                data['reais']     # Mostra o que veio do Parquet
            ])
            print(f"Auditado: {f_path.split('/')[-1]} -> {status}")

    print(f"\n✅ Auditoria completa! Detalhes em: {OUTPUT_FILE}")

if __name__ == "__main__":
    main()