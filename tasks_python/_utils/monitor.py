import time
import threading
import mariadb

class DBMonitor:
    def __init__(self, db_config):
        self.db_config = db_config
        self.stop_event = threading.Event()
        self.thread = None

    def _monitor_loop(self, table_name, total_expected_bytes):
        conn = None
        try:
            # 1. Conecta UMA vez antes do loop
            conn = mariadb.connect(**self.db_config)
            cursor = conn.cursor()
            print(f"\n📊 Monitor iniciado para tabela: {table_name}")
            
            start_time = time.time()
            
            while not self.stop_event.is_set():
                try:
                    # Verifica conexão
                    conn.ping()
                    
                    sql = f"""
                    SELECT data_length, table_rows 
                    FROM information_schema.tables 
                    WHERE table_schema = '{self.db_config['database']}' 
                    AND table_name = '{table_name}'
                    """
                    cursor.execute(sql)
                    result = cursor.fetchone()
                    
                    if result:
                        bytes_loaded = result[0] or 0
                        rows = result[1] or 0
                        mb_loaded = bytes_loaded / (1024 * 1024)
                        
                        # Cálculo Visual
                        pct = 0.0
                        if total_expected_bytes > 0:
                            pct = min((bytes_loaded / total_expected_bytes) * 100, 99.9)
                        
                        elapsed = time.time() - start_time
                        speed = mb_loaded / elapsed if elapsed > 0 else 0
                        
                        print(f"\r⏳ Carregando: {mb_loaded:,.2f} MB ({pct:.1f}%) | Linhas: {rows:,} | Vel: {speed:.1f} MB/s", end="")
                    
                except Exception as e:
                    # Se der erro pontual, não quebra a thread, só avisa
                    print(f" (Erro leitura: {e})", end="")

                time.sleep(2)

        except Exception as e:
            print(f"\n⚠️ Erro fatal no monitor: {e}")
        finally:
            if conn: 
                conn.close()

    def start(self, table_name, total_bytes_csv):
        self.stop_event.clear()
        self.thread = threading.Thread(
            target=self._monitor_loop, 
            args=(table_name, total_bytes_csv)
        )
        # Daemon garante que se o script principal morrer, o monitor morre junto
        self.thread.daemon = True 
        self.thread.start()

    def stop(self):
        if self.thread and self.thread.is_alive():
            self.stop_event.set()
            self.thread.join()
            print("\n✅ Monitoramento finalizado.")