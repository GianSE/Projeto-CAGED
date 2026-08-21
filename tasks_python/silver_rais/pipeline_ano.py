"""
Traduz e publica a RAIS um ano por vez, liberando o disco entre um e outro.

POR QUE ANO A ANO
-----------------
A silver completa do rais_vinc passa de 50 GB, e ela precisaria conviver com os
34 GB do bronze e com um espelho local do mesmo tamanho para o upload. Não cabe
— nem hoje, nem comprando disco no ritmo em que a base cresce.

Fazendo um ano de cada vez, o pico é o tamanho de UM ano em três cópias
(~3 GB cada), e não da base inteira. O ciclo por ano:

    1. constrói a silver daquele ano  (bronze -> MinIO)
    2. espelha só aquele ano          (MinIO -> disco)
    3. publica                         (disco -> Hugging Face)
    4. apaga o espelho local
    5. apaga a silver daquele ano no MinIO

O passo 5 costuma ser esquecido e é o que realmente enche o disco: o MinIO roda
em container NESTA máquina, então a silver ocupa o mesmo disco que o espelho.
Apagar só a pasta local resolveria metade do problema.

Nada disso perde dado: o bronze continua intacto (é a fonte da verdade, e a
silver é re-derivável a qualquer momento) e o ano já está publicado no Hub antes
de qualquer remoção. A limpeza só acontece se a publicação retornar sucesso.

Uso:
    python -m silver_rais.pipeline_ano --repo Gianpedro/rais-microdados-traduzidos
    python -m silver_rais.pipeline_ano --repo ... --ano-inicio 2010 --ano-fim 2012
    python -m silver_rais.pipeline_ano --repo ... --manter-silver   (não apaga do MinIO)
"""
import argparse
import re
import sys
import time
from pathlib import Path

from extracao_ftp.config_extracao import (
    BUCKET_BRONZE,
    BUCKET_SILVER,
    conectar_duckdb,
)
from silver_caged import publicar_hf as pub
from silver_rais import construir_silver as cs

TABELA = "rais_vinc"


def anos_no_bronze(fs, tabela: str) -> list[int]:
    """Anos presentes no bronze, lidos do caminho dos arquivos."""
    anos = set()
    for caminho in fs.glob(f"{BUCKET_BRONZE}/{tabela}/**/*.parquet"):
        m = re.search(r"ano=(\d{4})", caminho)
        if m:
            anos.add(int(m.group(1)))
    return sorted(anos)


def _tamanho_local(pasta: Path) -> float:
    return sum(a.stat().st_size for a in pasta.rglob("*.parquet")) / 1e9 if pasta.exists() else 0.0


def publicar_ano(con, fs, api, repo: str, ano: int, tabela: str,
                 manter_silver: bool) -> bool:
    """Um ciclo completo para um ano. Devolve False se algo falhou."""
    print(f"\n{'=' * 70}\n  📅 {tabela} — ano {ano}\n{'=' * 70}")

    # 1. traduzir
    if not cs.construir(con, fs, tabela, so_tecnologia=False,
                        ano_inicio=ano, ano_fim=ano):
        print(f"   ❌ {ano}: falha na construção da silver, ano preservado")
        return False

    # 2. espelhar só este ano
    print(f"   📥 Espelhando ano {ano}")
    baixados, pulados = pub.espelhar(fs, [tabela], pub.DIR_LOCAL, anos=[ano])
    tamanho = _tamanho_local(pub.DIR_LOCAL / tabela)
    print(f"   ✅ {baixados} baixado(s), {pulados} já no espelho · {tamanho:.2f} GB")

    if not baixados and not pulados:
        print(f"   ⏭️  {ano}: nada na silver, pulando")
        return True

    # 3. publicar
    print(f"   ⬆️  Enviando ano {ano} para {repo}")
    api.upload_large_folder(folder_path=str(pub.DIR_LOCAL), repo_id=repo,
                            repo_type="dataset", print_report=True)

    # 4 e 5. limpar as duas cópias, agora que o ano está no Hub
    pasta_local = pub.DIR_LOCAL / tabela / f"ano_particao={ano}"
    if pasta_local.exists():
        import shutil

        shutil.rmtree(pasta_local)
        print(f"   🧹 espelho local de {ano} removido ({tamanho:.2f} GB)")

    if not manter_silver:
        prefixo = f"{BUCKET_SILVER}/{tabela}/ano_particao={ano}"
        if fs.exists(prefixo):
            fs.rm(prefixo, recursive=True)
            print(f"   🧹 silver de {ano} removida do MinIO")

    return True


def main() -> int:
    p = argparse.ArgumentParser(description="Traduz e publica a RAIS ano a ano.")
    p.add_argument("--repo", required=True)
    p.add_argument("--tabela", default=TABELA, choices=("rais_vinc", "rais_estab"))
    p.add_argument("--ano-inicio", type=int, default=0)
    p.add_argument("--ano-fim", type=int, default=9999)
    p.add_argument("--manter-silver", action="store_true",
                   help="Não apaga a silver do MinIO depois de publicar. "
                        "Use só se houver disco sobrando — ela é re-derivável do bronze.")
    args = p.parse_args()

    # O publicador guarda o espelho por camada; aqui é sempre o da RAIS.
    pub.DIR_LOCAL = pub.DIRS["rais"]
    pub.DIR_LOCAL.mkdir(parents=True, exist_ok=True)

    con = conectar_duckdb()
    con.execute("SET enable_progress_bar=false")
    fs = cs._fs_minio()

    todos = anos_no_bronze(fs, args.tabela)
    anos = [a for a in todos if args.ano_inicio <= a <= args.ano_fim]
    if not anos:
        print(f"❌ Nenhum ano de {args.tabela} no bronze dentro da faixa pedida.")
        return 1

    token = pub._credencial()
    from huggingface_hub import HfApi, get_token

    if not token and not get_token():
        print("❌ Nenhuma credencial do Hugging Face.")
        return 1
    api = HfApi(token=token)
    api.create_repo(repo_id=args.repo, repo_type="dataset", exist_ok=True)

    print(f"🔁 {args.tabela}: {len(anos)} ano(s) — {anos[0]} a {anos[-1]}")
    print(f"   destino: https://huggingface.co/datasets/{args.repo}\n")

    inicio = time.time()
    falhas = []
    for n, ano in enumerate(anos, start=1):
        # Formato "[n/N]" de propósito: é o que o painel lê para montar a barra
        # de progressão do job (painel/processos.py:_progresso).
        print(f"\n   [{n}/{len(anos)}] ano {ano}")
        if not publicar_ano(con, fs, api, args.repo, ano, args.tabela,
                            args.manter_silver):
            falhas.append(ano)

    # O card é reescrito no fim, quando a tabela de arquivos já reflete tudo o
    # que foi publicado — regerá-lo a cada ano mostraria só o ano corrente, já
    # que o espelho é esvaziado a cada volta.
    print("\n📝 Atualizando card e dimensões")
    from silver_caged.dimensoes import gerar

    gerar("rais", pub.DIR_LOCAL / "dicionarios.parquet")

    print(f"\n🏁 {len(anos) - len(falhas)}/{len(anos)} ano(s) em "
          f"{(time.time() - inicio) / 60:.0f} min")
    if falhas:
        print(f"   ⚠️  anos com falha (silver preservada): {falhas}")
    print(f"   https://huggingface.co/datasets/{args.repo}")
    return 0 if not falhas else 2


if __name__ == "__main__":
    sys.exit(main())
