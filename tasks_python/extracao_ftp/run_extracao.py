"""
Orquestrador da extração CAGED/RAIS: FTP do MTE -> MinIO (bronze).

Fluxo por arquivo, um de cada vez, para manter o disco baixo:

    baixa .7z  ->  extrai  ->  trata + grava parquet ZSTD-3 no MinIO
                                     ->  apaga .7z e extraídos

Exemplos:
    # Ver o que seria feito, sem baixar nada
    python -m extracao_ftp.run_extracao --listar

    # Novo CAGED + RAIS de 2020 em diante (escopo moderno)
    python -m extracao_ftp.run_extracao --dataset novo_caged rais --ano-inicio 2020

    # Tudo que existe no FTP
    python -m extracao_ftp.run_extracao --ano-inicio 1985

    # Só a RAIS de vínculos, reprocessando o que já existe
    python -m extracao_ftp.run_extracao --tabela rais_vinc --forcar
"""
import argparse
import shutil
import sys
import time
import traceback

from extracao_ftp import extrator
from extracao_ftp.catalogo import DATASETS, ItemTrabalho, descobrir, resumir
from extracao_ftp.config_extracao import (
    DIR_DOWNLOAD,
    DIR_EXTRAIDO,
    MINIO_ENDPOINT,
    conectar_duckdb,
    preparar_diretorios,
)
from extracao_ftp import heartbeat
from extracao_ftp.dicionarios import extrair_dicionarios
from extracao_ftp.estado import EstadoLake, Manifesto
from extracao_ftp.ftp_utils import ClienteFTP
from extracao_ftp.transformador import converter

ANO_MIN_PADRAO = 2020
ANO_MAX_PADRAO = 2030


def _argumentos():
    p = argparse.ArgumentParser(
        description="Extrai microdados do CAGED/RAIS do FTP do MTE para o MinIO.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    p.add_argument("--dataset", nargs="+", choices=DATASETS, default=list(DATASETS),
                   help="Quais conjuntos varrer no FTP (padrão: todos)")
    p.add_argument("--tabela", nargs="+", default=None,
                   help="Filtra tabelas de destino (caged_mov, rais_vinc, ...)")
    p.add_argument("--ano-inicio", type=int, default=ANO_MIN_PADRAO,
                   help=f"Ano-base mínimo (padrão: {ANO_MIN_PADRAO})")
    p.add_argument("--ano-fim", type=int, default=ANO_MAX_PADRAO,
                   help=f"Ano-base máximo (padrão: {ANO_MAX_PADRAO})")
    p.add_argument("--incluir-parcial", action="store_true",
                   help="Inclui as pastas 'AAAA Parcial' da RAIS (padrão: ignora)")
    p.add_argument("--listar", action="store_true",
                   help="Só mostra o plano de extração e sai (dry-run)")
    p.add_argument("--forcar", action="store_true",
                   help="Reprocessa itens que já existem no MinIO")
    p.add_argument("--limite", type=int, default=None,
                   help="Processa no máximo N itens (útil para testar)")
    p.add_argument("--manter-temp", action="store_true",
                   help="Não apaga o .7z e os extraídos (debug; consome muito disco)")
    p.add_argument("--dicionarios", action="store_true",
                   help="Também extrai as planilhas de layout (tradução dos códigos)")
    p.add_argument("--so-dicionarios", action="store_true",
                   help="Extrai apenas as planilhas de layout e sai")
    return p.parse_args()


def _limpar_temporarios(compactado, arquivos_extraidos) -> None:
    """Apaga o .7z baixado e tudo que saiu dele."""
    liberado = 0
    if compactado and compactado.exists():
        liberado += compactado.stat().st_size
        compactado.unlink(missing_ok=True)
    for arquivo in arquivos_extraidos:
        if arquivo.exists():
            liberado += arquivo.stat().st_size
            arquivo.unlink(missing_ok=True)
    shutil.rmtree(DIR_EXTRAIDO, ignore_errors=True)
    DIR_EXTRAIDO.mkdir(parents=True, exist_ok=True)
    if liberado:
        print(f"      🧹 {liberado / 1e9:.2f} GB liberados do disco")


def processar_item(item: ItemTrabalho, cliente: ClienteFTP, con, manifesto: Manifesto,
                   manter_temp: bool, indice: int, total: int) -> bool:
    """Executa o ciclo completo de um item. Devolve True se gravou o parquet."""
    inicio = time.time()
    compactado = DIR_DOWNLOAD / item.nome_arquivo
    extraidos: list = []

    heartbeat_base = dict(
        indice=indice, total=total, tabela=item.tabela, ano=item.ano, mes=item.mes,
        recorte=item.recorte, arquivo=item.nome_arquivo, rotulo=item.rotulo,
        tamanho_mb=round(item.tamanho / 1e6, 1), iniciado_em=inicio,
    )

    try:
        # 1. Download
        heartbeat.escrever(**heartbeat_base, etapa="baixando")
        if not cliente.baixar(item.caminho_remoto, compactado, item.tamanho):
            manifesto.registrar(item, 0, time.time() - inicio, "erro", "download falhou")
            return False

        # 2. Descompactação
        heartbeat.escrever(**heartbeat_base, etapa="descompactando")
        print("      📦 Descompactando...")
        extraidos = extrator.extrair(compactado, DIR_EXTRAIDO)
        if not extraidos:
            manifesto.registrar(item, 0, time.time() - inicio, "erro",
                                "nenhum arquivo de dados no compactado")
            return False

        # 3. Tratamento + parquet no MinIO
        heartbeat.escrever(**heartbeat_base, etapa="gravando")
        print(f"      🔄 Gravando {item.destino_s3}")
        ok, linhas = converter(con, extraidos, item)

        segundos = time.time() - inicio
        if ok:
            print(f"      ✅ {linhas:,} linhas em {segundos:.0f}s")
            manifesto.registrar(item, linhas, segundos, "ok")
        else:
            manifesto.registrar(item, 0, segundos, "erro", "conversão falhou")
        return ok

    except Exception as e:
        print(f"      ❌ Erro inesperado: {e}")
        traceback.print_exc()
        manifesto.registrar(item, 0, time.time() - inicio, "erro", str(e)[:200])
        return False

    finally:
        # 4. Limpeza — sempre, mesmo se deu erro
        if not manter_temp:
            _limpar_temporarios(compactado, extraidos)


def main() -> int:
    args = _argumentos()
    preparar_diretorios()

    print("=" * 70)
    print("  EXTRAÇÃO DE MICRODADOS CAGED / RAIS  —  FTP do MTE -> MinIO")
    print("=" * 70)
    print(f"  MinIO....: {MINIO_ENDPOINT}")
    print(f"  Datasets.: {', '.join(args.dataset)}")
    print(f"  Anos.....: {args.ano_inicio} a {args.ano_fim}")
    if args.tabela:
        print(f"  Tabelas..: {', '.join(args.tabela)}")

    # --- descoberta ---
    cliente = ClienteFTP()
    try:
        cliente.conectar()
    except Exception as e:
        print(f"\n❌ Não consegui conectar no FTP: {e}")
        return 1

    # --- planilhas de layout / dicionário ---
    if args.dicionarios or args.so_dicionarios:
        try:
            extrair_dicionarios(cliente)
        except Exception as e:
            print(f"\n⚠️  Falha ao extrair dicionários: {e}")
            traceback.print_exc()
        if args.so_dicionarios:
            cliente.fechar()
            return 0

    itens = descobrir(cliente, args.dataset, args.ano_inicio, args.ano_fim,
                      args.incluir_parcial, args.tabela)
    resumir(itens)

    if args.listar:
        cliente.fechar()
        return 0

    if not itens:
        cliente.fechar()
        return 0

    # --- estado atual do lake ---
    estado = EstadoLake()
    if not estado.testar_conexao():
        cliente.fechar()
        return 1

    if not args.forcar:
        antes = len(itens)
        itens = [i for i in itens if not estado.ja_existe(i)]
        pulados = antes - len(itens)
        if pulados:
            print(f"\n⏭️  {pulados} item(ns) já existem no MinIO e serão pulados "
                  f"(use --forcar para reprocessar)")

    if args.limite:
        itens = itens[: args.limite]
        print(f"🔢 Limitado a {len(itens)} item(ns) por --limite")

    if not itens:
        print("\n✨ Nada a fazer: o lake já está em dia.")
        cliente.fechar()
        return 0

    # --- execução ---
    con = conectar_duckdb()
    manifesto = Manifesto()
    sucessos = falhas = 0
    inicio_geral = time.time()

    print(f"\n{'=' * 70}")
    print(f"  Processando {len(itens)} item(ns)")
    print(f"{'=' * 70}")

    for n, item in enumerate(itens, start=1):
        print(f"\n[{n}/{len(itens)}] 🎯 {item.rotulo}  ({item.tamanho / 1e6:.0f} MB)")
        if processar_item(item, cliente, con, manifesto, args.manter_temp, n, len(itens)):
            sucessos += 1
        else:
            falhas += 1

    heartbeat.limpar()
    cliente.fechar()
    con.close()

    decorrido = time.time() - inicio_geral
    print(f"\n{'=' * 70}")
    print(f"  FIM — {sucessos} ok, {falhas} falha(s) em {decorrido / 60:.1f} min")
    print(f"  Manifesto: {manifesto.caminho}")
    print(f"{'=' * 70}")
    return 0 if falhas == 0 else 2


if __name__ == "__main__":
    sys.exit(main())
