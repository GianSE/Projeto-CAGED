"""
Bronze RAIS — FTP do MTE -> Gianpedro/bronze-rais, sem tradução, sem
filtro de TI (espelho bruto do arquivo original, em parquet ZSTD-3).

Dois modos, um script:
    python bronze-rais.py --modo verificar [--ano-inicio N] [--ano-fim N]
    python bronze-rais.py --modo processar --tabela X --ano-inicio Y --ano-fim Y
"""
import argparse
import json
import os
import shutil
import sys
import time
import traceback
from pathlib import Path

_AQUI = Path(__file__).resolve().parent
sys.path.insert(0, str(_AQUI))
sys.path.insert(0, str(_AQUI.parent))

from extracao_ftp import extrator, heartbeat  # noqa: E402
from extracao_ftp.catalogo import ItemTrabalho, descobrir, resumir  # noqa: E402
from extracao_ftp.config_extracao import DIR_DOWNLOAD, DIR_EXTRAIDO, preparar_diretorios  # noqa: E402
from extracao_ftp.estado import EstadoLake, Manifesto  # noqa: E402
from extracao_ftp.ftp_utils import ClienteFTP  # noqa: E402
from extracao_ftp.transformador import converter  # noqa: E402
from _utils import nuvem, telegram  # noqa: E402


def _argumentos():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--modo", choices=("verificar", "processar"), required=True)
    p.add_argument("--tabela", nargs="+", default=None)
    p.add_argument("--ano-inicio", type=int, default=0)
    p.add_argument("--ano-fim", type=int, default=9999)
    p.add_argument("--forcar", action="store_true")
    return p.parse_args()


def modo_verificar(args) -> int:
    ano_inicio = args.ano_inicio or (time.localtime().tm_year - 1)
    ano_fim = args.ano_fim if args.ano_fim != 9999 else time.localtime().tm_year
    print(f"🔎 Varrendo o FTP (rais, {ano_inicio}-{ano_fim})...")

    cliente = ClienteFTP()
    cliente.conectar()
    itens = descobrir(cliente, ["rais"], ano_inicio, ano_fim)
    cliente.fechar()
    print(f"   {len(itens)} arquivo(s) no catálogo do FTP")

    estado = EstadoLake()
    faltantes = [i for i in itens if not estado.ja_existe(i)]
    print(f"   {len(faltantes)} arquivo(s) ainda não publicado(s)")

    pares = sorted({(i.tabela, i.ano) for i in faltantes})
    saida = {"tem_novidade": bool(pares),
             "anos": [{"tabela": t, "ano": a} for t, a in pares]}
    print(json.dumps(saida, ensure_ascii=False, indent=2))

    github_output = os.getenv("GITHUB_OUTPUT")
    if github_output:
        with open(github_output, "a", encoding="utf-8") as f:
            f.write(f"tem_novidade={str(saida['tem_novidade']).lower()}\n")
            f.write(f"anos={json.dumps(saida['anos'])}\n")

    if saida["tem_novidade"]:
        telegram.notificar("🔎 *Bronze RAIS*: novidade encontrada, baixando agora.")
    else:
        telegram.notificar("🔎 *Bronze RAIS*: verificado, já em dia.")
    return 0


def _limpar_temporarios(compactado, arquivos_extraidos) -> None:
    if compactado and compactado.exists():
        compactado.unlink(missing_ok=True)
    for arquivo in arquivos_extraidos:
        if arquivo.exists():
            arquivo.unlink(missing_ok=True)
    shutil.rmtree(DIR_EXTRAIDO, ignore_errors=True)
    DIR_EXTRAIDO.mkdir(parents=True, exist_ok=True)


def _processar_item(item: ItemTrabalho, cliente: ClienteFTP, con, manifesto: Manifesto,
                    indice: int, total: int) -> bool:
    inicio = time.time()
    compactado = DIR_DOWNLOAD / item.nome_arquivo
    extraidos: list = []
    try:
        heartbeat.escrever(indice=indice, total=total, tabela=item.tabela, ano=item.ano,
                           mes=item.mes, recorte=item.recorte, arquivo=item.nome_arquivo,
                           rotulo=item.rotulo, tamanho_mb=round(item.tamanho / 1e6, 1),
                           iniciado_em=inicio, etapa="baixando")
        if not cliente.baixar(item.caminho_remoto, compactado, item.tamanho):
            manifesto.registrar(item, 0, time.time() - inicio, "erro", "download falhou")
            return False

        print("      📦 Descompactando...")
        extraidos = extrator.extrair(compactado, DIR_EXTRAIDO)
        if not extraidos:
            manifesto.registrar(item, 0, time.time() - inicio, "erro", "sem arquivo de dados")
            return False

        print(f"      🔄 Gravando {item.destino_rel}")
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
        _limpar_temporarios(compactado, extraidos)


def modo_processar(args) -> int:
    preparar_diretorios()
    print(f"📥 Processando {', '.join(args.tabela or ['(todas)'])} "
          f"{args.ano_inicio}-{args.ano_fim}")

    cliente = ClienteFTP()
    try:
        cliente.conectar()
    except Exception as e:
        print(f"❌ Não consegui conectar no FTP: {e}")
        telegram.notificar(f"❌ *Bronze RAIS*: falha ao conectar no FTP: {e}")
        return 1

    itens = descobrir(cliente, ["rais"], args.ano_inicio, args.ano_fim, tabelas=args.tabela)
    resumir(itens)

    if not itens:
        cliente.fechar()
        return 0

    estado = EstadoLake()
    if not args.forcar:
        itens = [i for i in itens if not estado.ja_existe(i)]
    if not itens:
        print("✨ Nada a fazer: já está em dia.")
        cliente.fechar()
        return 0

    con = nuvem.conectar_duckdb()
    manifesto = Manifesto()
    sucessos = falhas = 0

    for n, item in enumerate(itens, start=1):
        print(f"\n[{n}/{len(itens)}] 🎯 {item.rotulo}  ({item.tamanho / 1e6:.0f} MB)")
        if _processar_item(item, cliente, con, manifesto, n, len(itens)):
            sucessos += 1
        else:
            falhas += 1

    heartbeat.limpar()
    cliente.fechar()
    con.close()

    print(f"\n🏁 {sucessos} ok, {falhas} falha(s)")
    icone = "🏁" if falhas == 0 else "⚠️"
    telegram.notificar(f"{icone} *Bronze RAIS*: {sucessos} ok, {falhas} falha(s) "
                       f"({', '.join(args.tabela or [])} {args.ano_inicio})")
    return 0 if falhas == 0 else 2


def main() -> int:
    args = _argumentos()
    if args.modo == "verificar":
        return modo_verificar(args)
    return modo_processar(args)


if __name__ == "__main__":
    sys.exit(main())
