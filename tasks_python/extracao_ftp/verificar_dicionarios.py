"""
Verifica se o FTP tem planilha de layout/dicionário nova — sem extrair
nada. A extração de verdade (`extrair_dicionarios`) ainda só escreve no
MinIO local (ver bootstrap_dicionarios_hf.py), então este script só avisa;
quem publica é você, manualmente, quando quiser.

Compara a CONTAGEM de planilhas por pasta contra `_contagem_planilhas.json`
(publicado pelo bootstrap) — não o conteúdo, não precisa baixar nada do
FTP além da listagem de diretório.

Uso:
    python -m extracao_ftp.verificar_dicionarios
"""
import json
import os
import sys

from extracao_ftp import nuvem
from extracao_ftp.dicionarios import PASTAS_LAYOUT, _listar_planilhas
from extracao_ftp.ftp_utils import ClienteFTP


def main() -> int:
    from huggingface_hub import hf_hub_download

    try:
        caminho = hf_hub_download(nuvem.REPO_DICIONARIOS, "_contagem_planilhas.json",
                                  repo_type="dataset")
        contagem_publicada = json.loads(open(caminho, encoding="utf-8").read())
    except Exception as e:
        print(f"⚠️  não consegui ler a contagem publicada: {e}")
        contagem_publicada = {}

    cliente = ClienteFTP()
    cliente.conectar()
    mudou = []
    for pasta, recursivo in PASTAS_LAYOUT:
        atual = len(_listar_planilhas(cliente, pasta, recursivo))
        anterior = contagem_publicada.get(pasta)
        print(f"   {pasta}: {atual} planilha(s) no FTP "
              f"(publicado: {anterior if anterior is not None else '?'})")
        if anterior is not None and atual != anterior:
            mudou.append((pasta, anterior, atual))
    cliente.fechar()

    github_output = os.getenv("GITHUB_OUTPUT")
    tem_novidade = bool(mudou)
    if github_output:
        with open(github_output, "a", encoding="utf-8") as f:
            f.write(f"tem_novidade={str(tem_novidade).lower()}\n")

    if mudou:
        print("\n⚠️  Pasta(s) com contagem diferente da publicada:")
        for pasta, antes, agora in mudou:
            print(f"   {pasta}: {antes} -> {agora}")
        print("\n   Rode manualmente na sua máquina (com o MinIO local):")
        print("   python -m extracao_ftp.run_extracao --so-dicionarios")
        print("   python -m extracao_ftp.bootstrap_dicionarios_hf")
    else:
        print("\n✅ Nenhuma planilha nova — dicionário publicado está em dia.")

    return 0


if __name__ == "__main__":
    sys.exit(main())
