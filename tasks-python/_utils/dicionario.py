"""
Dicionário do MTE (CBO, CNAE, escolaridade, ...) — as planilhas de layout
publicadas no FTP trazem as tabelas de/para que traduzem os códigos dos
microdados. Sem elas o parquet da silver fica ilegível.

Dois modos, um script:
    python dicionario.py --modo verificar
        Só lista o FTP (barato) e compara a CONTAGEM de planilhas por
        pasta contra a última extração publicada — sem baixar nada.
        Dicionário muda raramente; isso é um alerta, não uma garantia.

    python dicionario.py --modo extrair
        Baixa, converte cada aba em parquet e publica em
        Gianpedro/mte-dicionarios — direto do FTP pro Hub, sem MinIO no
        meio. Dicionário errado é o jeito mais silencioso de quebrar a
        tradução, então isto é manual — rode quando o alerta disparar.
"""
import argparse
import json
import os
import re
import shutil
import sys
import unicodedata
from pathlib import Path

_AQUI = Path(__file__).resolve().parent
sys.path.insert(0, str(_AQUI.parent))
sys.path.insert(0, str(_AQUI.parent / "1-bronze"))

from extracao_ftp.config_extracao import FTP_BASE, FTP_HOST  # noqa: E402
from extracao_ftp.ftp_utils import ClienteFTP  # noqa: E402
from _utils import nuvem, telegram  # noqa: E402

PASTAS_LAYOUT = [
    ("NOVO CAGED", False),
    ("NOVO CAGED/Legado/Estabelecimentos", False),
    ("NOVO CAGED/Legado/Movimentações", False),
    ("CAGED", False),
    ("CAGED_AJUSTES", False),
    ("RAIS/Layouts", True),
    ("TRABALHO_DOMESTICO", False),
]
EXTENSOES_PLANILHA = (".xls", ".xlsx", ".xlsm")
DIR_LOCAL = Path(os.getenv("STAGING_DIR") or ".") / "_dicionario_espelho"
ARQUIVO_CONTAGEM = "_contagem_planilhas.json"


def _slug(texto: str) -> str:
    texto = unicodedata.normalize("NFKD", str(texto))
    texto = "".join(c for c in texto if not unicodedata.combining(c))
    texto = re.sub(r"[^0-9a-zA-Z]+", "_", texto).strip("_").lower()
    return texto or "sem_nome"


def _listar_planilhas(cliente: ClienteFTP, pasta: str, recursivo: bool) -> list[tuple[str, str]]:
    """Devolve [(caminho_remoto, nome)] das planilhas em uma pasta do FTP."""
    caminho = f"{FTP_BASE}/{pasta}"
    achados = []
    for nome in cliente.listar(caminho):
        completo = f"{caminho}/{nome}"
        if nome.lower().endswith(EXTENSOES_PLANILHA):
            achados.append((completo, nome))
        elif recursivo and "." not in nome:
            for sub in cliente.listar(completo):
                if sub.lower().endswith(EXTENSOES_PLANILHA):
                    achados.append((f"{completo}/{sub}", sub))
    return achados


def _argumentos():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--modo", choices=("verificar", "extrair"), required=True)
    return p.parse_args()


def modo_verificar() -> int:
    from huggingface_hub import hf_hub_download

    try:
        caminho = hf_hub_download(nuvem.REPO_DICIONARIOS, ARQUIVO_CONTAGEM, repo_type="dataset")
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
        print(f"   {pasta}: {atual} planilha(s) no FTP (publicado: {anterior})")
        if anterior is not None and atual != anterior:
            mudou.append((pasta, anterior, atual))
    cliente.fechar()

    github_output = os.getenv("GITHUB_OUTPUT")
    if github_output:
        with open(github_output, "a", encoding="utf-8") as f:
            f.write(f"tem_novidade={str(bool(mudou)).lower()}\n")

    if mudou:
        detalhe = "; ".join(f"{p}: {a}->{d}" for p, a, d in mudou)
        print(f"\n⚠️  Mudou: {detalhe}")
        print("   Rode: python dicionario.py --modo extrair")
        telegram.notificar(f"⚠️ *Dicionário MTE*: planilha nova no FTP ({detalhe}) — rode a extração.")
    else:
        print("\n✅ Nenhuma planilha nova.")
        telegram.notificar("✅ *Dicionário MTE*: sem planilha nova, publicado está em dia.")
    return 0


def _converter_planilha(caminho_local: Path, origem_slug: str, dir_saida: Path,
                        caminho_ftp: str = "") -> int:
    import pandas as pd

    try:
        abas = pd.read_excel(caminho_local, sheet_name=None, header=None, dtype=str)
    except Exception as e:
        print(f"      ⚠️  Não consegui abrir como Excel ({str(e)[:120]}). Tentando HTML...")
        try:
            tabelas = pd.read_html(caminho_local)
            abas = {f"tabela_{i}": t.astype(str) for i, t in enumerate(tabelas)}
        except Exception as e2:
            print(f"      ❌ Também falhou como HTML: {str(e2)[:120]}")
            return 0

    gravadas = 0
    for nome_aba, df in abas.items():
        if df is None or df.empty:
            continue
        df = df.dropna(how="all").dropna(axis=1, how="all")
        if df.empty:
            continue

        df = df.astype(str).replace({"nan": None, "NaT": None})
        df.columns = [f"col_{i:02d}" for i in range(len(df.columns))]
        df["aba_origem"] = str(nome_aba)
        df["planilha_origem"] = caminho_local.name
        # Endereço real no FTP — sem isso a procedência só poderia ser
        # reconstruída pelo slug da pasta, e a RAIS (varredura recursiva)
        # perderia a subpasta (vínculos/ ou estabelecimento/).
        df["caminho_ftp"] = f"ftp://{FTP_HOST}{caminho_ftp}" if caminho_ftp else ""

        # O nome da PLANILHA entra no caminho, não só pasta e aba: os 7
        # layouts de vínculos da RAIS têm todos uma aba "RAIS - layout", e
        # sem isso só sobrevivia o último processado.
        planilha_slug = _slug(caminho_local.stem.replace(f"{origem_slug}__", ""))
        destino = dir_saida / origem_slug / planilha_slug / f"{_slug(nome_aba)}.parquet"
        destino.parent.mkdir(parents=True, exist_ok=True)
        try:
            df.to_parquet(destino, engine="pyarrow", compression="zstd",
                          compression_level=3, index=False)
            gravadas += 1
        except Exception as e:
            print(f"      ⚠️  Falha ao gravar aba '{nome_aba}': {str(e)[:150]}")
    return gravadas


def modo_extrair() -> int:
    if DIR_LOCAL.exists():
        shutil.rmtree(DIR_LOCAL)
    DIR_LOCAL.mkdir(parents=True, exist_ok=True)

    cliente = ClienteFTP()
    cliente.conectar()

    print("📚 DICIONÁRIOS / LAYOUTS — planilhas de tradução dos códigos")
    total_planilhas = total_abas = 0
    contagem = {}

    try:
        for pasta, recursivo in PASTAS_LAYOUT:
            planilhas = _listar_planilhas(cliente, pasta, recursivo)
            contagem[pasta] = len(planilhas)
            if not planilhas:
                continue

            print(f"\n📂 {pasta}  ({len(planilhas)} planilha(s))")
            origem_slug = _slug(pasta)

            for caminho_remoto, nome in planilhas:
                print(f"   📄 {nome}")
                local = DIR_LOCAL / "_download" / f"{origem_slug}__{nome}"
                if not cliente.baixar(caminho_remoto, local):
                    continue

                abas = _converter_planilha(local, origem_slug, DIR_LOCAL, caminho_remoto)
                if abas:
                    print(f"      ✅ {abas} aba(s)")
                    total_abas += abas
                    total_planilhas += 1
    finally:
        cliente.fechar()

    (DIR_LOCAL / ARQUIVO_CONTAGEM).write_text(
        json.dumps(contagem, ensure_ascii=False, indent=2), encoding="utf-8")
    shutil.rmtree(DIR_LOCAL / "_download", ignore_errors=True)

    print(f"\n📤 Publicando em {nuvem.REPO_DICIONARIOS}...")
    from huggingface_hub import HfApi

    api = HfApi()
    api.create_repo(repo_id=nuvem.REPO_DICIONARIOS, repo_type="dataset", exist_ok=True)
    api.upload_folder(folder_path=str(DIR_LOCAL), repo_id=nuvem.REPO_DICIONARIOS, repo_type="dataset")

    print(f"\n🏁 {total_planilhas} planilha(s), {total_abas} aba(s) -> "
          f"https://huggingface.co/datasets/{nuvem.REPO_DICIONARIOS}")
    telegram.notificar(f"🏁 *Dicionário MTE*: extraído e publicado "
                       f"({total_planilhas} planilha(s), {total_abas} aba(s)).")
    return 0


def main() -> int:
    args = _argumentos()
    if args.modo == "verificar":
        return modo_verificar()
    return modo_extrair()


if __name__ == "__main__":
    sys.exit(main())
