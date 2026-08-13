"""
Planilhas de layout / dicionário do MTE.

Os microdados vêm quase inteiramente codificados: sexo = 1/2, grau de instrução
= 1..11, CBO, CNAE, município IBGE, tipo de movimentação etc. As planilhas de
layout publicadas no FTP trazem essas tabelas de/para — sem elas o parquet é
ilegível.

Este módulo:
  1. Baixa as planilhas (.xls / .xlsx) do FTP;
  2. Guarda o original em  bronze/_layouts/... (para conferência manual);
  3. Converte CADA ABA em parquet em bronze/dicionarios/{origem}/{aba}.parquet.

A conversão é deliberadamente "burra" (não tenta adivinhar cabeçalho): preserva
a aba como veio, para a camada silver montar as tabelas de/para conforme o uso.
"""
import io
import re
import unicodedata
from pathlib import Path

from extracao_ftp.config_extracao import (
    BUCKET_BRONZE,
    DIR_DOWNLOAD,
    FTP_BASE,
    MINIO_ACCESS_KEY,
    MINIO_ENDPOINT,
    MINIO_REGION,
    MINIO_SECRET_KEY,
    PARQUET_COMPRESSION,
    PARQUET_COMPRESSION_LEVEL,
)
from extracao_ftp.ftp_utils import ClienteFTP

# Pastas do FTP onde moram as planilhas de layout.
# (caminho relativo a FTP_BASE, recursivo?)
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


def _slug(texto: str) -> str:
    """Normaliza para nome de arquivo/coluna: sem acento, minúsculo, com _."""
    texto = unicodedata.normalize("NFKD", str(texto))
    texto = "".join(c for c in texto if not unicodedata.combining(c))
    texto = re.sub(r"[^0-9a-zA-Z]+", "_", texto).strip("_").lower()
    return texto or "sem_nome"


def _fs_minio():
    import s3fs

    return s3fs.S3FileSystem(
        key=MINIO_ACCESS_KEY,
        secret=MINIO_SECRET_KEY,
        client_kwargs={
            "endpoint_url": f"http://{MINIO_ENDPOINT}",
            "region_name": MINIO_REGION,
        },
    )


def _listar_planilhas(cliente: ClienteFTP, pasta: str, recursivo: bool) -> list[tuple[str, str]]:
    """Devolve [(caminho_remoto, nome)] das planilhas em uma pasta do FTP."""
    caminho = f"{FTP_BASE}/{pasta}"
    achados = []

    for nome in cliente.listar(caminho):
        completo = f"{caminho}/{nome}"
        if nome.lower().endswith(EXTENSOES_PLANILHA):
            achados.append((completo, nome))
        elif recursivo and "." not in nome:
            # subpasta (ex.: RAIS/Layouts/estabelecimento e .../vínculos)
            for sub in cliente.listar(completo):
                if sub.lower().endswith(EXTENSOES_PLANILHA):
                    achados.append((f"{completo}/{sub}", sub))

    return achados


def _converter_planilha(caminho_local: Path, origem_slug: str, fs) -> int:
    """Lê todas as abas de uma planilha e grava cada uma como parquet. Devolve nº de abas."""
    import pandas as pd

    # .xls antigo precisa do xlrd; .xlsx usa openpyxl. O pandas escolhe sozinho,
    # mas alguns "xls" do MTE são na verdade HTML/xml disfarçado.
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

        # Tudo como texto: as planilhas misturam número, código e descrição.
        df = df.astype(str).replace({"nan": None, "NaT": None})
        df.columns = [f"col_{i:02d}" for i in range(len(df.columns))]
        df["aba_origem"] = str(nome_aba)
        df["planilha_origem"] = caminho_local.name

        # O nome da PLANILHA entra no caminho, não só o da pasta e o da aba.
        #
        # Sem isso, planilhas diferentes com abas de mesmo nome se
        # sobrescrevem: os 7 layouts de vínculos da RAIS têm todos uma aba
        # "RAIS - layout", e sobrava só a última processada — que era a de
        # 1985-1993. Os códigos dos layouts modernos (escolaridade após 2005,
        # tipo de admissão, indicador Simples) sumiam em silêncio, e a
        # tradução da RAIS recente ficava sem de/para.
        planilha_slug = _slug(caminho_local.stem.replace(f"{origem_slug}__", ""))
        destino = (f"{BUCKET_BRONZE}/dicionarios/{origem_slug}/"
                   f"{planilha_slug}/{_slug(nome_aba)}.parquet")
        try:
            with fs.open(destino, "wb") as f:
                df.to_parquet(
                    f,
                    engine="pyarrow",
                    compression=PARQUET_COMPRESSION,
                    compression_level=PARQUET_COMPRESSION_LEVEL,
                    index=False,
                )
            gravadas += 1
        except Exception as e:
            print(f"      ⚠️  Falha ao gravar aba '{nome_aba}': {str(e)[:150]}")

    return gravadas


def extrair_dicionarios(cliente: ClienteFTP | None = None) -> None:
    """Baixa e converte todas as planilhas de layout do FTP para o MinIO."""
    fechar_no_fim = cliente is None
    if cliente is None:
        cliente = ClienteFTP()
        cliente.conectar()

    fs = _fs_minio()
    destino_local = DIR_DOWNLOAD / "_layouts"
    destino_local.mkdir(parents=True, exist_ok=True)

    print("\n" + "=" * 70)
    print("📚 DICIONÁRIOS / LAYOUTS — planilhas de tradução dos códigos")
    print("=" * 70)

    total_planilhas = total_abas = 0

    try:
        for pasta, recursivo in PASTAS_LAYOUT:
            planilhas = _listar_planilhas(cliente, pasta, recursivo)
            if not planilhas:
                continue

            print(f"\n📂 {pasta}  ({len(planilhas)} planilha(s))")
            origem_slug = _slug(pasta)

            for caminho_remoto, nome in planilhas:
                print(f"   📄 {nome}")
                local = destino_local / f"{origem_slug}__{nome}"

                if not cliente.baixar(caminho_remoto, local):
                    continue

                # Guarda o original no lake, para conferência humana
                try:
                    with open(local, "rb") as origem, \
                         fs.open(f"{BUCKET_BRONZE}/_layouts/{origem_slug}/{nome}", "wb") as dest:
                        dest.write(origem.read())
                except Exception as e:
                    print(f"      ⚠️  Não consegui subir o original: {str(e)[:150]}")

                abas = _converter_planilha(local, origem_slug, fs)
                if abas:
                    print(f"      ✅ {abas} aba(s) -> bronze/dicionarios/{origem_slug}/")
                    total_abas += abas
                    total_planilhas += 1
    finally:
        if fechar_no_fim:
            cliente.fechar()

    print(f"\n🏁 Dicionários: {total_planilhas} planilha(s), {total_abas} aba(s) em parquet.")
    print(f"   Originais em  s3://{BUCKET_BRONZE}/_layouts/")
    print(f"   Parquets em   s3://{BUCKET_BRONZE}/dicionarios/")


if __name__ == "__main__":
    import sys

    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
    extrair_dicionarios()
