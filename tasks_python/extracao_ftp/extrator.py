"""
Descompactação dos arquivos .7z / .zip baixados do FTP.

Usa o binário `7z` (p7zip-full, instalado no worker) por ser muito mais rápido
e econômico em memória que o py7zr nos arquivos grandes da RAIS. Se o binário
não existir no ambiente, cai para o py7zr automaticamente.
"""
import shutil
import subprocess
import zipfile
from pathlib import Path

# Extensões que consideramos "arquivo de dados" dentro do compactado
EXTENSOES_DADOS = (".txt", ".csv", ".comt", ".dat")

# Abaixo disso é quase certo ser leia-me/lixo, não microdado
TAMANHO_MINIMO_BYTES = 10 * 1024


def _tem_7z() -> bool:
    return shutil.which("7z") is not None or shutil.which("7za") is not None


def _binario_7z() -> str:
    return shutil.which("7z") or shutil.which("7za")


def limpar_diretorio(caminho: Path) -> None:
    """Esvazia um diretório sem removê-lo."""
    caminho.mkdir(parents=True, exist_ok=True)
    for item in caminho.iterdir():
        try:
            if item.is_dir():
                shutil.rmtree(item)
            else:
                item.unlink()
        except Exception as e:
            print(f"      ⚠️  Não consegui limpar {item.name}: {e}")


def _extrair_7z_binario(arquivo: Path, destino: Path) -> bool:
    # 'e' extrai achatado (sem recriar a árvore de pastas interna)
    resultado = subprocess.run(
        [_binario_7z(), "e", str(arquivo), f"-o{destino}", "-y"],
        capture_output=True,
        text=True,
        errors="replace",
    )
    if resultado.returncode != 0:
        print(f"      ⚠️  7z retornou {resultado.returncode}: {resultado.stderr[:300]}")
    # returncode 1 = warning (extraiu, mas com ressalvas) -> ainda aproveitamos
    return resultado.returncode in (0, 1)


def _extrair_py7zr(arquivo: Path, destino: Path) -> bool:
    try:
        import py7zr

        with py7zr.SevenZipFile(arquivo, mode="r") as z:
            z.extractall(path=destino)
        return True
    except Exception as e:
        print(f"      ❌ py7zr falhou: {e}")
        return False


def _extrair_zip(arquivo: Path, destino: Path) -> bool:
    try:
        with zipfile.ZipFile(arquivo, "r") as z:
            z.extractall(destino)
        return True
    except Exception as e:
        print(f"      ❌ zipfile falhou: {e}")
        return False


def extrair(arquivo: Path, destino: Path) -> list[Path]:
    """
    Descompacta `arquivo` em `destino` e devolve os arquivos de dados achados.

    O diretório de destino é limpo antes da extração.
    """
    limpar_diretorio(destino)

    if arquivo.suffix.lower() == ".zip":
        ok = _extrair_zip(arquivo, destino)
    elif _tem_7z():
        ok = _extrair_7z_binario(arquivo, destino)
    else:
        ok = _extrair_py7zr(arquivo, destino)

    if not ok:
        return []

    # Varre recursivamente: alguns .7z guardam os dados em subpastas
    encontrados = [
        p
        for p in destino.rglob("*")
        if p.is_file()
        and p.suffix.lower() in EXTENSOES_DADOS
        and p.stat().st_size >= TAMANHO_MINIMO_BYTES
    ]

    # Alguns arquivos antigos da RAIS vêm sem extensão nenhuma
    if not encontrados:
        encontrados = [
            p
            for p in destino.rglob("*")
            if p.is_file() and not p.suffix and p.stat().st_size >= TAMANHO_MINIMO_BYTES
        ]

    encontrados.sort()
    return encontrados
