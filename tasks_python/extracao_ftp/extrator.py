"""
Descompactação dos arquivos .7z / .zip baixados do FTP.

Usa o binário `7z` (p7zip-full no worker; 7-Zip no Windows) por dois motivos:
  1. muito mais rápido e econômico em memória que o py7zr nos arquivos grandes
     da RAIS;
  2. TOLERANTE A CORRUPÇÃO — alguns .7z do FTP do MTE vêm com CRC inválido
     (download antigo do servidor, não é coisa nossa) e o `7z -y` ainda
     recupera o conteúdo, enquanto o `py7zr` (Python puro, usado como
     fallback quando não há binário) simplesmente aborta com "Corrupt input
     data" e não extrai nada. Os scripts antigos do projeto
     (bronze_caged/old_caged_to_parquet.py) já resolviam isso assim — aqui é
     a mesma solução, só automática e sem precisar reescrever nada na mão.

Se o binário não existir em lugar nenhum, cai para o py7zr (mais lento, e
falha duro em arquivo corrompido).
"""
import shutil
import subprocess
import zipfile
from pathlib import Path

# Extensões que consideramos "arquivo de dados" dentro do compactado
EXTENSOES_DADOS = (".txt", ".csv", ".comt", ".dat")

# Abaixo disso é quase certo ser leia-me/lixo, não microdado
TAMANHO_MINIMO_BYTES = 10 * 1024

# shutil.which() só acha o que está no PATH da sessão atual — o instalador do
# 7-Zip no Windows não se registra no PATH por padrão, e um processo já em
# execução não vê uma mudança de PATH feita depois que ele iniciou. Checa
# esses caminhos conhecidos como último recurso.
CAMINHOS_7Z_CONHECIDOS = (
    r"C:\Program Files\7-Zip\7z.exe",
    r"C:\Program Files (x86)\7-Zip\7z.exe",
)

_binario_cache: str | None = None
_binario_resolvido = False


def _binario_7z() -> str | None:
    global _binario_cache, _binario_resolvido
    if _binario_resolvido:
        return _binario_cache

    _binario_cache = shutil.which("7z") or shutil.which("7za")
    if not _binario_cache:
        for caminho in CAMINHOS_7Z_CONHECIDOS:
            if Path(caminho).is_file():
                _binario_cache = caminho
                break

    _binario_resolvido = True
    return _binario_cache


def _tem_7z() -> bool:
    return _binario_7z() is not None


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
        print(f"      ⚠️  7z retornou {resultado.returncode} (CRC/corrupção?): "
              f"{resultado.stderr[:300] or resultado.stdout[-300:]}")

    # Não trava no código de saída: mesmo em erro grave (returncode >= 2, CRC
    # inválido) o 7z costuma ter escrito o conteúdo recuperável em disco antes
    # de desistir. Quem decide se deu certo é `extrair()`, olhando se algum
    # arquivo de dados de tamanho razoável realmente apareceu em `destino`.
    return True


def _truncar_ultima_linha(arquivo: Path) -> None:
    """
    Remove a última linha de um arquivo recuperado via stream.

    Quando o 7z é interrompido no meio da descompactação (fluxo cortado, não
    erro limpo), a última linha escrita quase sempre está pela metade — corta
    fora em vez de deixar o DuckDB tropeçar nela.
    """
    try:
        with open(arquivo, "rb+") as f:
            f.seek(0, 2)
            tamanho = f.tell()
            if tamanho == 0:
                return
            pos = tamanho - 1
            while pos > 0:
                f.seek(pos)
                if f.read(1) == b"\n":
                    break
                pos -= 1
            if pos > 0:
                f.seek(pos)
                f.truncate()
    except Exception:
        pass


def _extrair_stream_forcado(arquivo: Path, destino: Path) -> Path | None:
    """
    Último recurso quando a extração normal não produz nada aproveitável:
    streama o conteúdo direto do 7z (`-so`), tentando forçar a leitura como
    cada formato de arquivo conhecido, e fica com o primeiro que renderizar
    dado de verdade.

    Baseado em bronze_rais/resgate_total_stream.py — nasceu de arquivos da
    RAIS tão corrompidos que nem "extrair para pasta" funcionava, só ler
    como fluxo contínuo (sem tentar montar a árvore de arquivos do
    compactado) conseguia tirar alguma coisa de dentro.
    """
    destino.mkdir(parents=True, exist_ok=True)
    saida = destino / f"{arquivo.stem}_resgatado.txt"

    # [] deixa o 7z detectar sozinho; os outros forçam um tipo específico
    # para o caso da extensão do arquivo estar mentindo sobre o formato real.
    for flags in ([], ["-t7z"], ["-tzip"], ["-tgzip"], ["-txz"]):
        cmd = [_binario_7z(), "e", str(arquivo), "-so", *flags]
        bytes_salvos = 0
        try:
            processo = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL)
            with open(saida, "wb") as f_out:
                while True:
                    bloco = processo.stdout.read(1024 * 1024)
                    if not bloco:
                        break
                    f_out.write(bloco)
                    bytes_salvos += len(bloco)
            processo.wait(timeout=10)
        except Exception:
            pass

        if bytes_salvos >= TAMANHO_MINIMO_BYTES:
            print(f"      🚑 Resgate via stream (modo {flags or 'auto'}): "
                  f"{bytes_salvos / 1_048_576:.1f} MB")
            _truncar_ultima_linha(saida)
            return saida

        saida.unlink(missing_ok=True)

    return None


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

    # Nada de aproveitável e o binário 7z existe: última tentativa via stream
    # forçando formato. Só entra aqui quando as duas tentativas normais
    # (extrair para pasta / py7zr) não renderam nenhum arquivo de dados.
    if not encontrados and arquivo.suffix.lower() != ".zip" and _tem_7z():
        resgatado = _extrair_stream_forcado(arquivo, destino)
        if resgatado:
            encontrados = [resgatado]

    encontrados.sort()
    return encontrados
