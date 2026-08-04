"""
Tratamento e escrita em parquet (camada bronze).

Estratégia de tratamento aplicada aqui:
  1. Leitura com `all_varchar` — bronze não adivinha tipo, preserva o dado cru.
  2. Nomes de coluna normalizados (snake_case, sem acento).
  3. TRIM em todos os campos — a RAIS vem com padding de espaços (hex 20).
  4. String vazia vira NULL.
  5. Colunas de linhagem: ano/mês/UF de partição, arquivo de origem e data de ingestão.

O TRIM é feito em SQL (e não linha a linha em Python) porque os arquivos da RAIS
passam de 10 GB descompactados — em Python levaria horas.

ENCODING: não dá para fixar um só. O acervo mistura as duas codificações —
o Novo CAGED (2020+) é UTF-8, o CAGED antigo (2007-2019) é Latin-1:

    CAGEDEXC202601.txt -> b'compet\\xc3\\xaanciamov'   (ê em UTF-8)
    CAGEDEST_012019.txt -> b'Compet\\xeancia'          (ê em Latin-1)

Errar para o lado do Latin-1 é o caso perigoso: o parser não acusa erro nenhum
e grava mojibake silenciosamente ("competência" vira "competÃªncia"). Por isso
o encoding é detectado por arquivo, e não assumido.

Se mesmo assim a leitura falhar (alguns arquivos da RAIS têm null bytes que
quebram o parser), o módulo faz uma limpeza física do arquivo e tenta de novo.
"""
import os
import re
from pathlib import Path

from extracao_ftp.catalogo import ItemTrabalho
from extracao_ftp.config_extracao import (
    PARQUET_COMPRESSION,
    PARQUET_COMPRESSION_LEVEL,
    PARQUET_ROW_GROUP_SIZE,
)

RE_NAO_ALFANUM = re.compile(r"[^0-9a-z]+")


def _sql_str(valor: str) -> str:
    """Escapa uma string para interpolar com segurança no SQL."""
    return valor.replace("'", "''")


def _opcoes_leitura(arquivos: list[Path], encoding: str) -> str:
    """Monta a chamada read_csv com as opções de parsing dos microdados do MTE."""
    if len(arquivos) == 1:
        alvo = f"'{_sql_str(arquivos[0].as_posix())}'"
    else:
        lista = ", ".join(f"'{_sql_str(a.as_posix())}'" for a in arquivos)
        alvo = f"[{lista}]"

    opcoes = [
        alvo,
        "delim=';'",
        "header=true",
        "all_varchar=true",
        "normalize_names=true",
        f"encoding='{encoding}'",
        "ignore_errors=true",
        "null_padding=true",
        "quote='\"'",
    ]
    if len(arquivos) > 1:
        # Arquivos do mesmo ano podem ter ordem de colunas diferente
        opcoes.append("union_by_name=true")

    return f"read_csv({', '.join(opcoes)})"


def _colunas(con, leitura: str) -> list[str]:
    """Descobre os nomes das colunas sem ler o arquivo inteiro."""
    descricao = con.execute(f"DESCRIBE SELECT * FROM {leitura} LIMIT 0").fetchall()
    return [linha[0] for linha in descricao]


def _select_tratado(colunas: list[str], item: ItemTrabalho) -> str:
    """
    Monta a lista do SELECT já com o tratamento aplicado.

    Colunas explícitas (em vez de COLUMNS(*)) para garantir que o nome original
    seja preservado no parquet.
    """
    expressoes = []
    vistas: set[str] = set()

    for col in colunas:
        # Blinda contra nome duplicado após a normalização
        seguro = RE_NAO_ALFANUM.sub("_", col.lower()).strip("_") or "coluna"
        candidato, n = seguro, 2
        while candidato in vistas:
            candidato = f"{seguro}_{n}"
            n += 1
        vistas.add(candidato)

        expressoes.append(
            f'nullif(trim("{col}"), \'\') AS "{candidato}"'
        )

    # --- colunas de linhagem / partição ---
    expressoes.append(f"{item.ano}::SMALLINT AS ano_particao")
    expressoes.append(
        f"{item.mes}::TINYINT AS mes_particao" if item.mes is not None
        else "NULL::TINYINT AS mes_particao"
    )
    expressoes.append(
        f"'{_sql_str(item.recorte)}'::VARCHAR AS recorte_particao" if item.recorte
        else "NULL::VARCHAR AS recorte_particao"
    )
    expressoes.append(f"'{_sql_str(item.nome_arquivo)}'::VARCHAR AS arquivo_fonte")
    expressoes.append(f"'{_sql_str(item.caminho_remoto)}'::VARCHAR AS caminho_fonte")
    expressoes.append("now()::TIMESTAMP AS data_ingestao")

    return ",\n            ".join(expressoes)


def _higienizar(arquivo: Path, encoding: str = "latin-1") -> bool:
    """
    Reescreve o arquivo removendo null bytes e convertendo para UTF-8.

    Só é chamado quando a leitura direta falha, porque é caro (passa byte a byte).
    Recebe o encoding de origem detectado: reler um arquivo UTF-8 como Latin-1
    aqui reintroduziria exatamente o "competÃªncia" que a detecção evita.
    """
    print(f"      🧹 Limpando fisicamente {arquivo.name} (null bytes / encoding)...")
    temporario = arquivo.with_suffix(arquivo.suffix + ".limpo")
    mantidas = descartadas = 0

    try:
        with open(arquivo, "r", encoding=encoding, errors="replace") as entrada, open(
            temporario, "w", encoding="utf-8", newline=""
        ) as saida:
            for linha in entrada:
                if "\0" in linha:
                    linha = linha.replace("\0", "")
                if len(linha.strip()) < 2:
                    descartadas += 1
                    continue
                saida.write(linha)
                mantidas += 1

        os.replace(temporario, arquivo)
        print(f"      ✨ {mantidas:,} linhas mantidas / {descartadas:,} descartadas")
        return True
    except Exception as e:
        print(f"      ❌ Falha na limpeza: {e}")
        temporario.unlink(missing_ok=True)
        return False


def _amostrar(arquivo: Path, tamanho: int, do_fim: bool = False) -> bytes:
    """Lê um pedaço do arquivo (início ou fim) para farejar o encoding."""
    with open(arquivo, "rb") as f:
        if do_fim:
            f.seek(max(0, arquivo.stat().st_size - tamanho))
        return f.read(tamanho)


def detectar_encoding(arquivo: Path, amostra_bytes: int = 8 * 1024 * 1024) -> str:
    """
    Descobre se o arquivo é UTF-8 ou Latin-1.

    Necessário porque a fonte NÃO é uniforme: os arquivos do Novo CAGED (2020+)
    vêm em UTF-8, enquanto os da RAIS e do CAGED antigo vêm em Latin-1. Ler um
    UTF-8 como Latin-1 transforma "competência" em "competÃªncia"; o caminho
    inverso derruba linhas inteiras.

    Regra: só afirma UTF-8 quando encontra bytes não-ASCII que formam sequências
    UTF-8 válidas. Na dúvida devolve latin-1, que decodifica qualquer byte sem
    erro — errar para latin-1 estraga acentos (visível e corrigível), errar para
    utf-8 faria o `ignore_errors` descartar linhas em silêncio.
    """
    for do_fim in (False, True):
        amostra = _amostrar(arquivo, amostra_bytes, do_fim)
        if not amostra:
            continue

        # Só ASCII neste pedaço? Encoding é indiferente aqui, tenta o outro pedaço.
        if all(b < 0x80 for b in amostra):
            continue

        # Descarta os últimos bytes: a amostra pode ter cortado um caractere
        # multibyte no meio e isso, sozinho, não significa que não seja UTF-8.
        try:
            amostra[:-4].decode("utf-8")
            return "utf-8"
        except UnicodeDecodeError:
            return "latin-1"

    return "latin-1"


def _copiar(con, leitura: str, item: ItemTrabalho) -> int:
    """Executa o COPY para o MinIO e devolve a contagem de linhas gravadas."""
    colunas = _colunas(con, leitura)
    if not colunas:
        raise RuntimeError("nenhuma coluna detectada no arquivo")

    select = _select_tratado(colunas, item)

    con.execute(
        f"""
        COPY (
            SELECT
            {select}
            FROM {leitura}
        ) TO '{_sql_str(item.destino_s3)}' (
            FORMAT PARQUET,
            COMPRESSION '{PARQUET_COMPRESSION}',
            COMPRESSION_LEVEL {PARQUET_COMPRESSION_LEVEL},
            ROW_GROUP_SIZE {PARQUET_ROW_GROUP_SIZE}
        );
        """
    )

    total = con.execute(
        f"SELECT count(*) FROM read_parquet('{_sql_str(item.destino_s3)}')"
    ).fetchone()[0]
    return total


def converter(con, arquivos: list[Path], item: ItemTrabalho) -> tuple[bool, int]:
    """
    Converte os arquivos de dados extraídos num único parquet no MinIO.

    Devolve (sucesso, linhas_gravadas).
    """
    if not arquivos:
        print("      ⚠️  Nenhum arquivo de dados encontrado no compactado")
        return False, 0

    nomes = ", ".join(a.name for a in arquivos[:3])
    if len(arquivos) > 3:
        nomes += f" (+{len(arquivos) - 3})"
    print(f"      📄 {len(arquivos)} arquivo(s): {nomes}")

    # 1ª tentativa: leitura direta no encoding detectado (rápida, sem reescrever nada).
    # O encoding é decidido pelo arquivo, não pela era — ver docstring do módulo.
    encoding = detectar_encoding(arquivos[0])
    print(f"      🔤 Encoding detectado: {encoding}")
    try:
        linhas = _copiar(con, _opcoes_leitura(arquivos, encoding), item)
        return True, linhas
    except Exception as e:
        print(f"      ⚠️  Leitura direta falhou: {str(e)[:200]}")

    # 2ª tentativa: higieniza os arquivos (na origem detectada) e lê como UTF-8
    for arquivo in arquivos:
        if not _higienizar(arquivo, encoding):
            return False, 0

    try:
        linhas = _copiar(con, _opcoes_leitura(arquivos, "utf-8"), item)
        return True, linhas
    except Exception as e:
        print(f"      ❌ Conversão falhou definitivamente: {str(e)[:300]}")
        return False, 0
