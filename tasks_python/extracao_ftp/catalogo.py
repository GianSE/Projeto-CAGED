"""
Catálogo dos microdados do PDET/MTE.

Varre a árvore do FTP e classifica cada arquivo compactado num ItemTrabalho,
já com o destino final no MinIO resolvido.

A classificação é por regex sobre o nome do arquivo (e não por caminho fixo)
porque o layout do FTP muda entre eras:

  NOVO CAGED/{ano}/{anomes}/CAGED{MOV|FOR|EXC}{anomes}.7z      2020 -> hoje
  CAGED/{ano}/CAGEDEST_{MM}{AAAA}.7z                           2007 -> 2019
  CAGED_AJUSTES/{ano}/CAGEDEST_AJUSTES_{MM}{AAAA}.7z           2010 -> 2019
  CAGED_AJUSTES/2002a2009/CAGEDEST_AJUSTES_{AAAA}.7z           2002 -> 2009
  RAIS/{ano}/RAIS_VINC_PUB_{REGIAO}.7z + RAIS_ESTAB_PUB.7z     2018 -> hoje
  RAIS/{ano}/{UF}{ano}.7z + ESTB{ano}.7z                       1985 -> 2017
"""
import re
from dataclasses import dataclass

from extracao_ftp.config_extracao import BUCKET_BRONZE, FTP_BASE
from extracao_ftp.ftp_utils import ClienteFTP

# --- Padrões de nome de arquivo -------------------------------------------------
RE_CAGED_NOVO = re.compile(r"^CAGED(MOV|FOR|EXC)(\d{4})(\d{2})\.(7z|zip)$", re.I)
RE_CAGED_EST = re.compile(r"^CAGEDEST_(\d{2})(\d{4})\.(7z|zip)$", re.I)
RE_CAGED_AJUSTE_MENSAL = re.compile(r"^CAGEDEST_AJUSTES_(\d{2})(\d{4})\.(7z|zip)$", re.I)
RE_CAGED_AJUSTE_ANUAL = re.compile(r"^CAGEDEST_AJUSTES_(\d{4})\.(7z|zip)$", re.I)
RE_RAIS_VINC_NOVO = re.compile(r"^RAIS_VINC_PUB_(.+?)\.(7z|zip)$", re.I)
RE_RAIS_ESTAB_NOVO = re.compile(r"^RAIS_ESTAB_PUB\.(7z|zip)$", re.I)
RE_RAIS_ESTAB_ANTIGO = re.compile(r"^ESTB(\d{4})\.(7z|zip)$", re.I)
RE_RAIS_VINC_ANTIGO = re.compile(r"^([A-Za-z]{2})(\d{4})\.(7z|zip)$", re.I)
# Vínculos sem UF identificada (equivalente ao "NI" da RAIS moderna).
# A grafia varia na própria fonte: IGNORANDOS1985, IGNORADOS1986, IGNORADO1988.
RE_RAIS_VINC_IGNORADO = re.compile(r"^IGNORAN?DOS?(\d{4})\.(7z|zip)$", re.I)

RE_ANO_PASTA = re.compile(r"(\d{4})")

# Raízes de cada dataset dentro do FTP
RAIZES = {
    "novo_caged": f"{FTP_BASE}/NOVO CAGED",
    "caged": f"{FTP_BASE}/CAGED",
    "caged_ajustes": f"{FTP_BASE}/CAGED_AJUSTES",
    "rais": f"{FTP_BASE}/RAIS",
}

# Tabelas produzidas por dataset (usado pelo filtro --dataset do CLI)
DATASETS = tuple(RAIZES.keys())

EXTENSOES_COMPACTADAS = (".7z", ".zip")

# Pastas que NÃO devem ser varridas.
#
# "Legado" republica a série inteira a cada divulgação: RAIS/2019/Legado tem os
# mesmos RAIS_VINC_PUB_*.7z de RAIS/2019, e NOVO CAGED/Legado guarda um snapshot
# mensal da série toda. Sem esse filtro os itens colidiriam no mesmo destino S3
# (sobrescrevendo-se) e o volume baixado explodiria sem ganho de informação.
#
# "EEC" é a Enquete Empresarial Conjuntural, pesquisa distinta do CAGED.
DIRS_IGNORADOS = {"legado", "eec"}


@dataclass
class ItemTrabalho:
    """Uma unidade de trabalho: 1 arquivo compactado -> 1 parquet no MinIO."""

    tabela: str            # caged_mov, caged_old, rais_vinc, ...
    dataset: str           # novo_caged, caged, caged_ajustes, rais
    ano: int
    mes: int | None        # None para bases anuais (RAIS, ajustes antigos)
    recorte: str | None    # UF ou região, para a RAIS (sp, sul, nordeste, ...)
    caminho_remoto: str    # caminho completo no FTP
    nome_arquivo: str
    tamanho: int = 0
    parcial: bool = False  # veio de uma pasta "AAAA Parcial"

    @property
    def destino_s3(self) -> str:
        """Caminho final no MinIO, particionado no padrão Hive."""
        sufixo_ano = f"{self.ano}_parcial" if self.parcial else str(self.ano)
        base = f"s3://{BUCKET_BRONZE}/{self.tabela}/ano={sufixo_ano}"

        if self.mes is not None:
            return f"{base}/mes={self.mes}/{self.tabela}_{self.ano}{self.mes:02d}.parquet"
        if self.recorte:
            return f"{base}/{self.tabela}_{self.recorte}.parquet"
        return f"{base}/{self.tabela}_{self.ano}.parquet"

    @property
    def rotulo(self) -> str:
        """Identificação curta para logs."""
        partes = [self.tabela, str(self.ano)]
        if self.mes is not None:
            partes.append(f"{self.mes:02d}")
        if self.recorte:
            partes.append(self.recorte)
        if self.parcial:
            partes.append("parcial")
        return "/".join(partes)


def _ano_da_pasta(nome: str) -> int | None:
    m = RE_ANO_PASTA.search(nome)
    return int(m.group(1)) if m else None


def classificar(nome: str, caminho_remoto: str, ano_pasta: int | None,
                dataset: str, parcial: bool) -> ItemTrabalho | None:
    """Traduz um nome de arquivo do FTP num ItemTrabalho. None = não é microdado."""
    comum = dict(caminho_remoto=caminho_remoto, nome_arquivo=nome,
                 dataset=dataset, parcial=parcial)

    if m := RE_CAGED_NOVO.match(nome):
        tipo, ano, mes = m.group(1).lower(), int(m.group(2)), int(m.group(3))
        return ItemTrabalho(tabela=f"caged_{tipo}", ano=ano, mes=mes,
                            recorte=None, **comum)

    if m := RE_CAGED_AJUSTE_MENSAL.match(nome):
        mes, ano = int(m.group(1)), int(m.group(2))
        return ItemTrabalho(tabela="caged_ajustes", ano=ano, mes=mes,
                            recorte=None, **comum)

    if m := RE_CAGED_AJUSTE_ANUAL.match(nome):
        return ItemTrabalho(tabela="caged_ajustes", ano=int(m.group(1)), mes=None,
                            recorte=None, **comum)

    if m := RE_CAGED_EST.match(nome):
        mes, ano = int(m.group(1)), int(m.group(2))
        return ItemTrabalho(tabela="caged_old", ano=ano, mes=mes,
                            recorte=None, **comum)

    if m := RE_RAIS_ESTAB_NOVO.match(nome):
        if ano_pasta is None:
            return None
        return ItemTrabalho(tabela="rais_estab", ano=ano_pasta, mes=None,
                            recorte=None, **comum)

    if m := RE_RAIS_VINC_NOVO.match(nome):
        if ano_pasta is None:
            return None
        return ItemTrabalho(tabela="rais_vinc", ano=ano_pasta, mes=None,
                            recorte=m.group(1).lower(), **comum)

    if m := RE_RAIS_ESTAB_ANTIGO.match(nome):
        return ItemTrabalho(tabela="rais_estab", ano=int(m.group(1)), mes=None,
                            recorte=None, **comum)

    if m := RE_RAIS_VINC_IGNORADO.match(nome):
        return ItemTrabalho(tabela="rais_vinc", ano=int(m.group(1)), mes=None,
                            recorte="ignorado", **comum)

    if m := RE_RAIS_VINC_ANTIGO.match(nome):
        return ItemTrabalho(tabela="rais_vinc", ano=int(m.group(2)), mes=None,
                            recorte=m.group(1).lower(), **comum)

    return None


def _varrer(cliente: ClienteFTP, caminho: str, ano_pasta: int | None,
            dataset: str, parcial: bool, profundidade: int,
            itens: list[ItemTrabalho], ignorados: list[str]) -> None:
    """Percorre recursivamente um diretório do FTP acumulando itens."""
    if profundidade < 0:
        return

    for nome in cliente.listar(caminho):
        completo = f"{caminho}/{nome}"

        # Arquivo compactado -> tenta classificar
        if nome.lower().endswith(EXTENSOES_COMPACTADAS):
            item = classificar(nome, completo, ano_pasta, dataset, parcial)
            if item is None:
                ignorados.append(completo)
            else:
                item.tamanho = cliente.tamanho(completo)
                itens.append(item)
            continue

        # Documentação (pdf/xlsx/txt/htm) -> ignora silenciosamente
        if "." in nome and not nome.lower().endswith(EXTENSOES_COMPACTADAS):
            continue

        # Sem extensão -> provavelmente um diretório, desce um nível
        if nome.strip().lower() in DIRS_IGNORADOS:
            continue

        novo_ano = _ano_da_pasta(nome) or ano_pasta
        nova_parcial = parcial or ("parcial" in nome.lower())
        _varrer(cliente, completo, novo_ano, dataset, nova_parcial,
                profundidade - 1, itens, ignorados)


def descobrir(cliente: ClienteFTP, datasets: list[str], ano_min: int, ano_max: int,
              incluir_parcial: bool = False,
              tabelas: list[str] | None = None) -> list[ItemTrabalho]:
    """
    Varre o FTP e devolve os itens que batem com os filtros pedidos.

    ano_min / ano_max filtram pelo ano-base do dado (não pelo ano da pasta).
    """
    itens: list[ItemTrabalho] = []
    ignorados: list[str] = []

    for dataset in datasets:
        raiz = RAIZES[dataset]
        print(f"\n🔎 Varrendo {dataset}  ({raiz})")
        _varrer(cliente, raiz, None, dataset, False, 3, itens, ignorados)

    # --- filtros ---
    filtrados = [i for i in itens if ano_min <= i.ano <= ano_max]
    if not incluir_parcial:
        filtrados = [i for i in filtrados if not i.parcial]
    if tabelas:
        filtrados = [i for i in filtrados if i.tabela in tabelas]

    filtrados.sort(key=lambda i: (i.tabela, i.ano, i.mes or 0, i.recorte or ""))

    if ignorados:
        print(f"\n⚠️  {len(ignorados)} compactado(s) não reconhecido(s) pelo catálogo:")
        for c in ignorados[:10]:
            print(f"      - {c}")
        if len(ignorados) > 10:
            print(f"      ... e mais {len(ignorados) - 10}")

    return filtrados


def resumir(itens: list[ItemTrabalho]) -> None:
    """Imprime um resumo por tabela do que foi encontrado."""
    if not itens:
        print("\n⚠️  Nenhum item encontrado com os filtros informados.")
        return

    print(f"\n📋 {len(itens)} arquivo(s) no plano de extração:\n")
    print(f"    {'TABELA':<16} {'ARQUIVOS':>9} {'ANOS':>14} {'TAMANHO':>12}")
    print(f"    {'-' * 16} {'-' * 9} {'-' * 14} {'-' * 12}")

    total_bytes = 0
    for tabela in sorted({i.tabela for i in itens}):
        grupo = [i for i in itens if i.tabela == tabela]
        anos = sorted({i.ano for i in grupo})
        soma = sum(i.tamanho for i in grupo)
        total_bytes += soma
        faixa = f"{anos[0]}-{anos[-1]}" if len(anos) > 1 else str(anos[0])
        print(f"    {tabela:<16} {len(grupo):>9} {faixa:>14} {soma / 1e9:>9.2f} GB")

    print(f"    {'-' * 16} {'-' * 9} {'-' * 14} {'-' * 12}")
    print(f"    {'TOTAL':<16} {len(itens):>9} {'':>14} {total_bytes / 1e9:>9.2f} GB")
    print("\n    (tamanho = compactado no FTP; o parquet ZSTD final costuma ficar bem menor)")
