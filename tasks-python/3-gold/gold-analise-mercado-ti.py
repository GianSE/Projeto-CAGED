"""
Gold análise mercado TI — reconstrói os agregados finais a partir da
silver-caged-ti/silver-rais-ti já publicadas, e publica em
Gianpedro/gold-analise-mercado-ti.

Sem verificação prévia: a gold inteira é pequena (poucos MB) e cara de
construir é a tradução (silver), não a agregação — reconstruir tudo de
novo a cada vez é mais simples e barato que rastrear o que mudou.

Uso:
    python gold-analise-mercado-ti.py
"""
import sys
from pathlib import Path

_AQUI = Path(__file__).resolve().parent
sys.path.insert(0, str(_AQUI))
sys.path.insert(0, str(_AQUI.parent))

from _utils import nuvem, telegram  # noqa: E402


def _semear_geo_municipios() -> None:
    """
    geo_municipios (coordenadas do IBGE) é referência estática — não é
    reconstruída a cada rodada (muda só se o IBGE redesenhar municípios).
    Baixa a versão já publicada antes dos construtores que dependem dela.
    """
    from huggingface_hub import hf_hub_download
    import shutil

    nuvem.DIR_GOLD_LOCAL.mkdir(parents=True, exist_ok=True)
    try:
        caminho = hf_hub_download(nuvem.REPO_GOLD, "geo_municipios.parquet", repo_type="dataset")
        shutil.copy(caminho, nuvem.DIR_GOLD_LOCAL / "geo_municipios.parquet")
        print("✅ geo_municipios.parquet semeado")
    except Exception as e:
        print(f"⚠️  geo_municipios.parquet ainda não existe no Hub ({e})")


def _etapa(titulo: str, func, *args) -> bool:
    print(f"\n{'=' * 70}\n  {titulo}\n{'=' * 70}")
    try:
        resultado = func(*args)
        return resultado if isinstance(resultado, bool) else True
    except SystemExit as e:
        return e.code in (0, None)
    except Exception as e:
        print(f"❌ {titulo} falhou: {e}")
        return False


def main() -> int:
    _semear_geo_municipios()

    from gold_rais import construir_gold as gr
    from gold_unificado import mapa
    from ciencia_dados import clusters_municipios, materializar
    from gold_caged import consolidar, unificar

    con = nuvem.conectar_duckdb()

    etapas = [
        ("RAIS — estoque de emprego", lambda: all(gr.construir(con, n) for n in gr.AGREGADOS)),
        ("Mapa — território (RAIS + CAGED)", lambda: all(mapa.construir(con, n) for n in mapa.TABELAS)),
        ("Perfis de município (clusters)", lambda: clusters_municipios.main() in (0, None)),
        ("Modelos (previsão, nowcast, hiato, sobrevivência)",
         lambda: materializar.main() in (0, None)),
        ("Consolidados CAGED (por ano)",
         lambda: all(consolidar.consolidar(con, t) for t in consolidar.COLUNA_TEMPO)),
        ("CAGED unificado (por ano)",
         lambda: all(unificar.unificar_ano(con, a) for a in unificar.anos_disponiveis(con))),
    ]

    resultados = {titulo: _etapa(titulo, func) for titulo, func in etapas}
    falhas = [t for t, ok in resultados.items() if not ok]

    print(f"\n📤 Publicando no Hugging Face...")
    from gold_caged import publicar_hf
    from gold_unificado import publicar_gold

    publicar_hf.main()
    publicar_gold.main()

    if falhas:
        print(f"\n⚠️  Etapa(s) com falha: {', '.join(falhas)}")
        telegram.notificar(f"⚠️ *Gold análise mercado TI*: concluído com falha em "
                           f"{', '.join(falhas)}")
        return 2

    print("\n🏁 Gold reconstruída e publicada com sucesso.")
    telegram.notificar("🏁 *Gold análise mercado TI*: reconstruída e publicada com sucesso.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
