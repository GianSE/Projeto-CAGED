"""
Gold modelos — previsão, nowcast, hiato salarial e sobrevivência de vínculo,
materializados na gold.

Depende de gold-rais.py: nowcast lê rais_estoque_anual, construído e
publicado lá. Como cada job roda num runner novo (sem acesso ao disco de
gold-rais.py), este script baixa a tabela já publicada antes de rodar os
modelos — mesmo padrão que gold-territorio.py usa pra semear
geo_municipios. Os outros três modelos (previsão, hiato, sobrevivência)
leem a silver direto e não precisariam esperar, mas materializar.py já
roda os quatro numa função só — separar só o nowcast não valeria a
complexidade.

Publica a própria fatia em Gianpedro/gold-analise-mercado-ti ao final.

Uso:
    python gold-modelos.py
"""
import sys
from pathlib import Path

_AQUI = Path(__file__).resolve().parent
sys.path.insert(0, str(_AQUI))
sys.path.insert(0, str(_AQUI.parent))

from _utils import log, nuvem, telegram  # noqa: E402

log.ativar_horario()


def _semear_rais_estoque_anual() -> None:
    """nowcast_estoque.py lê isto como arquivo local (ver _utils/nuvem.py:
    caminho_gold) — sem semear, o primeiro read_parquet falha com arquivo
    inexistente, já que gold-rais.py roda num runner diferente."""
    from huggingface_hub import hf_hub_download
    import shutil

    nuvem.DIR_GOLD_LOCAL.mkdir(parents=True, exist_ok=True)
    try:
        caminho = hf_hub_download(nuvem.REPO_GOLD, "rais_estoque_anual.parquet",
                                  repo_type="dataset")
        shutil.copy(caminho, nuvem.DIR_GOLD_LOCAL / "rais_estoque_anual.parquet")
        print("✅ rais_estoque_anual.parquet semeado (de gold-rais.py)")
    except Exception as e:
        print(f"⚠️  rais_estoque_anual.parquet ainda não existe no Hub ({e}) — "
             f"nowcast provavelmente vai falhar")


def main() -> int:
    _semear_rais_estoque_anual()

    from ciencia_dados import materializar

    codigo = materializar.main()
    if codigo != 0:
        telegram.notificar("⚠️ *Gold modelos*: concluído com falha, publicação pulada.")
        return codigo

    print("\n📤 Publicando no Hugging Face...")
    from gold_unificado import publicar_gold

    codigo_publicar = publicar_gold.main()
    if codigo_publicar != 0:
        telegram.notificar("⚠️ *Gold modelos*: materializado, mas a publicação falhou.")
        return codigo_publicar

    telegram.notificar("🏁 *Gold modelos*: previsão, nowcast, hiato e sobrevivência "
                       "materializados e publicados.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
