"""
Gold CAGED consolidado — um parquet por tabela e por ano (para servir por
HTTPS) mais o arquivo unificado das duas gerações, a partir da
silver-caged-ti já publicada. Publica no mesmo repositório da silver
(Gianpedro/silver-caged-ti), como um espelho pronto pra consulta direta.

Totalmente independente do resto da gold: só depende de silver-caged-ti,
nunca lê nada que gold-rais.py, gold-territorio.py ou gold-modelos.py
produzem.

Uso:
    python gold-caged-consolidado.py
"""
import sys
from pathlib import Path

_AQUI = Path(__file__).resolve().parent
sys.path.insert(0, str(_AQUI))
sys.path.insert(0, str(_AQUI.parent))

from _utils import log, telegram  # noqa: E402

log.ativar_horario()


def main() -> int:
    from gold_caged import consolidar, unificar, publicar_hf

    codigo_consolidar = consolidar.main()
    codigo_unificar = unificar.main()

    if codigo_consolidar != 0 or codigo_unificar != 0:
        telegram.notificar("⚠️ *Gold CAGED consolidado*: consolidar/unificar falhou, "
                           "publicação pulada.")
        return 2

    print("\n📤 Publicando no Hugging Face...")
    codigo_publicar = publicar_hf.main()
    if codigo_publicar != 0:
        telegram.notificar("⚠️ *Gold CAGED consolidado*: publicação falhou.")
        return codigo_publicar

    telegram.notificar("🏁 *Gold CAGED consolidado*: reconstruído e publicado.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
