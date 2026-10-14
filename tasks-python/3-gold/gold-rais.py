"""
Gold RAIS — estoque de emprego em tecnologia, a partir da silver-rais-ti
já publicada. Publica a própria fatia em Gianpedro/gold-analise-mercado-ti
ao final: cada job de gold roda num runner novo, sem acesso ao disco de
outro job, então publicar aqui (em vez de depender de um job final que leria
arquivo local) é o que entrega o dado pra quem depende dele (gold-modelos,
pelo rais_estoque_anual).

Independente das outras partes da gold: só depende de silver-rais-ti.

Uso:
    python gold-rais.py
"""
import sys
from pathlib import Path

_AQUI = Path(__file__).resolve().parent
sys.path.insert(0, str(_AQUI))
sys.path.insert(0, str(_AQUI.parent))

from _utils import log, telegram  # noqa: E402

log.ativar_horario()


def main() -> int:
    from gold_rais import construir_gold as gr
    from gold_unificado import publicar_gold

    codigo = gr.main()
    if codigo != 0:
        telegram.notificar("⚠️ *Gold RAIS*: concluído com falha, publicação pulada.")
        return codigo

    print("\n📤 Publicando no Hugging Face...")
    codigo_publicar = publicar_gold.main()
    if codigo_publicar != 0:
        telegram.notificar("⚠️ *Gold RAIS*: reconstruído, mas a publicação falhou.")
        return codigo_publicar

    telegram.notificar("🏁 *Gold RAIS*: estoque de emprego reconstruído e publicado.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
