"""
Reconstrói, em ordem, tudo o que deriva da silver de TI.

POR QUE UM ORQUESTRADOR
-----------------------
Uma mudança na silver — como a revisão do recorte — invalida seis camadas
derivadas, e elas dependem umas das outras:

    auditoria           a silver está íntegra? se não, NADA segue
    gold da RAIS        agregados de estoque
    mapa                território das duas bases
    perfis de município depende do mapa
    modelos             previsão, sobrevivência, nowcast e hiato
    consolidados CAGED  os arquivos que o dashboard em produção lê
    CAGED unificado     as duas gerações com rótulos canônicos

Rodar à mão, na memória, é como uma camada fica velha sem ninguém notar: o
mapa reconstruído e os perfis de município ainda calculados sobre o mapa
antigo. Aqui a ordem está escrita, e a primeira falha interrompe o resto.

A AUDITORIA VEM PRIMEIRO
------------------------
Reconstruir a gold em cima de uma silver com defeito espalha o defeito por
todas as camadas de uma vez, e depois é preciso descobrir em qual delas ele
entrou. Se a auditoria acusar inconsistência, nada é reconstruído.

PUBLICAR FICA DE FORA
---------------------
Este script só reconstrói no lake e em `publicacao/`. Publicar no Hugging Face
é sair da máquina: o dado fica visível para qualquer um. Isso é passo à parte,
depois de conferir os números.

Uso:
    python -m reconstruir_camadas
    python -m reconstruir_camadas --pular auditoria
"""
import argparse
import os
import subprocess
import sys
import time
from pathlib import Path

ETAPAS = [
    ("auditoria", "auditoria.consistencia", []),
    ("gold da RAIS", "gold_rais.construir_gold", []),
    ("mapa", "gold_unificado.mapa", []),
    ("perfis de município", "ciencia_dados.clusters_municipios", []),
    ("modelos", "ciencia_dados.materializar", []),
    ("consolidados CAGED", "gold_caged.consolidar", []),
    ("CAGED unificado", "gold_caged.unificar", []),
]


def main() -> int:
    p = argparse.ArgumentParser(description="Reconstrói as camadas derivadas da silver de TI.")
    p.add_argument("--pular", nargs="*", default=[],
                   choices=[nome for nome, _, _ in ETAPAS])
    args = p.parse_args()

    raiz = Path(__file__).resolve().parent
    ambiente = {**os.environ, "PYTHONUNBUFFERED": "1", "PYTHONIOENCODING": "utf-8"}
    etapas = [e for e in ETAPAS if e[0] not in args.pular]

    print("=" * 72)
    print("  RECONSTRUÇÃO DAS CAMADAS DERIVADAS")
    print("=" * 72)
    inicio = time.time()

    for n, (nome, modulo, extra) in enumerate(etapas, start=1):
        print(f"\n[{n}/{len(etapas)}] ▶ {nome}  (python -m {modulo})", flush=True)
        t = time.time()
        r = subprocess.run([sys.executable, "-u", "-m", modulo, *extra],
                           cwd=raiz, env=ambiente)
        if r.returncode != 0:
            print(f"\n❌ '{nome}' terminou com código {r.returncode}. "
                  f"As etapas seguintes NÃO rodaram: elas dependeriam de uma camada "
                  f"que não ficou pronta.")
            return r.returncode
        print(f"   ✅ {nome} em {(time.time() - t) / 60:.1f} min", flush=True)

    print(f"\n🏁 {len(etapas)} etapa(s) em {(time.time() - inicio) / 60:.1f} min. "
          f"Publicar é o próximo passo, depois de conferir os números.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
