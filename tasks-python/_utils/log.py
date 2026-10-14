"""Prefixa cada linha impressa com a hora de Brasília, pra acompanhar o log do Actions."""
import sys
from datetime import datetime, timedelta, timezone

# Fixo em -3: o Brasil não tem horário de verão desde 2019, e assim não
# depende de base de fusos (o runner é UTC; no Windows ela nem vem instalada).
BRASILIA = timezone(timedelta(hours=-3), "BRT")


class _ComHorario:
    def __init__(self, destino):
        self._destino = destino
        self._inicio_de_linha = True

    def write(self, texto: str) -> int:
        saida = []
        for pedaco in texto.splitlines(keepends=True):
            # Linha em branco fica em branco; o print manda o texto e o "\n"
            # em duas chamadas, por isso o estado de "início de linha".
            if self._inicio_de_linha and pedaco.strip("\r\n"):
                saida.append(datetime.now(BRASILIA).strftime("[%H:%M:%S] "))
            saida.append(pedaco)
            self._inicio_de_linha = pedaco.endswith("\n")
        self._destino.write("".join(saida))
        return len(texto)

    def flush(self) -> None:
        self._destino.flush()

    def __getattr__(self, nome):
        return getattr(self._destino, nome)


def ativar_horario() -> None:
    if not isinstance(sys.stdout, _ComHorario):
        sys.stdout = _ComHorario(sys.stdout)
