"""
Painel web de observabilidade: progresso da extração + estado do MinIO.

O painel vive na raiz do projeto, mas os módulos de domínio (configuração,
catálogo do FTP, heartbeat) ficam em `tasks_python/`. O bootstrap de caminho
mora aqui, no __init__ do pacote, e não em `app.py`: assim vale para QUALQUER
entrada — `python -m painel.app`, um import isolado de `painel.processos` num
teste, ou o próprio Flask recarregando um módulo. Deixá-lo só no app.py fazia
`from painel import processos` quebrar quando o app não era o ponto de partida.
"""
import sys
from pathlib import Path

_TASKS_PYTHON = Path(__file__).resolve().parents[1] / "tasks_python"
if str(_TASKS_PYTHON) not in sys.path:
    sys.path.insert(0, str(_TASKS_PYTHON))
