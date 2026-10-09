"""
Notificação no Telegram — um lugar só, em vez do bloco de curl repetido em
cada workflow do Actions.

Uso:
    from _utils.telegram import notificar
    notificar("🏁 Bronze CAGED concluído")
"""
import json
import os
import urllib.request


def notificar(mensagem: str) -> bool:
    """
    Envia uma mensagem ao Telegram. Silenciosamente não faz nada se as
    credenciais não estiverem no ambiente (ex.: rodando local, sem secret).
    """
    token = os.getenv("TELEGRAM_BOT_TOKEN")
    chat_id = os.getenv("TELEGRAM_CHAT_ID")
    if not token or not chat_id:
        print("   (Telegram não configurado — pulando notificação)")
        return False

    url = f"https://api.telegram.org/bot{token}/sendMessage"
    corpo = json.dumps({
        "chat_id": chat_id,
        "text": mensagem,
        "parse_mode": "Markdown",
    }).encode("utf-8")

    try:
        req = urllib.request.Request(
            url, data=corpo, headers={"Content-Type": "application/json"})
        with urllib.request.urlopen(req, timeout=10):
            pass
        return True
    except Exception as e:
        print(f"   ⚠️  falha ao notificar no Telegram: {e}")
        return False
