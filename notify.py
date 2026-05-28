# notify.py
import requests

TELEGRAM_AUTH = "Basic ZG5sOjEyMzQ1Ng=="
TELEGRAM_BASE = "https://smb.videv.cloud/webhook"

def notify(message: str, level: str = "error"):
    """
    level: "info" | "warning" | "error"
    """
    try:
        requests.post(
            f"{TELEGRAM_BASE}/{level}",
            headers={
                "Content-Type": "application/json",
                "Authorization": TELEGRAM_AUTH,
            },
            json={"message": message},
            timeout=5,
        )
    except Exception as e:
        print(f"Telegram notify failed: {e}")