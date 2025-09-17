import json
import logging
from typing import Optional
from airflow.models import Variable
import requests

log = logging.getLogger(__name__)

DISCORD_LIMIT = 2000  # Discord message hard limit (approx)

def _truncate_for_discord(text: str, limit: int = DISCORD_LIMIT) -> str:
    if len(text) <= limit:
        return text
    # Keep a small suffix to show truncation
    return text[: limit - 20] + "\n… (truncated)"

def send_discord_alert(message: str, username: Optional[str] = "Database Sync Alert",
                       avatar_url: Optional[str] = None) -> None:
    """
    Sends a simple Discord webhook message. Expects Airflow Variable 'DISCORD_WEBHOOK'.
    If you want a JSON response (200), append '?wait=true' to your webhook URL; otherwise Discord returns 204.
    """
    webhook_url: str = Variable.get("DISCORD_WEBHOOK", default_var="")
    if not webhook_url:
        log.warning("No Discord webhook URL configured (Variable 'DISCORD_WEBHOOK'), skipping alert.")
        return

    safe_message = _truncate_for_discord(message)

    payload = {
        "content": safe_message,
        "username": username,
    }
    if avatar_url:
        payload["avatar_url"] = avatar_url

    try:
        response = requests.post(webhook_url, json=payload, timeout=10)
    except Exception as e:
        log.exception("Exception while sending Discord alert: %s", e)
        return

    # Discord returns 204 No Content for non-waiting calls; 200 OK if '?wait=true'
    if response.status_code in (200, 204):
        log.info("Discord alert sent successfully (status %s).", response.status_code)
    else:
        # Try to capture any error details
        body = ""
        try:
            body = response.text
        except Exception:
            pass
        log.error("Failed to send Discord alert: status=%s body=%s", response.status_code, body)
