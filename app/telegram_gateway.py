from __future__ import annotations

import http.server
import json
import logging
import socketserver
import threading
import time
from datetime import timedelta
from pathlib import Path

import requests

from app.config import get_settings
from app.db import (
    add_rule,
    fetch_due_notifications,
    get_health_snapshot,
    get_last_snapshot,
    get_setting,
    init_db,
    list_recent_events,
    mark_notification_retry,
    mark_notification_sent,
    set_heartbeat,
    set_setting,
)
from app.logging_setup import configure_logging
from app.utils import safe_unlink, utc_now, utc_now_iso

LOGGER = logging.getLogger("telegram-gateway")


class HealthHandler(http.server.BaseHTTPRequestHandler):
    def do_GET(self) -> None:  # noqa: N802
        if self.path != "/healthz":
            self.send_error(404)
            return
        self.send_response(200)
        self.end_headers()
        self.wfile.write(b"ok")

    def log_message(self, fmt: str, *args: object) -> None:
        return


def serve_health() -> None:
    with socketserver.TCPServer(("0.0.0.0", 9913), HealthHandler) as server:
        server.serve_forever()


def api_base(token: str) -> str:
    return f"https://api.telegram.org/bot{token}"


def send_text(token: str, chat_id: str, text: str) -> None:
    response = requests.post(
        f"{api_base(token)}/sendMessage",
        json={"chat_id": chat_id, "text": text},
        timeout=30,
    )
    response.raise_for_status()


def send_photo(token: str, chat_id: str, image_path: Path, caption: str) -> None:
    with image_path.open("rb") as handle:
        response = requests.post(
            f"{api_base(token)}/sendDocument",
            data={"chat_id": chat_id, "caption": caption},
            files={"document": handle},
            timeout=60,
        )
        response.raise_for_status()


def send_voice(token: str, chat_id: str, audio_path: Path, caption: str) -> None:
    with audio_path.open("rb") as handle:
        response = requests.post(
            f"{api_base(token)}/sendVoice",
            data={"chat_id": chat_id, "caption": caption},
            files={"voice": handle},
            timeout=60,
        )
        response.raise_for_status()


def send_audio_attachment(token: str, chat_id: str, audio_path: Path, caption: str) -> None:
    if audio_path.suffix.lower() in {".ogg", ".oga", ".opus"}:
        send_voice(token, chat_id, audio_path, caption)
        return
    with audio_path.open("rb") as handle:
        response = requests.post(
            f"{api_base(token)}/sendDocument",
            data={"chat_id": chat_id, "caption": caption},
            files={"document": handle},
            timeout=60,
        )
        response.raise_for_status()


def format_status(settings) -> str:
    snapshot = get_health_snapshot(settings)
    lines = ["WB SDR status"]
    lines.append(f"Queue pending: {snapshot['queue_pending']}")
    lines.append(f"Events 24h: {snapshot['recent_events_24h']}")
    for heartbeat in snapshot["heartbeats"]:
        lines.append(f"{heartbeat['service']}: {heartbeat['status']} @ {heartbeat['updated_at']}")
    return "\n".join(lines)


def format_last(settings) -> str:
    events = list_recent_events(settings, limit=5)
    if not events:
        return "No events yet"
    lines = []
    for event in events:
        lines.append(
            f"#{event['id']} {event['profile_id']} {int(event['frequency_hz']) / 1_000_000:.5f} MHz {float(event['signal_db']):.1f} dB"
        )
    return "\n".join(lines)


def parse_frequency_arg(arg: str) -> dict[str, object]:
    token = arg.strip().lower().replace("mhz", "")
    if "-" in token:
        start_text, end_text = token.split("-", 1)
        return {
            "rule_type": "range",
            "start_hz": int(float(start_text) * 1_000_000),
            "end_hz": int(float(end_text) * 1_000_000),
        }
    return {"rule_type": "exact", "exact_hz": int(float(token) * 1_000_000)}


def process_command(settings, token: str, chat_id: str, text: str) -> None:
    parts = text.strip().split(maxsplit=1)
    command = parts[0].lower()
    arg = "" if len(parts) == 1 else parts[1]
    if command == "/status":
        send_text(token, chat_id, format_status(settings))
        return
    if command == "/last":
        send_text(token, chat_id, format_last(settings))
        return
    if command == "/mute":
        minutes = int(arg or "30")
        until = (utc_now() + timedelta(minutes=minutes)).isoformat()
        set_setting(settings, "mute_until", until)
        send_text(token, chat_id, f"Muted until {until}")
        return
    if command == "/blacklist":
        if not arg:
            send_text(token, chat_id, "Usage: /blacklist 433.92 or /blacklist 433.90-433.95")
            return
        rule = parse_frequency_arg(arg)
        rule["source"] = "telegram"
        rule["note"] = "telegram command"
        add_rule(settings, "blacklist_rules", rule)
        send_text(token, chat_id, f"Blacklist added: {arg}")
        return
    if command == "/whitelist":
        if not arg:
            send_text(token, chat_id, "Usage: /whitelist 433.92 or /whitelist 433.90-433.95")
            return
        rule = parse_frequency_arg(arg)
        rule["source"] = "telegram"
        rule["note"] = "telegram command"
        add_rule(settings, "whitelist_rules", rule)
        send_text(token, chat_id, f"Whitelist added: {arg}")
        return
    if command == "/spectrum":
        snapshot = get_last_snapshot(settings)
        if not snapshot:
            send_text(token, chat_id, "No spectrum snapshot yet")
            return
        send_photo(token, chat_id, Path(snapshot), "Latest spectrum snapshot")
        return
    send_text(token, chat_id, "Supported commands: /status /last /mute /blacklist /whitelist /spectrum")


def poll_commands(settings) -> None:
    token = settings.telegram_bot_token
    if not settings.telegram_enabled or not token:
        return
    offset = int(get_setting(settings, "telegram_offset", "0") or "0")
    response = requests.get(
        f"{api_base(token)}/getUpdates",
        params={"timeout": settings.telegram_poll_timeout_sec, "offset": offset},
        timeout=settings.telegram_poll_timeout_sec + 5,
    )
    response.raise_for_status()
    payload = response.json()
    for update in payload.get("result", []):
        update_id = int(update["update_id"])
        message = update.get("message") or {}
        chat_id = str(message.get("chat", {}).get("id", settings.telegram_chat_id))
        text = message.get("text") or ""
        if text.startswith("/") and chat_id:
            process_command(settings, token, chat_id, text)
        offset = update_id + 1
        set_setting(settings, "telegram_offset", str(offset))


def flush_notifications(settings) -> int:
    if not settings.telegram_enabled or not settings.telegram_bot_token or not settings.telegram_chat_id:
        return 0
    sent = 0
    for row in fetch_due_notifications(settings):
        payload = json.loads(row["payload_json"])
        queue_id = int(row["id"])
        event_id = int(row["event_id"])
        try:
            sent_primary = False
            audio_path = payload.get("audio_path")
            snapshot = payload.get("snapshot_path")
            if payload.get("authorized_audio") and audio_path and Path(audio_path).exists():
                send_audio_attachment(settings.telegram_bot_token, settings.telegram_chat_id, Path(audio_path), payload["text"])
                sent_primary = True
            elif snapshot and settings.telegram_send_snapshots and Path(snapshot).exists():
                send_photo(settings.telegram_bot_token, settings.telegram_chat_id, Path(snapshot), payload["text"])
                sent_primary = True
            if not sent_primary:
                send_text(settings.telegram_bot_token, settings.telegram_chat_id, payload["text"])
            mark_notification_sent(settings, queue_id, event_id)
            safe_unlink(Path(row["spool_path"]))
            sent += 1
        except Exception as exc:
            attempts = int(row["attempts"]) + 1
            delay = min(settings.telegram_retry_max_sec, settings.telegram_retry_base_sec * (2 ** max(0, attempts - 1)))
            mark_notification_retry(settings, queue_id, str(exc), (utc_now() + timedelta(seconds=delay)).isoformat())
    return sent


def main() -> None:
    settings = get_settings()
    configure_logging()
    init_db(settings)
    health_thread = threading.Thread(target=serve_health, daemon=True)
    health_thread.start()
    while True:
        try:
            sent = flush_notifications(settings)
            poll_commands(settings)
            set_heartbeat(
                settings,
                "telegram-gateway",
                "ok" if settings.telegram_enabled else "disabled",
                {"sent": sent, "telegram_enabled": settings.telegram_enabled},
            )
        except Exception as exc:
            LOGGER.exception("Telegram gateway loop failed")
            set_heartbeat(settings, "telegram-gateway", "degraded", {"error": str(exc)})
        time.sleep(2)


if __name__ == "__main__":
    main()
