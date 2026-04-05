from __future__ import annotations

from datetime import timedelta
from pathlib import Path

from fastapi import FastAPI, Form, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from app.config import get_settings
from app.db import (
    add_rule,
    get_health_snapshot,
    get_setting,
    init_db,
    list_blacklist_suggestions,
    list_recent_events,
    list_rules,
    set_setting,
)
from app.utils import disk_usage_bytes, ensure_directories, utc_now, utc_now_iso

settings = get_settings()
init_db(settings)
ensure_directories(settings.data_dir, settings.snapshot_dir, settings.archive_dir, settings.telegram_spool_dir)

app = FastAPI(title="WB SDR Dashboard")
templates = Jinja2Templates(directory=str(Path(__file__).parent / "templates"))
app.mount("/static", StaticFiles(directory=str(Path(__file__).parent / "static")), name="static")
app.mount("/files/snapshots", StaticFiles(directory=str(settings.snapshot_dir)), name="snapshots")


def parse_rule_value(value: str) -> dict[str, object]:
    token = value.strip().lower().replace("mhz", "")
    if "-" in token:
        start_text, end_text = token.split("-", 1)
        return {
            "rule_type": "range",
            "start_hz": int(float(start_text) * 1_000_000),
            "end_hz": int(float(end_text) * 1_000_000),
        }
    return {"rule_type": "exact", "exact_hz": int(float(token) * 1_000_000)}


@app.get("/healthz")
def healthz() -> dict[str, object]:
    return {"status": "ok", "time": utc_now_iso(), **get_health_snapshot(settings)}


@app.get("/", response_class=HTMLResponse)
def index(request: Request) -> HTMLResponse:
    health = get_health_snapshot(settings)
    events = list_recent_events(settings, limit=50)
    blacklist = list_rules(settings, "blacklist_rules")
    whitelist = list_rules(settings, "whitelist_rules")
    suggestions = list_blacklist_suggestions(settings)
    archive_usage = disk_usage_bytes(settings.archive_dir)
    snapshot_usage = disk_usage_bytes(settings.snapshot_dir)
    return templates.TemplateResponse(
        request,
        "index.html",
        {
            "health": health,
            "events": events,
            "blacklist": blacklist,
            "whitelist": whitelist,
            "suggestions": suggestions,
            "archive_usage": archive_usage,
            "snapshot_usage": snapshot_usage,
            "now": utc_now_iso(),
            "muted_until": get_setting(settings, "mute_until", ""),
        },
    )


@app.post("/mute")
def mute(minutes: int = Form(...)) -> RedirectResponse:
    set_setting(settings, "mute_until", (utc_now() + timedelta(minutes=minutes)).isoformat())
    return RedirectResponse(url="/", status_code=303)


@app.post("/rules/{target}")
def add_dashboard_rule(target: str, value: str = Form(...), note: str = Form("dashboard")) -> RedirectResponse:
    table = "blacklist_rules" if target == "blacklist" else "whitelist_rules"
    payload = parse_rule_value(value)
    payload["note"] = note
    payload["source"] = "dashboard"
    add_rule(settings, table, payload)
    return RedirectResponse(url="/", status_code=303)
