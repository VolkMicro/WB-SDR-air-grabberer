from __future__ import annotations

import json
import sqlite3
from datetime import timedelta
from pathlib import Path
from typing import Any

from app.config import Settings
from app.utils import utc_now, utc_now_iso


SCHEMA = """
PRAGMA journal_mode=WAL;
PRAGMA synchronous=NORMAL;
PRAGMA foreign_keys=ON;

CREATE TABLE IF NOT EXISTS service_heartbeats (
    service TEXT PRIMARY KEY,
    status TEXT NOT NULL,
    details_json TEXT NOT NULL DEFAULT '{}',
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS scan_state (
    id INTEGER PRIMARY KEY CHECK (id = 1),
    profile_id TEXT,
    label TEXT,
    last_started_at TEXT,
    last_finished_at TEXT,
    details_json TEXT NOT NULL DEFAULT '{}'
);

CREATE TABLE IF NOT EXISTS scan_candidates (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    created_at TEXT NOT NULL,
    profile_id TEXT NOT NULL,
    profile_label TEXT NOT NULL,
    frequency_hz INTEGER NOT NULL,
    modulation TEXT NOT NULL,
    bandwidth_hz INTEGER NOT NULL,
    signal_db REAL NOT NULL,
    noise_floor_db REAL NOT NULL,
    detector_type TEXT NOT NULL,
    integration_sec REAL NOT NULL,
    snapshot_path TEXT,
    meta_json TEXT NOT NULL DEFAULT '{}',
    processed INTEGER NOT NULL DEFAULT 0,
    processed_at TEXT,
    suppression_reason TEXT
);

CREATE INDEX IF NOT EXISTS idx_scan_candidates_processed_created
    ON scan_candidates(processed, created_at);

CREATE TABLE IF NOT EXISTS events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    profile_id TEXT NOT NULL,
    profile_label TEXT NOT NULL,
    frequency_hz INTEGER NOT NULL,
    modulation TEXT NOT NULL,
    bandwidth_hz INTEGER NOT NULL,
    signal_db REAL NOT NULL,
    noise_floor_db REAL NOT NULL,
    duration_sec REAL NOT NULL,
    detector_type TEXT NOT NULL,
    suppression_flags_json TEXT NOT NULL DEFAULT '[]',
    delivery_state TEXT NOT NULL DEFAULT 'queued',
    live_state TEXT NOT NULL DEFAULT 'live',
    duplicate_count INTEGER NOT NULL DEFAULT 0,
    snapshot_path TEXT,
    audio_path TEXT,
    meta_json TEXT NOT NULL DEFAULT '{}'
);

CREATE INDEX IF NOT EXISTS idx_events_created_at ON events(created_at);
CREATE INDEX IF NOT EXISTS idx_events_profile_frequency ON events(profile_id, frequency_hz, created_at);

CREATE TABLE IF NOT EXISTS notification_queue (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    event_id INTEGER NOT NULL,
    spool_path TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending',
    attempts INTEGER NOT NULL DEFAULT 0,
    next_attempt_at TEXT NOT NULL,
    last_error TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    FOREIGN KEY(event_id) REFERENCES events(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_notification_queue_due
    ON notification_queue(status, next_attempt_at);

CREATE TABLE IF NOT EXISTS blacklist_rules (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    rule_type TEXT NOT NULL,
    exact_hz INTEGER,
    start_hz INTEGER,
    end_hz INTEGER,
    note TEXT,
    source TEXT NOT NULL,
    created_at TEXT NOT NULL,
    expires_at TEXT
);

CREATE TABLE IF NOT EXISTS whitelist_rules (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    rule_type TEXT NOT NULL,
    exact_hz INTEGER,
    start_hz INTEGER,
    end_hz INTEGER,
    note TEXT,
    source TEXT NOT NULL,
    created_at TEXT NOT NULL,
    expires_at TEXT
);

CREATE TABLE IF NOT EXISTS noise_stats (
    profile_id TEXT NOT NULL,
    frequency_bucket_hz INTEGER NOT NULL,
    sample_count INTEGER NOT NULL,
    mean_db REAL NOT NULL,
    max_db REAL NOT NULL,
    hit_count INTEGER NOT NULL,
    last_seen_at TEXT NOT NULL,
    PRIMARY KEY(profile_id, frequency_bucket_hz)
);

CREATE TABLE IF NOT EXISTS blacklist_suggestions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    profile_id TEXT NOT NULL,
    start_hz INTEGER NOT NULL,
    end_hz INTEGER NOT NULL,
    reason TEXT NOT NULL,
    hit_count INTEGER NOT NULL,
    created_at TEXT NOT NULL,
    last_seen_at TEXT NOT NULL,
    promoted INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS kv_settings (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL,
    updated_at TEXT NOT NULL
);
"""


def connect(db_path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path, timeout=30, isolation_level=None, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn


def init_db(settings: Settings) -> None:
    settings.db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = connect(settings.db_path)
    try:
        conn.executescript(SCHEMA)
        conn.execute("INSERT OR IGNORE INTO scan_state(id, details_json) VALUES(1, '{}')")
    finally:
        conn.close()


def set_heartbeat(settings: Settings, service: str, status: str, details: dict[str, Any]) -> None:
    conn = connect(settings.db_path)
    try:
        conn.execute(
            """
            INSERT INTO service_heartbeats(service, status, details_json, updated_at)
            VALUES(?, ?, ?, ?)
            ON CONFLICT(service) DO UPDATE SET
                status=excluded.status,
                details_json=excluded.details_json,
                updated_at=excluded.updated_at
            """,
            (service, status, json.dumps(details), utc_now_iso()),
        )
    finally:
        conn.close()


def update_scan_state(settings: Settings, profile_id: str, label: str, details: dict[str, Any], started: bool) -> None:
    conn = connect(settings.db_path)
    try:
        now = utc_now_iso()
        if started:
            conn.execute(
                "UPDATE scan_state SET profile_id=?, label=?, last_started_at=?, details_json=? WHERE id=1",
                (profile_id, label, now, json.dumps(details)),
            )
        else:
            conn.execute(
                "UPDATE scan_state SET profile_id=?, label=?, last_finished_at=?, details_json=? WHERE id=1",
                (profile_id, label, now, json.dumps(details)),
            )
    finally:
        conn.close()


def insert_candidate(settings: Settings, payload: dict[str, Any]) -> None:
    conn = connect(settings.db_path)
    try:
        conn.execute(
            """
            INSERT INTO scan_candidates(
                created_at, profile_id, profile_label, frequency_hz, modulation, bandwidth_hz,
                signal_db, noise_floor_db, detector_type, integration_sec, snapshot_path, meta_json
            ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                utc_now_iso(),
                payload["profile_id"],
                payload["profile_label"],
                payload["frequency_hz"],
                payload["modulation"],
                payload["bandwidth_hz"],
                payload["signal_db"],
                payload["noise_floor_db"],
                payload["detector_type"],
                payload["integration_sec"],
                payload.get("snapshot_path"),
                json.dumps(payload.get("meta", {})),
            ),
        )
    finally:
        conn.close()


def fetch_candidates(settings: Settings, limit: int = 64) -> list[sqlite3.Row]:
    conn = connect(settings.db_path)
    try:
        rows = conn.execute(
            "SELECT * FROM scan_candidates WHERE processed=0 ORDER BY id ASC LIMIT ?",
            (limit,),
        ).fetchall()
        return rows
    finally:
        conn.close()


def mark_candidate_processed(settings: Settings, candidate_id: int, reason: str | None) -> None:
    conn = connect(settings.db_path)
    try:
        conn.execute(
            "UPDATE scan_candidates SET processed=1, processed_at=?, suppression_reason=? WHERE id=?",
            (utc_now_iso(), reason, candidate_id),
        )
    finally:
        conn.close()


def bucket_for_frequency(frequency_hz: int, bandwidth_hz: int) -> int:
    bucket_width = max(5_000, bandwidth_hz)
    return int(round(frequency_hz / bucket_width) * bucket_width)


def get_noise_stat(settings: Settings, profile_id: str, bucket_hz: int) -> sqlite3.Row | None:
    conn = connect(settings.db_path)
    try:
        return conn.execute(
            "SELECT * FROM noise_stats WHERE profile_id=? AND frequency_bucket_hz=?",
            (profile_id, bucket_hz),
        ).fetchone()
    finally:
        conn.close()


def update_noise_stat(settings: Settings, profile_id: str, bucket_hz: int, signal_db: float, hit: bool) -> None:
    current = get_noise_stat(settings, profile_id, bucket_hz)
    now = utc_now_iso()
    conn = connect(settings.db_path)
    try:
        if current is None:
            conn.execute(
                """
                INSERT INTO noise_stats(profile_id, frequency_bucket_hz, sample_count, mean_db, max_db, hit_count, last_seen_at)
                VALUES(?, ?, 1, ?, ?, ?, ?)
                """,
                (profile_id, bucket_hz, signal_db, signal_db, int(hit), now),
            )
            return
        sample_count = int(current["sample_count"]) + 1
        mean_db = ((float(current["mean_db"]) * int(current["sample_count"])) + signal_db) / sample_count
        max_db = max(float(current["max_db"]), signal_db)
        hit_count = int(current["hit_count"]) + int(hit)
        conn.execute(
            """
            UPDATE noise_stats
            SET sample_count=?, mean_db=?, max_db=?, hit_count=?, last_seen_at=?
            WHERE profile_id=? AND frequency_bucket_hz=?
            """,
            (sample_count, mean_db, max_db, hit_count, now, profile_id, bucket_hz),
        )
    finally:
        conn.close()


def get_recent_bucket_hits(settings: Settings, profile_id: str, bucket_hz: int, since_iso: str) -> list[sqlite3.Row]:
    conn = connect(settings.db_path)
    try:
        rows = conn.execute(
            """
            SELECT id, created_at, signal_db, frequency_hz, bandwidth_hz
            FROM scan_candidates
            WHERE profile_id=?
              AND frequency_hz BETWEEN ? AND ?
              AND created_at >= ?
            ORDER BY created_at DESC
            """,
            (profile_id, bucket_hz - 5_000, bucket_hz + 5_000, since_iso),
        ).fetchall()
        return rows
    finally:
        conn.close()


def find_recent_event(settings: Settings, profile_id: str, frequency_hz: int, bandwidth_hz: int, since_iso: str) -> sqlite3.Row | None:
    conn = connect(settings.db_path)
    try:
        return conn.execute(
            """
            SELECT * FROM events
            WHERE profile_id=?
              AND frequency_hz BETWEEN ? AND ?
              AND updated_at >= ?
            ORDER BY updated_at DESC
            LIMIT 1
            """,
            (profile_id, frequency_hz - bandwidth_hz, frequency_hz + bandwidth_hz, since_iso),
        ).fetchone()
    finally:
        conn.close()


def update_existing_event(settings: Settings, event_id: int, signal_db: float, duration_increment: float) -> None:
    conn = connect(settings.db_path)
    try:
        conn.execute(
            """
            UPDATE events
            SET updated_at=?, duration_sec=duration_sec + ?, signal_db=MAX(signal_db, ?), duplicate_count=duplicate_count + 1
            WHERE id=?
            """,
            (utc_now_iso(), duration_increment, signal_db, event_id),
        )
    finally:
        conn.close()


def insert_event(settings: Settings, payload: dict[str, Any]) -> int:
    conn = connect(settings.db_path)
    try:
        cursor = conn.execute(
            """
            INSERT INTO events(
                created_at, updated_at, profile_id, profile_label, frequency_hz, modulation, bandwidth_hz,
                signal_db, noise_floor_db, duration_sec, detector_type, suppression_flags_json,
                delivery_state, live_state, snapshot_path, audio_path, meta_json
            ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                utc_now_iso(),
                utc_now_iso(),
                payload["profile_id"],
                payload["profile_label"],
                payload["frequency_hz"],
                payload["modulation"],
                payload["bandwidth_hz"],
                payload["signal_db"],
                payload["noise_floor_db"],
                payload["duration_sec"],
                payload["detector_type"],
                json.dumps(payload["suppression_flags"]),
                payload.get("delivery_state", "queued"),
                payload.get("live_state", "live"),
                payload.get("snapshot_path"),
                payload.get("audio_path"),
                json.dumps(payload.get("meta", {})),
            ),
        )
        return int(cursor.lastrowid)
    finally:
        conn.close()


def queue_notification(settings: Settings, event_id: int, spool_path: Path, payload: dict[str, Any]) -> None:
    conn = connect(settings.db_path)
    try:
        now = utc_now_iso()
        conn.execute(
            """
            INSERT INTO notification_queue(event_id, spool_path, payload_json, status, attempts, next_attempt_at, created_at, updated_at)
            VALUES(?, ?, ?, 'pending', 0, ?, ?, ?)
            """,
            (event_id, str(spool_path), json.dumps(payload), now, now, now),
        )
    finally:
        conn.close()


def fetch_due_notifications(settings: Settings, limit: int = 20) -> list[sqlite3.Row]:
    conn = connect(settings.db_path)
    try:
        rows = conn.execute(
            """
            SELECT * FROM notification_queue
            WHERE status IN ('pending', 'retry') AND next_attempt_at <= ?
            ORDER BY id ASC
            LIMIT ?
            """,
            (utc_now_iso(), limit),
        ).fetchall()
        return rows
    finally:
        conn.close()


def mark_notification_sent(settings: Settings, queue_id: int, event_id: int) -> None:
    conn = connect(settings.db_path)
    try:
        now = utc_now_iso()
        conn.execute(
            "UPDATE notification_queue SET status='sent', updated_at=? WHERE id=?",
            (now, queue_id),
        )
        conn.execute(
            "UPDATE events SET delivery_state='sent', updated_at=? WHERE id=?",
            (now, event_id),
        )
    finally:
        conn.close()


def mark_notification_retry(settings: Settings, queue_id: int, error: str, next_attempt_at: str) -> None:
    conn = connect(settings.db_path)
    try:
        conn.execute(
            """
            UPDATE notification_queue
            SET status='retry', attempts=attempts + 1, last_error=?, next_attempt_at=?, updated_at=?
            WHERE id=?
            """,
            (error[:500], next_attempt_at, utc_now_iso(), queue_id),
        )
    finally:
        conn.close()


def list_recent_events(settings: Settings, limit: int = 20) -> list[sqlite3.Row]:
    conn = connect(settings.db_path)
    try:
        return conn.execute("SELECT * FROM events ORDER BY id DESC LIMIT ?", (limit,)).fetchall()
    finally:
        conn.close()


def get_last_snapshot(settings: Settings) -> str | None:
    conn = connect(settings.db_path)
    try:
        row = conn.execute(
            "SELECT snapshot_path FROM events WHERE snapshot_path IS NOT NULL ORDER BY id DESC LIMIT 1"
        ).fetchone()
        return None if row is None else str(row["snapshot_path"])
    finally:
        conn.close()


def list_rules(settings: Settings, table_name: str) -> list[sqlite3.Row]:
    conn = connect(settings.db_path)
    try:
        return conn.execute(f"SELECT * FROM {table_name} ORDER BY id DESC").fetchall()
    finally:
        conn.close()


def add_rule(settings: Settings, table_name: str, rule: dict[str, Any]) -> None:
    conn = connect(settings.db_path)
    try:
        conn.execute(
            f"""
            INSERT INTO {table_name}(rule_type, exact_hz, start_hz, end_hz, note, source, created_at, expires_at)
            VALUES(?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                rule["rule_type"],
                rule.get("exact_hz"),
                rule.get("start_hz"),
                rule.get("end_hz"),
                rule.get("note"),
                rule.get("source", "manual"),
                utc_now_iso(),
                rule.get("expires_at"),
            ),
        )
    finally:
        conn.close()


def rule_matches(settings: Settings, table_name: str, frequency_hz: int) -> bool:
    conn = connect(settings.db_path)
    try:
        row = conn.execute(
            f"""
            SELECT 1 FROM {table_name}
            WHERE (exact_hz IS NOT NULL AND exact_hz = ?)
               OR (start_hz IS NOT NULL AND end_hz IS NOT NULL AND ? BETWEEN start_hz AND end_hz)
            LIMIT 1
            """,
            (frequency_hz, frequency_hz),
        ).fetchone()
        return row is not None
    finally:
        conn.close()


def upsert_blacklist_suggestion(settings: Settings, profile_id: str, start_hz: int, end_hz: int, reason: str) -> None:
    conn = connect(settings.db_path)
    try:
        existing = conn.execute(
            """
            SELECT id, hit_count FROM blacklist_suggestions
            WHERE profile_id=? AND start_hz=? AND end_hz=? AND promoted=0
            LIMIT 1
            """,
            (profile_id, start_hz, end_hz),
        ).fetchone()
        if existing is None:
            conn.execute(
                """
                INSERT INTO blacklist_suggestions(profile_id, start_hz, end_hz, reason, hit_count, created_at, last_seen_at)
                VALUES(?, ?, ?, ?, 1, ?, ?)
                """,
                (profile_id, start_hz, end_hz, reason, utc_now_iso(), utc_now_iso()),
            )
        else:
            conn.execute(
                "UPDATE blacklist_suggestions SET hit_count=?, last_seen_at=? WHERE id=?",
                (int(existing["hit_count"]) + 1, utc_now_iso(), int(existing["id"])),
            )
    finally:
        conn.close()


def list_blacklist_suggestions(settings: Settings, limit: int = 20) -> list[sqlite3.Row]:
    conn = connect(settings.db_path)
    try:
        return conn.execute(
            "SELECT * FROM blacklist_suggestions WHERE promoted=0 ORDER BY hit_count DESC, id DESC LIMIT ?",
            (limit,),
        ).fetchall()
    finally:
        conn.close()


def get_setting(settings: Settings, key: str, default: str = "") -> str:
    conn = connect(settings.db_path)
    try:
        row = conn.execute("SELECT value FROM kv_settings WHERE key=?", (key,)).fetchone()
        return default if row is None else str(row["value"])
    finally:
        conn.close()


def set_setting(settings: Settings, key: str, value: str) -> None:
    conn = connect(settings.db_path)
    try:
        conn.execute(
            """
            INSERT INTO kv_settings(key, value, updated_at)
            VALUES(?, ?, ?)
            ON CONFLICT(key) DO UPDATE SET value=excluded.value, updated_at=excluded.updated_at
            """,
            (key, value, utc_now_iso()),
        )
    finally:
        conn.close()


def get_health_snapshot(settings: Settings) -> dict[str, Any]:
    conn = connect(settings.db_path)
    try:
        heartbeats = [dict(row) for row in conn.execute("SELECT * FROM service_heartbeats").fetchall()]
        state = conn.execute("SELECT * FROM scan_state WHERE id=1").fetchone()
        queue_pending = conn.execute(
            "SELECT COUNT(*) AS count FROM notification_queue WHERE status IN ('pending', 'retry')"
        ).fetchone()["count"]
        recent_horizon = (utc_now() - timedelta(hours=24)).isoformat()
        recent_events = conn.execute(
            "SELECT COUNT(*) AS count FROM events WHERE created_at >= ?",
            (recent_horizon,),
        ).fetchone()["count"]
        return {
            "heartbeats": heartbeats,
            "scan_state": None if state is None else dict(state),
            "queue_pending": queue_pending,
            "recent_events_24h": recent_events,
            "mute_until": get_setting(settings, "mute_until", ""),
        }
    finally:
        conn.close()


def cleanup_old_data(settings: Settings) -> dict[str, int]:
    conn = connect(settings.db_path)
    try:
        horizon = (utc_now() - timedelta(hours=int(settings.retention_hours))).isoformat()
        deleted_candidates = conn.execute(
            "DELETE FROM scan_candidates WHERE created_at < ? AND processed=1",
            (horizon,),
        ).rowcount
        deleted_queue = conn.execute(
            "DELETE FROM notification_queue WHERE updated_at < ? AND status='sent'",
            (horizon,),
        ).rowcount
        deleted_events = conn.execute(
            "DELETE FROM events WHERE created_at < ?",
            (horizon,),
        ).rowcount
        return {
            "deleted_candidates": deleted_candidates,
            "deleted_queue": deleted_queue,
            "deleted_events": deleted_events,
        }
    finally:
        conn.close()
