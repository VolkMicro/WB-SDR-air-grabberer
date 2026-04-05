from __future__ import annotations

import http.server
import json
import logging
import socketserver
import threading
import time
from datetime import timedelta
from pathlib import Path

from app.config import get_settings
from app.db import (
    bucket_for_frequency,
    cleanup_old_data,
    fetch_candidates,
    find_recent_event,
    get_noise_stat,
    get_recent_bucket_hits,
    get_setting,
    init_db,
    insert_event,
    list_rules,
    mark_candidate_processed,
    queue_notification,
    rule_matches,
    set_heartbeat,
    update_existing_event,
    update_noise_stat,
    upsert_blacklist_suggestion,
)
from app.logging_setup import configure_logging
from app.profiles import ScanProfile, load_profiles
from app.utils import atomic_json_write, ensure_directories, utc_now, utc_now_iso

LOGGER = logging.getLogger("event-engine")


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
    with socketserver.TCPServer(("0.0.0.0", 9912), HealthHandler) as server:
        server.serve_forever()


def lookup_profile(profiles: list[ScanProfile], profile_id: str) -> ScanProfile:
    for profile in profiles:
        if profile.profile_id == profile_id:
            return profile
    raise KeyError(profile_id)


def is_muted(settings) -> bool:
    muted_until = get_setting(settings, "mute_until", "")
    return bool(muted_until) and muted_until > utc_now_iso()


def compute_backpressure_flags(profile: ScanProfile, recent_hits: list, signal_db: float) -> list[str]:
    flags: list[str] = []
    hit_count = len(recent_hits)
    if hit_count >= profile.suppression.constant_carrier_hits:
        flags.append("constant_carrier")
    duty_cycle = min(1.0, (hit_count * profile.integration_sec) / max(1, profile.suppression.duty_cycle_window_sec))
    if duty_cycle >= profile.suppression.duty_cycle_ratio:
        flags.append("duty_cycle")
    if hit_count >= profile.suppression.repeated_noise_hits:
        spread = max(float(hit["signal_db"]) for hit in recent_hits) - min(float(hit["signal_db"]) for hit in recent_hits)
        if spread <= 6.0:
            flags.append("repeated_noise")
    if signal_db < profile.min_event_db:
        flags.append("below_min_level")
    return flags


def maybe_capture_authorized_audio(settings, candidate, meta: dict[str, object]) -> str | None:
    if not settings.authorized_audio_mode:
        return None
    if not meta.get("authorized_audio") or not meta.get("authorized_source"):
        return None
    LOGGER.warning("Authorized audio capture for live radio sources is not implemented in MVP and remains disabled")
    return None


def build_notification_payload(event_id: int, event_payload: dict[str, object]) -> dict[str, object]:
    text = (
        f"Event #{event_id}\n"
        f"Profile: {event_payload['profile_id']}\n"
        f"Frequency: {event_payload['frequency_hz'] / 1_000_000:.5f} MHz\n"
        f"Modulation: {event_payload['modulation']}\n"
        f"Bandwidth: {event_payload['bandwidth_hz']} Hz\n"
        f"Signal: {event_payload['signal_db']:.1f} dB\n"
        f"Noise floor: {event_payload['noise_floor_db']:.1f} dB\n"
        f"Duration: {event_payload['duration_sec']:.1f} s\n"
        f"Detector: {event_payload['detector_type']}\n"
        f"Flags: {', '.join(event_payload['suppression_flags']) or 'none'}"
    )
    return {
        "event_id": event_id,
        "text": text,
        "snapshot_path": event_payload.get("snapshot_path"),
        "audio_path": event_payload.get("audio_path"),
        "authorized_audio": bool(event_payload.get("audio_path")),
        "queued_at": utc_now_iso(),
    }


def main() -> None:
    settings = get_settings()
    configure_logging()
    ensure_directories(settings.data_dir, settings.snapshot_dir, settings.archive_dir, settings.telegram_spool_dir)
    init_db(settings)
    profiles = load_profiles(settings.profiles_path)
    health_thread = threading.Thread(target=serve_health, daemon=True)
    health_thread.start()
    last_cleanup = 0.0

    while True:
        processed = 0
        candidates = fetch_candidates(settings)
        for candidate in candidates:
            processed += 1
            profile = lookup_profile(profiles, str(candidate["profile_id"]))
            candidate_meta = json.loads(candidate["meta_json"] or "{}")
            frequency_hz = int(candidate["frequency_hz"])
            bandwidth_hz = int(candidate["bandwidth_hz"])
            bucket_hz = bucket_for_frequency(frequency_hz, bandwidth_hz)
            noise_stat = get_noise_stat(settings, profile.profile_id, bucket_hz)
            baseline = float(candidate["noise_floor_db"]) if noise_stat is None else float(noise_stat["mean_db"])
            adaptive_threshold = max(profile.min_event_db, baseline + profile.threshold_offset_db)
            recent_hits = get_recent_bucket_hits(
                settings,
                profile.profile_id,
                bucket_hz,
                (utc_now() - timedelta(seconds=profile.suppression.repeated_noise_window_sec)).isoformat(),
            )

            suppression_flags: list[str] = []
            if rule_matches(settings, "whitelist_rules", frequency_hz):
                suppression_flags.append("whitelisted")
            else:
                if rule_matches(settings, "blacklist_rules", frequency_hz):
                    suppression_flags.append("blacklist")
                suppression_flags.extend(compute_backpressure_flags(profile, recent_hits, float(candidate["signal_db"])))
            if float(candidate["signal_db"]) < adaptive_threshold and not settings.aggressive_detect:
                suppression_flags.append("adaptive_threshold")
            if is_muted(settings):
                suppression_flags.append("muted")

            suppressed = any(flag for flag in suppression_flags if flag not in {"whitelisted"})
            update_noise_stat(settings, profile.profile_id, bucket_hz, float(candidate["signal_db"]), not suppressed)

            if suppressed:
                mark_candidate_processed(settings, int(candidate["id"]), ",".join(suppression_flags))
                if "repeated_noise" in suppression_flags or "constant_carrier" in suppression_flags:
                    upsert_blacklist_suggestion(
                        settings,
                        profile.profile_id,
                        bucket_hz - (bandwidth_hz // 2),
                        bucket_hz + (bandwidth_hz // 2),
                        ",".join(suppression_flags),
                    )
                continue

            dedup_since = (utc_now() - timedelta(seconds=profile.suppression.dedup_window_sec)).isoformat()
            existing = find_recent_event(settings, profile.profile_id, frequency_hz, bandwidth_hz, dedup_since)
            if existing is not None:
                update_existing_event(settings, int(existing["id"]), float(candidate["signal_db"]), float(candidate["integration_sec"]))
                mark_candidate_processed(settings, int(candidate["id"]), "dedup")
                continue

            audio_path = maybe_capture_authorized_audio(settings, candidate, candidate_meta)
            event_payload = {
                "profile_id": profile.profile_id,
                "profile_label": profile.label,
                "frequency_hz": frequency_hz,
                "modulation": str(candidate["modulation"]),
                "bandwidth_hz": bandwidth_hz,
                "signal_db": float(candidate["signal_db"]),
                "noise_floor_db": float(candidate["noise_floor_db"]),
                "duration_sec": float(candidate["integration_sec"]),
                "detector_type": str(candidate["detector_type"]),
                "suppression_flags": suppression_flags,
                "delivery_state": "queued",
                "live_state": "queued" if settings.telegram_enabled else "local_only",
                "snapshot_path": candidate["snapshot_path"],
                "audio_path": audio_path,
                "meta": {
                    "candidate_id": int(candidate["id"]),
                    "adaptive_threshold": adaptive_threshold,
                },
            }
            event_id = insert_event(settings, event_payload)
            spool_path = settings.telegram_spool_dir / f"event-{event_id}.json"
            queue_payload = build_notification_payload(event_id, event_payload)
            atomic_json_write(spool_path, queue_payload)
            queue_notification(settings, event_id, spool_path, queue_payload)
            mark_candidate_processed(settings, int(candidate["id"]), None)

        if time.time() - last_cleanup >= 3600:
            cleanup_stats = cleanup_old_data(settings)
            set_heartbeat(settings, "event-engine", "ok", {"processed": processed, **cleanup_stats})
            last_cleanup = time.time()
        else:
            set_heartbeat(settings, "event-engine", "ok", {"processed": processed})
        time.sleep(2.0 if processed == 0 else 0.25)


if __name__ == "__main__":
    main()
