from __future__ import annotations

import logging
import math
import struct
import wave
from pathlib import Path

from app.config import get_settings
from app.db import init_db, insert_event, queue_notification
from app.logging_setup import configure_logging
from app.utils import atomic_json_write, ensure_directories, utc_now_iso

LOGGER = logging.getLogger("selftest")


def generate_snapshot(snapshot_path: Path) -> None:
    svg = """<svg xmlns='http://www.w3.org/2000/svg' width='960' height='240'>
<rect width='100%' height='100%' fill='#0b1020'/>
<text x='20' y='24' fill='#e2e8f0' font-size='18'>Synthetic self-test spectrum</text>
<polyline fill='none' stroke='#38bdf8' stroke-width='2' points='20,200 120,180 240,170 360,80 500,170 620,180 760,190 920,195'/>
</svg>
"""
    snapshot_path.write_text(svg, encoding="utf-8")


def generate_audio(settings, audio_path: Path) -> str | None:
    if not settings.authorized_audio_mode or not settings.selftest_audio_enabled:
        return None
    duration_sec = min(settings.audio_clip_max_sec, settings.selftest_audio_sec)
    sample_rate = 16000
    amplitude = 16000
    wav_path = audio_path.with_suffix(".wav")
    with wave.open(str(wav_path), "wb") as wav_file:
        wav_file.setnchannels(1)
        wav_file.setsampwidth(2)
        wav_file.setframerate(sample_rate)
        total_samples = int(sample_rate * duration_sec)
        frames = bytearray()
        for index in range(total_samples):
            value = int(amplitude * math.sin(2 * math.pi * settings.selftest_audio_freq_hz * index / sample_rate))
            frames.extend(struct.pack("<h", value))
        wav_file.writeframes(bytes(frames))
    return str(wav_path)


def main() -> None:
    settings = get_settings()
    configure_logging()
    ensure_directories(settings.data_dir, settings.snapshot_dir, settings.archive_dir, settings.telegram_spool_dir)
    init_db(settings)

    snapshot_path = settings.snapshot_dir / f"selftest-{utc_now_iso().replace(':', '-')}.svg"
    audio_path = settings.archive_dir / f"selftest-{utc_now_iso().replace(':', '-')}.ogg"
    generate_snapshot(snapshot_path)
    resolved_audio_path = generate_audio(settings, audio_path)

    payload = {
        "profile_id": "selftest-authorized",
        "profile_label": "Authorized self-test",
        "frequency_hz": 433_920_000,
        "modulation": "authorized_test",
        "bandwidth_hz": 12500,
        "signal_db": -24.0,
        "noise_floor_db": -86.0,
        "duration_sec": float(min(settings.audio_clip_max_sec, settings.selftest_audio_sec)),
        "detector_type": "selftest",
        "suppression_flags": ["selftest"],
        "delivery_state": "queued",
        "live_state": "queued" if settings.telegram_enabled else "local_only",
        "snapshot_path": str(snapshot_path),
        "audio_path": resolved_audio_path,
        "meta": {"authorized_audio": bool(resolved_audio_path), "selftest": True},
    }
    event_id = insert_event(settings, payload)
    queue_payload = {
        "event_id": event_id,
        "text": "Authorized self-test event\nProfile: selftest-authorized\nFrequency: 433.92000 MHz\nDetector: selftest\nFlags: selftest",
        "snapshot_path": str(snapshot_path),
        "audio_path": resolved_audio_path,
        "authorized_audio": bool(resolved_audio_path),
        "queued_at": utc_now_iso(),
    }
    spool_path = settings.telegram_spool_dir / f"event-{event_id}.json"
    atomic_json_write(spool_path, queue_payload)
    queue_notification(settings, event_id, spool_path, queue_payload)
    LOGGER.info("Self-test event queued: event_id=%s snapshot=%s audio=%s", event_id, snapshot_path, resolved_audio_path)


if __name__ == "__main__":
    main()
