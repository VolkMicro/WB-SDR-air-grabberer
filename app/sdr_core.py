from __future__ import annotations

import csv
import http.server
import logging
import math
import random
import socketserver
import subprocess
import tempfile
import threading
import time
from pathlib import Path

from app.config import get_settings
from app.db import init_db, insert_candidate, set_heartbeat, update_scan_state
from app.logging_setup import configure_logging
from app.profiles import ScanProfile, load_profiles
from app.utils import ensure_directories, utc_now_iso

LOGGER = logging.getLogger("sdr-core")


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
    with socketserver.TCPServer(("0.0.0.0", 9911), HealthHandler) as server:
        server.serve_forever()


def percentile(values: list[float], pct: float) -> float:
    if not values:
        return -120.0
    idx = max(0, min(len(values) - 1, int(len(values) * pct)))
    ordered = sorted(values)
    return ordered[idx]


def write_svg_snapshot(path: Path, bins: list[tuple[int, float]], title: str) -> None:
    width = 960
    height = 260
    min_db = min(level for _, level in bins)
    max_db = max(level for _, level in bins)
    span = max(1.0, max_db - min_db)
    points: list[str] = []
    for index, (_, level) in enumerate(bins):
        x = int(index / max(1, len(bins) - 1) * (width - 40)) + 20
        y = int(height - 30 - ((level - min_db) / span) * (height - 60))
        points.append(f"{x},{y}")
    svg = f"""<svg xmlns='http://www.w3.org/2000/svg' width='{width}' height='{height}' viewBox='0 0 {width} {height}'>
<rect width='100%' height='100%' fill='#0b1020'/>
<text x='20' y='24' fill='#e2e8f0' font-size='18'>{title}</text>
<polyline fill='none' stroke='#38bdf8' stroke-width='2' points='{' '.join(points)}'/>
<line x1='20' y1='{height - 30}' x2='{width - 20}' y2='{height - 30}' stroke='#334155'/>
</svg>
"""
    path.write_text(svg, encoding="utf-8")


def parse_rtl_power_csv(csv_path: Path) -> list[tuple[int, float]]:
    bins: list[tuple[int, float]] = []
    with csv_path.open("r", encoding="utf-8") as handle:
        for row in csv.reader(handle):
            if len(row) < 7:
                continue
            low_hz = int(float(row[2].strip()))
            step_hz = int(float(row[4].strip()))
            power_values = [float(item) for item in row[6:] if item.strip()]
            for index, power_db in enumerate(power_values):
                bins.append((low_hz + index * step_hz, power_db))
    return bins


def generate_fake_bins(profile: ScanProfile) -> list[tuple[int, float]]:
    bins: list[tuple[int, float]] = []
    total_bins = max(10, int((profile.end_hz - profile.start_hz) / profile.bin_size_hz) + 1)
    spike_index = random.randint(0, total_bins - 1)
    for index in range(total_bins):
        frequency_hz = profile.start_hz + index * profile.bin_size_hz
        noise = random.uniform(-92.0, -76.0)
        if random.random() > 0.88 and index == spike_index:
            noise = random.uniform(-48.0, -30.0)
        bins.append((frequency_hz, noise))
    return bins


def run_scan(profile: ScanProfile, fake_sdr: bool, timeout_sec: int) -> list[tuple[int, float]]:
    if fake_sdr:
        return generate_fake_bins(profile)
    with tempfile.TemporaryDirectory() as tmp_dir:
        csv_path = Path(tmp_dir) / "scan.csv"
        frequency_arg = f"{profile.start_hz}:{profile.end_hz}:{profile.bin_size_hz}"
        command = ["rtl_power", "-f", frequency_arg, "-i", str(profile.integration_sec), "-1", str(csv_path)]
        if profile.gain != "auto":
            command.extend(["-g", profile.gain])
        command.extend(["-p", str(profile.ppm)])
        LOGGER.debug("Running scan command: %s", " ".join(command))
        subprocess.run(command, check=True, timeout=timeout_sec)
        return parse_rtl_power_csv(csv_path)


def select_peaks(profile: ScanProfile, bins: list[tuple[int, float]]) -> tuple[float, list[tuple[int, float]]]:
    levels = [level for _, level in bins]
    noise_floor = percentile(levels, 0.6)
    min_level = max(profile.min_event_db, noise_floor + profile.threshold_offset_db)
    peaks = sorted((item for item in bins if item[1] >= min_level), key=lambda item: item[1], reverse=True)
    deconflicted: list[tuple[int, float]] = []
    for frequency_hz, level in peaks:
        if any(abs(existing_hz - frequency_hz) < profile.bandwidth_hz for existing_hz, _ in deconflicted):
            continue
        deconflicted.append((frequency_hz, level))
        if len(deconflicted) >= profile.max_peaks:
            break
    return noise_floor, deconflicted


def main() -> None:
    settings = get_settings()
    configure_logging()
    ensure_directories(settings.data_dir, settings.snapshot_dir, settings.archive_dir, settings.telegram_spool_dir)
    init_db(settings)
    profiles = load_profiles(settings.profiles_path)
    if not profiles:
        raise RuntimeError("No enabled profiles found in profiles.yaml")

    health_thread = threading.Thread(target=serve_health, daemon=True)
    health_thread.start()

    LOGGER.info("Loaded %s scan profiles", len(profiles))
    while True:
        for profile in profiles:
            started_at = utc_now_iso()
            update_scan_state(
                settings,
                profile.profile_id,
                profile.label,
                {"state": "scanning", "started_at": started_at},
                started=True,
            )
            try:
                bins = run_scan(profile, settings.fake_sdr, settings.scan_timeout_sec)
                if not bins:
                    raise RuntimeError("rtl_power produced no bins")
                noise_floor, peaks = select_peaks(profile, bins)
                snapshot_name = f"{int(time.time())}_{profile.profile_id}.svg"
                snapshot_path = settings.snapshot_dir / snapshot_name
                write_svg_snapshot(snapshot_path, bins, f"{profile.label} {started_at}")
                for frequency_hz, signal_db in peaks:
                    insert_candidate(
                        settings,
                        {
                            "profile_id": profile.profile_id,
                            "profile_label": profile.label,
                            "frequency_hz": frequency_hz,
                            "modulation": profile.modulation,
                            "bandwidth_hz": profile.bandwidth_hz,
                            "signal_db": signal_db,
                            "noise_floor_db": noise_floor,
                            "detector_type": "energy_peak",
                            "integration_sec": profile.integration_sec,
                            "snapshot_path": str(snapshot_path),
                            "meta": {
                                "authorized_audio": profile.authorized_audio,
                                "authorized_source": profile.authorized_source,
                                "threshold_db": max(profile.min_event_db, noise_floor + profile.threshold_offset_db),
                            },
                        },
                    )
                set_heartbeat(
                    settings,
                    "sdr-core",
                    "ok",
                    {
                        "profile": profile.profile_id,
                        "peaks": len(peaks),
                        "noise_floor_db": round(noise_floor, 2),
                        "fake_sdr": settings.fake_sdr,
                    },
                )
                update_scan_state(
                    settings,
                    profile.profile_id,
                    profile.label,
                    {"state": "idle", "finished_at": utc_now_iso(), "peaks": len(peaks)},
                    started=False,
                )
            except Exception as exc:
                LOGGER.exception("Scan failed for profile %s", profile.profile_id)
                set_heartbeat(
                    settings,
                    "sdr-core",
                    "degraded",
                    {"profile": profile.profile_id, "error": str(exc)},
                )
            time.sleep(settings.scan_loop_pause_sec)


if __name__ == "__main__":
    main()
