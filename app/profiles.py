from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml


@dataclass(slots=True)
class SuppressionConfig:
    dedup_window_sec: int = 45
    constant_carrier_window_sec: int = 300
    constant_carrier_hits: int = 8
    duty_cycle_window_sec: int = 600
    duty_cycle_ratio: float = 0.4
    repeated_noise_window_sec: int = 1800
    repeated_noise_hits: int = 12


@dataclass(slots=True)
class ScanProfile:
    profile_id: str
    label: str
    enabled: bool
    priority: int
    start_mhz: float
    end_mhz: float
    bin_size_hz: int
    integration_sec: int
    gain: str
    ppm: int
    modulation: str
    bandwidth_hz: int
    min_event_db: float
    threshold_offset_db: float
    max_peaks: int
    authorized_audio: bool
    authorized_source: bool
    suppression: SuppressionConfig

    @property
    def start_hz(self) -> int:
        return int(self.start_mhz * 1_000_000)

    @property
    def end_hz(self) -> int:
        return int(self.end_mhz * 1_000_000)


def _to_profile(raw: dict[str, Any]) -> ScanProfile:
    suppression = SuppressionConfig(**raw.get("suppression", {}))
    return ScanProfile(
        profile_id=raw["id"],
        label=raw.get("label", raw["id"]),
        enabled=bool(raw.get("enabled", True)),
        priority=int(raw.get("priority", 100)),
        start_mhz=float(raw["start_mhz"]),
        end_mhz=float(raw["end_mhz"]),
        bin_size_hz=int(raw.get("bin_size_hz", 12500)),
        integration_sec=int(raw.get("integration_sec", 5)),
        gain=str(raw.get("gain", "auto")),
        ppm=int(raw.get("ppm", 0)),
        modulation=str(raw.get("modulation", "energy")),
        bandwidth_hz=int(raw.get("bandwidth_hz", raw.get("bin_size_hz", 12500))),
        min_event_db=float(raw.get("min_event_db", -55.0)),
        threshold_offset_db=float(raw.get("threshold_offset_db", 10.0)),
        max_peaks=int(raw.get("max_peaks", 3)),
        authorized_audio=bool(raw.get("authorized_audio", False)),
        authorized_source=bool(raw.get("authorized_source", False)),
        suppression=suppression,
    )


def load_profiles(path: Path) -> list[ScanProfile]:
    data = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    profiles = [_to_profile(item) for item in data.get("profiles", [])]
    profiles = [profile for profile in profiles if profile.enabled]
    profiles.sort(key=lambda item: item.priority, reverse=True)
    return profiles
