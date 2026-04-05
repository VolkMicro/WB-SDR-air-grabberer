from functools import lru_cache
from pathlib import Path

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    app_env: str = "production"
    tz: str = "UTC"

    data_dir: Path = Path("/data")
    db_path: Path = Path("/data/airgrabber.db")
    profiles_path: Path = Path("/app/profiles.yaml")
    snapshot_dir: Path = Path("/data/snapshots")
    archive_dir: Path = Path("/data/archive")
    telegram_spool_dir: Path = Path("/data/spool/telegram")

    dashboard_host: str = "0.0.0.0"
    dashboard_port: int = 8080

    fake_sdr: bool = False
    aggressive_detect: bool = False
    authorized_audio_mode: bool = False

    scan_timeout_sec: int = 20
    scan_loop_pause_sec: float = 1.0
    service_heartbeat_ttl_sec: int = 90
    retention_hours: int = 24

    telegram_enabled: bool = False
    telegram_bot_token: str = ""
    telegram_chat_id: str = ""
    telegram_poll_timeout_sec: int = 25
    telegram_retry_base_sec: int = 15
    telegram_retry_max_sec: int = 300
    telegram_send_snapshots: bool = True

    audio_clip_max_sec: int = Field(default=60, le=60)
    audio_pre_roll_sec: int = 5
    audio_post_roll_sec: int = 10
    selftest_audio_enabled: bool = True
    selftest_audio_freq_hz: int = 1000
    selftest_audio_sec: int = 8
    wb_deploy_host: str = ""
    wb_deploy_user: str = "root"
    wb_deploy_path: str = "/opt/wb-sdr-air-grabberer"

    model_config = SettingsConfigDict(env_file=".env", extra="ignore")


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()
