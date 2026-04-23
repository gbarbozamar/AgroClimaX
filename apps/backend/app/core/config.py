from pathlib import Path

from pydantic import Field, field_validator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


BASE_DIR = Path(__file__).resolve().parents[2]


def _normalize_postgres_scheme(value: str, *, async_driver: bool) -> str:
    normalized = value.strip()
    if normalized.startswith("postgres://"):
        normalized = "postgresql://" + normalized[len("postgres://"):]
    if async_driver and normalized.startswith("postgresql://"):
        normalized = "postgresql+asyncpg://" + normalized[len("postgresql://"):]
    if not async_driver and normalized.startswith("postgresql+asyncpg://"):
        normalized = "postgresql://" + normalized[len("postgresql+asyncpg://"):]
    return normalized


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=BASE_DIR / ".env",
        case_sensitive=False,
        extra="ignore",
    )

    # App
    app_env: str = "development"
    app_runtime_role: str = "all-in-one"
    secret_key: str = "change_in_production"
    api_prefix: str = "/api/v1"
    legacy_api_prefix: str = "/api"
    default_timezone: str = "America/Montevideo"

    # Base de datos
    database_url: str = f"sqlite+aiosqlite:///{(BASE_DIR / 'agroclimax.db').as_posix()}"
    database_sync_url: str | None = None
    database_use_postgis: bool = True

    # Auth / Google OAuth
    google_client_id: str = ""
    google_client_secret: str = ""
    google_discovery_url: str = "https://accounts.google.com/.well-known/openid-configuration"
    google_redirect_uri: str = ""
    auth_cookie_name: str = "agroclimax_session"
    auth_session_ttl_hours: int = 72
    auth_state_ttl_minutes: int = 10
    auth_csrf_header_name: str = "X-CSRF-Token"
    auth_login_success_redirect: str = "/"
    auth_bypass_for_tests: bool = False
    public_app_base_url: str = ""

    # Redis / workers
    redis_url: str = "redis://localhost:6379/0"
    pipeline_cron_hour: int = 3
    pipeline_cron_minute: int = 30
    recalibration_weekday: str = "monday"
    pipeline_scheduler_enabled: bool = False
    pipeline_scheduler_poll_seconds: int = 300
    pipeline_bootstrap_backfill_days: int = 7
    pipeline_stale_after_hours: int = 6
    pipeline_startup_warmup_enabled: bool = True
    timeline_historical_window_days: int = 365
    coneat_cache_ttl_hours: int = 168
    coneat_prewarm_enabled: bool = True
    coneat_prewarm_zoom_levels: list[int] = Field(default_factory=lambda: [6, 7, 8])
    preload_enabled: bool = True
    preload_neighbor_days: int = 1
    preload_adjacent_zoom_delta: int = 1
    preload_max_tiles_per_zoom: int = 48
    preload_run_ttl_hours: int = 24
    # Safety net: preload_runs stuck en running/queued más de X min se marcan failed.
    preload_stale_minutes: int = 15

    # Object storage / buckets
    storage_backend: str = "filesystem"
    storage_s3_endpoint_url: str = ""
    storage_s3_region: str = "us-east-1"
    storage_s3_bucket: str = ""
    storage_s3_access_key_id: str = ""
    storage_s3_secret_access_key: str = ""
    storage_s3_prefix: str = "agroclimax"

    # Copernicus / Sentinel Hub
    copernicus_client_id: str = ""
    copernicus_client_secret: str = ""
    sentinelhub_client_id: str = ""
    sentinelhub_client_secret: str = ""
    sentinelhub_instance_id: str = ""
    cds_api_key: str = ""
    cds_api_url: str = "https://cds.climate.copernicus.eu/api/v2"

    # Forecast
    openmeteo_base_url: str = "https://api.open-meteo.com/v1/forecast"
    department_boundaries_metadata_url: str = "https://www.geoboundaries.org/api/current/gbOpen/URY/ADM1/"
    department_boundaries_geojson_url: str = ""

    # AOI / escala
    aoi_department: str = "Rivera"
    aoi_bbox_west: float = -57.5
    aoi_bbox_south: float = -32.0
    aoi_bbox_east: float = -53.5
    aoi_bbox_north: float = -30.0
    default_hex_resolution: int = 9
    hex_display_resolution: int = 6
    default_scope: str = "departamento"
    calibration_window_days: int = 56
    calibration_min_samples: int = 8
    national_pipeline_live_workers: int = 3
    live_carry_forward_max_age_days: int = 7

    # Umbrales / pesos
    risk_weight_magnitude: float = 35.0
    risk_weight_persistence: float = 20.0
    risk_weight_anomaly: float = 15.0
    risk_weight_weather: float = 15.0
    risk_weight_soil: float = 15.0
    confidence_weight_freshness: float = 25.0
    confidence_weight_agreement: float = 25.0
    confidence_weight_applicability: float = 20.0
    confidence_weight_calibration: float = 20.0
    confidence_weight_ground_truth: float = 10.0

    # Notificaciones
    smtp_host: str = "smtp.gmail.com"
    smtp_port: int = 587
    smtp_user: str = ""
    smtp_password: str = ""
    alert_from_email: str = "alerts@agroclimax.uy"
    twilio_account_sid: str = ""
    twilio_auth_token: str = ""
    twilio_whatsapp_from: str = ""
    twilio_sms_from: str = ""
    notification_state_change_only: bool = True

    # Ground truth / sensores
    ground_truth_api_keys: list[str] = Field(default_factory=list)

    # Frontend / static
    frontend_mount_path: str = "/static"

    @field_validator("ground_truth_api_keys", mode="before")
    @classmethod
    def parse_ground_truth_keys(cls, value: object) -> list[str]:
        if value is None or value == "":
            return []
        if isinstance(value, list):
            return [str(item).strip() for item in value if str(item).strip()]
        return [part.strip() for part in str(value).split(",") if part.strip()]

    @field_validator("coneat_prewarm_zoom_levels", mode="before")
    @classmethod
    def parse_coneat_zoom_levels(cls, value: object) -> list[int]:
        if value is None or value == "":
            return [6, 7, 8]
        if isinstance(value, list):
            return [int(item) for item in value]
        return [int(part.strip()) for part in str(value).split(",") if part.strip()]

    @field_validator("database_url", mode="before")
    @classmethod
    def normalize_database_url(cls, value: object) -> str:
        if value is None or value == "":
            return f"sqlite+aiosqlite:///{(BASE_DIR / 'agroclimax.db').as_posix()}"
        return _normalize_postgres_scheme(str(value), async_driver=True)

    @field_validator("database_sync_url", mode="before")
    @classmethod
    def normalize_database_sync_url(cls, value: object) -> str:
        if value is None or value == "":
            return ""
        return _normalize_postgres_scheme(str(value), async_driver=False)

    @model_validator(mode="after")
    def finalize_database_urls(self) -> "Settings":
        if not self.database_sync_url:
            if self.database_url.startswith("postgresql+asyncpg://"):
                self.database_sync_url = _normalize_postgres_scheme(self.database_url, async_driver=False)
            else:
                self.database_sync_url = f"sqlite:///{(BASE_DIR / 'agroclimax.db').as_posix()}"
        return self

    @field_validator("app_runtime_role", mode="before")
    @classmethod
    def normalize_runtime_role(cls, value: object) -> str:
        normalized = str(value or "all-in-one").strip().lower()
        allowed = {"all-in-one", "web", "worker"}
        return normalized if normalized in allowed else "all-in-one"

    @field_validator("storage_backend", mode="before")
    @classmethod
    def normalize_storage_backend(cls, value: object) -> str:
        normalized = str(value or "filesystem").strip().lower()
        allowed = {"filesystem", "s3"}
        return normalized if normalized in allowed else "filesystem"

    @property
    def copernicus_enabled(self) -> bool:
        return bool(self.copernicus_client_id and self.copernicus_client_secret)

    @property
    def sentinelhub_enabled(self) -> bool:
        return bool(
            self.sentinelhub_client_id
            and self.sentinelhub_client_secret
            and self.sentinelhub_instance_id
        )

    @property
    def twilio_enabled(self) -> bool:
        return bool(self.twilio_account_sid and self.twilio_auth_token)

    @property
    def storage_bucket_enabled(self) -> bool:
        return (
            self.storage_backend == "s3"
            and bool(self.storage_s3_endpoint_url)
            and bool(self.storage_s3_bucket)
            and bool(self.storage_s3_access_key_id)
            and bool(self.storage_s3_secret_access_key)
        )

    @property
    def google_oauth_enabled(self) -> bool:
        return bool(self.google_client_id and self.google_client_secret)


settings = Settings()
