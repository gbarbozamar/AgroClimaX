from pathlib import Path

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


BASE_DIR = Path(__file__).resolve().parents[2]


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=BASE_DIR / ".env",
        case_sensitive=False,
        extra="ignore",
    )

    # App
    app_env: str = "development"
    secret_key: str = "change_in_production"
    api_prefix: str = "/api/v1"
    legacy_api_prefix: str = "/api"
    default_timezone: str = "America/Montevideo"

    # Base de datos
    database_url: str = f"sqlite+aiosqlite:///{(BASE_DIR / 'agroclimax.db').as_posix()}"
    database_sync_url: str = f"sqlite:///{(BASE_DIR / 'agroclimax.db').as_posix()}"

    # Redis / workers
    redis_url: str = "redis://localhost:6379/0"
    pipeline_cron_hour: int = 3
    pipeline_cron_minute: int = 30
    recalibration_weekday: str = "monday"

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


settings = Settings()
