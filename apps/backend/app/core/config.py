from pydantic_settings import BaseSettings
from pydantic import Field


class Settings(BaseSettings):
    # App
    app_env: str = "development"
    secret_key: str = "change_in_production"
    api_prefix: str = "/api/v1"

    # Base de datos
    database_url: str
    database_sync_url: str

    # Redis
    redis_url: str = "redis://localhost:6379/0"

    # Copernicus Data Space
    copernicus_client_id: str
    copernicus_client_secret: str

    # Sentinel Hub
    sentinelhub_client_id: str
    sentinelhub_client_secret: str
    sentinelhub_instance_id: str

    # CDS ERA5
    cds_api_key: str
    cds_api_url: str = "https://cds.climate.copernicus.eu/api/v2"

    # AOI Rivera
    aoi_department: str = "Rivera"
    aoi_bbox_west: float = -57.5
    aoi_bbox_south: float = -32.0
    aoi_bbox_east: float = -53.5
    aoi_bbox_north: float = -30.0

    # Pipeline
    s1_collection: str = "sentinel-1-grd"
    s2_collection: str = "sentinel-2-l2a"
    pipeline_cron_hour: int = 3
    pipeline_cron_minute: int = 0

    # Umbrales alerta (% humedad)
    alert_verde_min: float = 50.0
    alert_amarillo_min: float = 25.0
    alert_naranja_min: float = 15.0

    # Google OAuth
    google_client_id: str = ""
    google_client_secret: str = ""

    # JWT
    jwt_secret_key: str = "change_in_production"
    jwt_algorithm: str = "HS256"
    jwt_expire_minutes: int = 1440  # 24 hours

    # Frontend (optional override; defaults to request origin)
    frontend_url: str = ""

    # Notificaciones
    smtp_host: str = "smtp.gmail.com"
    smtp_port: int = 587
    smtp_user: str = ""
    smtp_password: str = ""
    alert_from_email: str = "alerts@agroclimax.uy"

    class Config:
        env_file = ".env"
        case_sensitive = False


settings = Settings()
