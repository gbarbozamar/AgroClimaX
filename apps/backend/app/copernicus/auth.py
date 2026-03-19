"""
Autenticación con Copernicus Data Space Ecosystem (CDSE)
y Sentinel Hub.
"""
import httpx
from app.core.config import settings


CDSE_TOKEN_URL = "https://identity.dataspace.copernicus.eu/auth/realms/CDSE/protocol/openid-connect/token"


async def get_cdse_token() -> str:
    """Obtiene token OAuth2 de Copernicus Data Space Ecosystem."""
    async with httpx.AsyncClient() as client:
        response = await client.post(
            CDSE_TOKEN_URL,
            data={
                "grant_type": "client_credentials",
                "client_id": settings.copernicus_client_id,
                "client_secret": settings.copernicus_client_secret,
            },
        )
        response.raise_for_status()
        return response.json()["access_token"]


def get_sentinelhub_config():
    """Retorna configuración de Sentinel Hub para sentinelhub-py."""
    from sentinelhub import SHConfig
    config = SHConfig()
    config.sh_client_id = settings.sentinelhub_client_id
    config.sh_client_secret = settings.sentinelhub_client_secret
    config.instance_id = settings.sentinelhub_instance_id
    return config
