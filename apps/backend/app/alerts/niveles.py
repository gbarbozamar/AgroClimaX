"""
Niveles de alerta hídrica AgroClimaX.

VERDE     → humedad > 50%  |  NDMI > 0.10   → Normal
AMARILLO  → 25–50%         |  NDMI 0–0.10   → Vigilancia
NARANJA   → 15–25%         |  NDMI -0.10–0  → Alerta
ROJO      → < 15%          |  NDMI < -0.10  → Emergencia
"""
from enum import IntEnum
from dataclasses import dataclass


class NivelAlerta(IntEnum):
    VERDE = 0
    AMARILLO = 1
    NARANJA = 2
    ROJO = 3


@dataclass(frozen=True)
class DefinicionNivel:
    nivel: NivelAlerta
    nombre: str
    color_hex: str
    humedad_min: float   # % humedad suelo S1
    humedad_max: float
    ndmi_min: float
    ndmi_max: float
    descripcion: str
    accion: str


NIVELES: dict[NivelAlerta, DefinicionNivel] = {
    NivelAlerta.VERDE: DefinicionNivel(
        nivel=NivelAlerta.VERDE,
        nombre="Normal",
        color_hex="#2ecc71",
        humedad_min=50.0,
        humedad_max=100.0,
        ndmi_min=0.10,
        ndmi_max=1.0,
        descripcion="Condiciones hídricas normales. Sin déficit.",
        accion="Monitoreo rutinario.",
    ),
    NivelAlerta.AMARILLO: DefinicionNivel(
        nivel=NivelAlerta.AMARILLO,
        nombre="Vigilancia",
        color_hex="#f1c40f",
        humedad_min=25.0,
        humedad_max=50.0,
        ndmi_min=0.0,
        ndmi_max=0.10,
        descripcion="Inicio de déficit hídrico. Monitoreo reforzado.",
        accion="Verificar fuentes de agua. Evaluar riego suplementario.",
    ),
    NivelAlerta.NARANJA: DefinicionNivel(
        nivel=NivelAlerta.NARANJA,
        nombre="Alerta",
        color_hex="#e67e22",
        humedad_min=15.0,
        humedad_max=25.0,
        ndmi_min=-0.10,
        ndmi_max=0.0,
        descripcion="Déficit hídrico moderado. Estrés en cultivos y pasturas.",
        accion="Activar protocolos de emergencia agropecuaria.",
    ),
    NivelAlerta.ROJO: DefinicionNivel(
        nivel=NivelAlerta.ROJO,
        nombre="Emergencia",
        color_hex="#e74c3c",
        humedad_min=0.0,
        humedad_max=15.0,
        ndmi_min=-1.0,
        ndmi_max=-0.10,
        descripcion="Emergencia hídrica severa. Riesgo crítico para ganadería y agricultura.",
        accion="Notificar MGAP. Activar declaración de emergencia agropecuaria.",
    ),
}


def clasificar_por_humedad(humedad_pct: float) -> NivelAlerta:
    """Clasifica nivel de alerta según % humedad superficial (Sentinel-1)."""
    if humedad_pct >= 50.0:
        return NivelAlerta.VERDE
    if humedad_pct >= 25.0:
        return NivelAlerta.AMARILLO
    if humedad_pct >= 15.0:
        return NivelAlerta.NARANJA
    return NivelAlerta.ROJO


def clasificar_por_ndmi(ndmi: float) -> NivelAlerta:
    """Clasifica nivel de alerta según NDMI (Sentinel-2)."""
    if ndmi >= 0.10:
        return NivelAlerta.VERDE
    if ndmi >= 0.0:
        return NivelAlerta.AMARILLO
    if ndmi >= -0.10:
        return NivelAlerta.NARANJA
    return NivelAlerta.ROJO


def clasificar_combinado(humedad_pct: float | None, ndmi: float | None) -> NivelAlerta:
    """
    Clasificación combinada S1 + S2 (más robusta).
    Toma el nivel más severo de ambos indicadores disponibles.
    """
    niveles_calculados = []

    if humedad_pct is not None and not (humedad_pct != humedad_pct):  # no NaN
        niveles_calculados.append(clasificar_por_humedad(humedad_pct))

    if ndmi is not None and not (ndmi != ndmi):  # no NaN
        niveles_calculados.append(clasificar_por_ndmi(ndmi))

    if not niveles_calculados:
        return NivelAlerta.VERDE  # sin datos → no alarmar

    return max(niveles_calculados)
