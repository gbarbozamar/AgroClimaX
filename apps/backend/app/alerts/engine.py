"""
Motor de alertas AgroClimaX.
Evalúa condiciones hídricas y genera eventos de alerta.
"""
import logging
from datetime import date, timedelta
from dataclasses import dataclass, field

import numpy as np

from app.alerts.niveles import (
    NivelAlerta,
    clasificar_combinado,
    clasificar_por_humedad,
    clasificar_por_ndmi,
    NIVELES,
)
from app.copernicus.era5 import clasificar_spi

logger = logging.getLogger(__name__)

# Días consecutivos en alerta para activar "alerta prolongada"
DIAS_CONSECUTIVOS_UMBRAL = 5


@dataclass
class EventoAlerta:
    fecha: date
    nivel: NivelAlerta
    tipo: str                  # "hídrico_combinado", "solo_s1", "solo_s2", "spi"
    humedad_media: float | None = None
    ndmi_medio: float | None = None
    spi_valor: float | None = None
    spi_categoria: str | None = None
    pct_area_afectada: float = 0.0  # % del área Rivera en alerta
    es_prolongada: bool = False
    descripcion: str = ""
    accion_recomendada: str = ""

    def a_dict(self) -> dict:
        return {
            "fecha": str(self.fecha),
            "nivel": self.nivel.name,
            "nivel_codigo": int(self.nivel),
            "color": NIVELES[self.nivel].color_hex,
            "tipo": self.tipo,
            "humedad_media_pct": self.humedad_media,
            "ndmi_medio": self.ndmi_medio,
            "spi": self.spi_valor,
            "spi_categoria": self.spi_categoria,
            "pct_area_afectada": round(self.pct_area_afectada, 1),
            "es_prolongada": self.es_prolongada,
            "descripcion": self.descripcion,
            "accion_recomendada": self.accion_recomendada,
        }


class AlertaEngine:
    """
    Motor principal de evaluación de alertas hídricas.

    Lógica:
    1. Calcula nivel por pixel (S1 + S2 combinado)
    2. Agrega a nivel departamental (% área afectada)
    3. Detecta eventos prolongados (N días consecutivos)
    4. Integra SPI de ERA5 para confirmar déficit pluviométrico
    """

    def evaluar(
        self,
        fecha: date,
        humedad_s1: np.ndarray | None = None,
        ndmi_s2: np.ndarray | None = None,
        spi_30d: float | None = None,
        historial_niveles: list[NivelAlerta] | None = None,
    ) -> EventoAlerta:
        """
        Genera evento de alerta para la fecha dada.

        Args:
            fecha: Fecha de evaluación
            humedad_s1: Array 2D % humedad (Sentinel-1)
            ndmi_s2: Array 2D NDMI (Sentinel-2)
            spi_30d: Valor SPI de 30 días (ERA5)
            historial_niveles: Lista de niveles de los N días previos

        Returns:
            EventoAlerta con nivel, estadísticas y recomendaciones
        """
        humedad_media = None
        ndmi_medio = None

        if humedad_s1 is not None:
            validos = humedad_s1[~np.isnan(humedad_s1)]
            humedad_media = float(np.mean(validos)) if len(validos) > 0 else None

        if ndmi_s2 is not None:
            validos_ndmi = ndmi_s2[~np.isnan(ndmi_s2)]
            ndmi_medio = float(np.mean(validos_ndmi)) if len(validos_ndmi) > 0 else None

        # Nivel combinado
        nivel = clasificar_combinado(humedad_media, ndmi_medio)

        # Reforzar con SPI si hay déficit pluviométrico severo
        if spi_30d is not None and spi_30d < -1.5 and nivel < NivelAlerta.NARANJA:
            nivel = NivelAlerta.NARANJA
            logger.info("Nivel reforzado a NARANJA por SPI=%.2f", spi_30d)

        # Detectar alerta prolongada
        es_prolongada = False
        if historial_niveles and len(historial_niveles) >= DIAS_CONSECUTIVOS_UMBRAL:
            ultimos = historial_niveles[-DIAS_CONSECUTIVOS_UMBRAL:]
            if all(n >= NivelAlerta.AMARILLO for n in ultimos):
                es_prolongada = True
                nivel = max(nivel, NivelAlerta.NARANJA)
                logger.warning(
                    "ALERTA PROLONGADA detectada: %d días consecutivos en déficit",
                    DIAS_CONSECUTIVOS_UMBRAL,
                )

        # Calcular % del área afectada (píxeles en alerta ≥ AMARILLO)
        pct_afectada = self._calcular_pct_afectada(humedad_s1, ndmi_s2)

        # Determinar tipo de alerta
        if humedad_s1 is not None and ndmi_s2 is not None:
            tipo = "hidrico_combinado"
        elif humedad_s1 is not None:
            tipo = "solo_s1_radar"
        elif ndmi_s2 is not None:
            tipo = "solo_s2_optico"
        else:
            tipo = "solo_spi_era5"

        definicion = NIVELES[nivel]

        evento = EventoAlerta(
            fecha=fecha,
            nivel=nivel,
            tipo=tipo,
            humedad_media=round(humedad_media, 2) if humedad_media else None,
            ndmi_medio=round(ndmi_medio, 4) if ndmi_medio else None,
            spi_valor=round(spi_30d, 3) if spi_30d else None,
            spi_categoria=clasificar_spi(spi_30d) if spi_30d else None,
            pct_area_afectada=pct_afectada,
            es_prolongada=es_prolongada,
            descripcion=definicion.descripcion,
            accion_recomendada=definicion.accion,
        )

        if nivel >= NivelAlerta.NARANJA:
            logger.warning("ALERTA %s — %s", nivel.name, evento.descripcion)

        return evento

    def _calcular_pct_afectada(
        self,
        humedad_s1: np.ndarray | None,
        ndmi_s2: np.ndarray | None,
    ) -> float:
        """% del área donde el nivel es ≥ AMARILLO."""
        if humedad_s1 is None and ndmi_s2 is None:
            return 0.0

        # Usar S1 si disponible, sino S2
        arr = humedad_s1 if humedad_s1 is not None else None

        if arr is not None:
            validos = arr[~np.isnan(arr)]
            if len(validos) == 0:
                return 0.0
            en_alerta = np.sum(validos < 50.0)  # < 50% humedad = ≥ AMARILLO
            return 100.0 * float(en_alerta) / len(validos)

        # Solo NDMI disponible
        validos_n = ndmi_s2[~np.isnan(ndmi_s2)]
        if len(validos_n) == 0:
            return 0.0
        en_alerta_n = np.sum(validos_n < 0.10)
        return 100.0 * float(en_alerta_n) / len(validos_n)
