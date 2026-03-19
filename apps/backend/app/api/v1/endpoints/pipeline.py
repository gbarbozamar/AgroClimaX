"""
Endpoints API — Control del pipeline Copernicus.
Permite ejecutar el pipeline manualmente y consultar su estado.
"""
from datetime import date
from typing import Optional

from fastapi import APIRouter, BackgroundTasks, Query, HTTPException

from app.copernicus.pipeline import ejecutar_pipeline

router = APIRouter(prefix="/pipeline", tags=["pipeline"])


@router.post("/ejecutar")
async def ejecutar_pipeline_manual(
    background_tasks: BackgroundTasks,
    fecha: Optional[date] = Query(None, description="Fecha objetivo (por defecto: ayer)"),
    ventana_dias: int = Query(6, description="Ventana de búsqueda de imágenes en días"),
):
    """
    Ejecuta el pipeline Copernicus manualmente en background.
    Descarga S1 + S2, calcula humedad y NDMI, evalúa alertas.
    """
    background_tasks.add_task(ejecutar_pipeline, fecha, ventana_dias)
    return {
        "mensaje": "Pipeline iniciado en background.",
        "fecha_objetivo": str(fecha or "ayer"),
        "ventana_dias": ventana_dias,
    }


@router.get("/estado")
async def estado_pipeline():
    """Estado del pipeline y última ejecución."""
    # En producción: consultar tabla de ejecuciones o Redis
    return {
        "estado": "disponible",
        "descripcion": "Pipeline listo para ejecución manual o automática (cron).",
        "fuentes": ["Sentinel-1 GRD", "Sentinel-2 L2A", "ERA5 CDS"],
        "area": "Rivera, Uruguay",
        "evalscripts": ["ndmi_s2_filtrado.js", "humedad_suelo_s1.js"],
    }
