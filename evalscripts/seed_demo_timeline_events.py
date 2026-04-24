"""
Seed script de desarrollo: inserta ~15 AlertaEvento + NotificationEvent demo
distribuidos en los ultimos 45 dias, sobre departamentos variados, para que
el timeline del frontend tenga algo que mostrar durante dev local.

Uso (desde apps/backend con PYTHONPATH apuntando a la app):
    cd apps/backend
    python -c "import sys; sys.path.insert(0,'.'); exec(open('../../evalscripts/seed_demo_timeline_events.py').read())"

O con el runner estandar:
    python evalscripts/seed_demo_timeline_events.py

Deshacer: DELETE FROM alertas_eventos WHERE tipo='demo_seed';
         DELETE FROM notification_events WHERE reason='demo_seed';

NO va en produccion: los eventos sembrados llevan tipo='demo_seed' para que
puedas purgarlos facilmente y no contaminar analisis reales.
"""
from __future__ import annotations

import os
import sys
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path
from random import Random


def _bootstrap_path() -> Path:
    backend_root = Path(__file__).resolve().parents[1] / "apps" / "backend"
    if str(backend_root) not in sys.path:
        sys.path.insert(0, str(backend_root))
    return backend_root


_bootstrap_path()

from sqlalchemy import create_engine, select  # noqa: E402
from sqlalchemy.orm import Session  # noqa: E402

from app.core.config import settings  # noqa: E402
from app.models import AlertaEvento, NotificationEvent  # noqa: E402
from app.models.humedad import AOIUnit  # noqa: E402


SEVERITIES = [
    (1, "Vigilancia"),
    (2, "Alerta"),
    (3, "Emergencia"),
    (1, "Vigilancia"),
    (2, "Alerta"),
]

DEPARTMENTS = ["Rivera", "Salto", "Artigas", "Tacuarembo"]


def _sync_url() -> str:
    # Settings por defecto tiene sqlite+aiosqlite, necesitamos sync.
    raw = settings.database_sync_url or settings.database_url
    if raw.startswith("sqlite+aiosqlite:"):
        raw = "sqlite:" + raw[len("sqlite+aiosqlite:"):]
    if raw.startswith("postgresql+asyncpg://"):
        raw = "postgresql://" + raw[len("postgresql+asyncpg://"):]
    return raw


def seed(count: int = 15) -> None:
    engine = create_engine(_sync_url(), future=True)
    rng = Random(42)
    now = datetime.now(timezone.utc)
    created_alertas = 0
    created_notifs = 0
    with Session(engine) as session:
        # Buscar unidades por departamento
        units_by_dept: dict[str, list[AOIUnit]] = {}
        for dept in DEPARTMENTS:
            rows = session.execute(
                select(AOIUnit).where(AOIUnit.department == dept).limit(20)
            ).scalars().all()
            if rows:
                units_by_dept[dept] = list(rows)
        if not units_by_dept:
            print("[seed] No AOIUnit rows found — run the pipeline first.")
            return
        depts = list(units_by_dept.keys())
        for _ in range(count):
            dept = rng.choice(depts)
            unit = rng.choice(units_by_dept[dept])
            nivel, nivel_nombre = rng.choice(SEVERITIES)
            days_ago = rng.randint(1, 45)
            fecha = now - timedelta(days=days_ago, hours=rng.randint(0, 23))
            alerta = AlertaEvento(
                id=str(uuid.uuid4()),
                unit_id=unit.id,
                fecha=fecha,
                departamento=dept,
                scope="departamento",
                nivel=nivel,
                nivel_nombre=nivel_nombre,
                tipo="demo_seed",
                humedad_media_pct=round(rng.uniform(25, 75), 1),
                ndmi_medio=round(rng.uniform(-0.3, 0.5), 3),
                spi_valor=round(rng.uniform(-2.2, 1.5), 2),
                spi_categoria=rng.choice(["seco_severo", "seco", "normal"]),
                pct_area_afectada=round(rng.uniform(5, 90), 1),
                largest_cluster_pct=round(rng.uniform(0, 60), 1),
                risk_score=round(rng.uniform(25, 95), 1),
                confidence_score=round(rng.uniform(55, 92), 1),
                days_in_state=rng.randint(1, 12),
                actionable=nivel >= 2,
                drivers=[{"name": "humedad_suelo", "score": 0.78}],
                forecast=[{"expected_risk": round(rng.uniform(30, 85), 1)}],
                soil_context={"texture": "franco_arenosa"},
                calibration_ref="calib_demo_seed",
                descripcion=f"Evento demo {nivel_nombre} en {dept}",
                accion_recomendada="Revisar dashboard y contactar productores",
            )
            session.add(alerta)
            session.flush()
            created_alertas += 1
            notif = NotificationEvent(
                id=str(uuid.uuid4()),
                alert_event_id=alerta.id,
                channel=rng.choice(["email", "whatsapp", "sms"]),
                recipient=f"demo-user@{dept.lower()}.uy",
                status="delivered",
                reason="demo_seed",
                payload={"title": f"Alerta {nivel_nombre} - {dept}", "body": alerta.descripcion},
                delivered_at=fecha + timedelta(minutes=5),
                created_at=fecha,
            )
            session.add(notif)
            created_notifs += 1
        session.commit()
    print(f"[seed] Inserted {created_alertas} alertas_eventos + {created_notifs} notification_events (tipo='demo_seed').")


if __name__ == "__main__":
    try:
        total = int(os.environ.get("SEED_COUNT", "15"))
    except ValueError:
        total = 15
    seed(total)
