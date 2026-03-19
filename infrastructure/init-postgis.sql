-- Habilitar extensiones necesarias para AgroClimaX
CREATE EXTENSION IF NOT EXISTS postgis;
CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;

-- Tabla de humedad como hypertable (TimescaleDB)
-- Se ejecuta después de que Alembic crea la tabla
-- Ver scripts/setup_timescaledb.py
