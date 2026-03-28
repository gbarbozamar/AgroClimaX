# Runbook de Despliegue
## AgroClimaX en Railway + PostGIS Gestionado

**Fecha:** 2026-03-28  
**Objetivo:** dejar AgroClimaX corriendo con runtime en Railway, base espacial gestionada con PostGIS real, cache/colas en Railway y objetos pesados en bucket S3-compatible.  
**Estado del repo:** preparado para separar runtime `web` y `worker`, soportar bucket opcional y seguir operando en modo actual si no se configuran los nuevos servicios.

## 1. Arquitectura objetivo

- `AgroClimaX Web` en Railway
- `AgroClimaX Worker` en Railway
- `Railway Redis`
- `Railway Bucket`
- `PostgreSQL + PostGIS` gestionado fuera del Postgres default simple
- fuentes externas mantenidas en esta fase:
  - Copernicus / Sentinel
  - Open-Meteo
  - MGAP CONEAT WMS

## 2. Servicios a provisionar

### 2.1 Railway
- 1 servicio web: `AgroClimaX`
- 1 servicio worker: `AgroClimaX Worker`
- 1 servicio `Redis`
- 1 `Bucket` privado S3-compatible

### 2.2 Base de datos espacial
- Recomendado: `Crunchy Bridge` con `PostGIS`
- Alternativa aceptable: cualquier PostgreSQL gestionado que habilite `CREATE EXTENSION postgis`

### 2.3 Notificaciones
- `Postmark` para SMTP transaccional
- `Twilio` para SMS / WhatsApp

## 3. Variables de entorno requeridas

### 3.1 Comunes a Web y Worker
- `APP_ENV=production`
- `SECRET_KEY=<valor seguro>`
- `DATABASE_URL=<url async de Postgres/PostGIS>`
- `DATABASE_SYNC_URL=<url sync de Postgres/PostGIS>`
- `DATABASE_USE_POSTGIS=true`
- `REDIS_URL=<redis railway>`
- `PIPELINE_STALE_AFTER_HOURS=6`
- `PIPELINE_BOOTSTRAP_BACKFILL_DAYS=7`
- `PIPELINE_STARTUP_WARMUP_ENABLED=true`
- `CONEAT_PREWARM_ENABLED=true`

### 3.2 Web
- `APP_RUNTIME_ROLE=web`
- `PIPELINE_SCHEDULER_ENABLED=false`

### 3.3 Worker
- `APP_RUNTIME_ROLE=worker`
- `PIPELINE_SCHEDULER_ENABLED=true`

### 3.4 Bucket S3-compatible
- `STORAGE_BACKEND=s3`
- `STORAGE_S3_ENDPOINT_URL=<endpoint del bucket>`
- `STORAGE_S3_REGION=<region>`
- `STORAGE_S3_BUCKET=<bucket>`
- `STORAGE_S3_ACCESS_KEY_ID=<access key>`
- `STORAGE_S3_SECRET_ACCESS_KEY=<secret key>`
- `STORAGE_S3_PREFIX=agroclimax`

### 3.5 Ingesta externa
- `COPERNICUS_CLIENT_ID`
- `COPERNICUS_CLIENT_SECRET`
- `SENTINELHUB_CLIENT_ID`
- `SENTINELHUB_CLIENT_SECRET`
- `SENTINELHUB_INSTANCE_ID`
- `CDS_API_KEY`
- `CDS_API_URL=https://cds.climate.copernicus.eu/api/v2`

### 3.6 Notificaciones
- `SMTP_HOST`
- `SMTP_PORT`
- `SMTP_USER`
- `SMTP_PASSWORD`
- `ALERT_FROM_EMAIL`
- `TWILIO_ACCOUNT_SID`
- `TWILIO_AUTH_TOKEN`
- `TWILIO_SMS_FROM`
- `TWILIO_WHATSAPP_FROM`

## 4. Preparacion de la base PostGIS

1. Crear la base `agroclimax` en el proveedor PostGIS elegido.
2. Crear un usuario de aplicacion con permisos de lectura y escritura.
3. Ejecutar:

```sql
CREATE EXTENSION IF NOT EXISTS postgis;
```

4. Verificar:

```sql
SELECT PostGIS_Version();
```

5. Cargar `DATABASE_URL` y `DATABASE_SYNC_URL` en Railway.

## 5. Provision de Railway

### 5.1 Servicio Web
- usar el Dockerfile actual: [Dockerfile](C:/Users/barbo/Documents/PhD/AI%20Deep%20Economics/AgroClimaX/apps/backend/Dockerfile)
- comando por defecto:

```bash
uvicorn app.main:app --host 0.0.0.0 --port ${PORT:-8000}
```

### 5.2 Servicio Worker
- usar la misma imagen del backend
- comando:

```bash
python -m app.worker
```

### 5.3 Redis
- provisionar un servicio Redis en Railway
- pasar la `REDIS_URL` a Web y Worker

### 5.4 Bucket
- crear un `Bucket` privado en Railway
- copiar `endpoint`, `region`, `bucket`, `access key` y `secret`
- cargar esas variables en Web y Worker

## 6. Flujo de corte a produccion

1. Desplegar `Web` apuntando a PostGIS y bucket, con `APP_RUNTIME_ROLE=web`.
2. Desplegar `Worker` apuntando a la misma base y bucket, con `APP_RUNTIME_ROLE=worker`.
3. Confirmar que el `Worker` inicialice tablas y ejecute el warmup.
4. Verificar el estado:
   - `GET /api/health`
   - `GET /api/v1/pipeline/estado`
   - `GET /api/v1/settings`
   - `GET /api/v1/capas/departamentos`
5. Disparar manualmente una corrida:

```http
POST /api/v1/pipeline/ejecutar
POST /api/v1/pipeline/materializar
POST /api/v1/pipeline/prewarm-coneat
```

6. Confirmar que `latest_state_cache`, `unit_index_snapshots` y `spatial_layer_features` queden pobladas.
7. Recién después redirigir el uso productivo al nuevo despliegue.

## 7. Que ya soporta el repo

- `APP_RUNTIME_ROLE` para separar `web`, `worker` y `all-in-one`
- bucket S3-compatible opcional para tiles y cache externo
- `python -m app.worker` como runtime canonico del worker
- compose local actualizado con servicio `worker`
- fallback a filesystem si no se configura bucket
- compatibilidad con el modo actual si se mantiene `APP_RUNTIME_ROLE=all-in-one`

## 8. Validaciones minimas de salida

- `39 tests OK` desde `apps/backend`
- arranque `worker` con scheduler deshabilitado sin errores
- arranque `web` en modo `APP_RUNTIME_ROLE=web` con `GET /api/health -> 200`
- si el bucket esta habilitado:
  - tiles Copernicus se escriben en bucket
  - cache CONEAT se replica en bucket

## 9. Riesgos operativos a vigilar

- no ejecutar scheduler en `web` y `worker` a la vez
- no dejar la base productiva sin `postgis`
- no depender de filesystem efimero para cache importante si ya existe bucket
- no habilitar Twilio/Postmark sin probar antes `POST /api/v1/notificaciones/test`

## 10. Siguiente fase recomendada

Una vez estabilizado este despliegue:

1. migrar geometrias y consultas criticas a PostGIS real sin fallback JSON
2. mover caches persistentes pesados al bucket
3. consolidar observabilidad y alertas operativas
4. evaluar internalizacion progresiva de datasets externos
