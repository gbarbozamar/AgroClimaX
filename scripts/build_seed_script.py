"""Build a standalone seed script that embeds the Nort payload inline."""
import json, sys, pathlib

PAYLOAD_FILE = pathlib.Path('scripts/nort_payload.json')
OUT = pathlib.Path('scripts/seed_nort_on_prod.py')

payload = json.loads(PAYLOAD_FILE.read_text(encoding='utf-8'))
# JSON fields in SQLite come through as strings — leave as-is so we pass them
# to Postgres as ::jsonb for parsing server-side.

SCRIPT = '''
"""Seed Campo Nort + Estancia Norte + 3 paddocks into Railway Postgres. Idempotent."""
import asyncio, json, os
from datetime import datetime, timezone
import asyncpg

PAYLOAD = __PAYLOAD__


def as_jsonb(v):
    if v is None: return None
    if isinstance(v, str): return v
    return json.dumps(v, ensure_ascii=False)


async def main():
    url = os.environ["DATABASE_URL"].replace("postgresql+asyncpg://", "postgresql://")
    p = PAYLOAD
    user_id = p["user_id"]
    est = p["establishment"]
    field = p["field"]
    paddocks = p["paddocks"]
    now = datetime.now(timezone.utc)

    conn = await asyncpg.connect(url)
    try:
        # 1. User
        r = await conn.execute(
            """
            INSERT INTO app_users (id, google_sub, email, email_verified, is_active, created_at, updated_at)
            VALUES ($1, $2, $3, false, true, $4, $4)
            ON CONFLICT (id) DO NOTHING
            """,
            user_id, f"agroclimax-seed:{user_id}", f"{user_id}@agroclimax.local", now,
        )
        print(f"user insert: {r}")

        # 2. Establishment
        r = await conn.execute(
            """
            INSERT INTO farm_establishments (id, user_id, name, description, active, created_at, updated_at)
            VALUES ($1, $2, $3, $4, true, $5, $5)
            ON CONFLICT (id) DO NOTHING
            """,
            est["id"], user_id, est["name"], est.get("description"), now,
        )
        print(f"establishment insert: {r}")

        # 3. Field
        r = await conn.execute(
            """
            INSERT INTO farm_fields (
              id, establishment_id, user_id, name, department, padron_value, padron_source,
              padron_lookup_payload, padron_geometry_geojson, field_geometry_geojson,
              centroid_lat, centroid_lon, area_ha, aoi_unit_id, active, created_at, updated_at
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8::jsonb, $9::jsonb, $10::jsonb,
                      $11, $12, $13, NULL, true, $14, $14)
            ON CONFLICT (id) DO NOTHING
            """,
            field["id"], field["establishment_id"], user_id, field["name"], field["department"],
            field["padron_value"], field["padron_source"],
            as_jsonb(field.get("padron_lookup_payload")),
            as_jsonb(field.get("padron_geometry_geojson")),
            as_jsonb(field["field_geometry_geojson"]),
            field.get("centroid_lat"), field.get("centroid_lon"), field.get("area_ha"), now,
        )
        print(f"field insert: {r}")

        # 4. Paddocks
        for pad in paddocks:
            r = await conn.execute(
                """
                INSERT INTO farm_paddocks (
                  id, field_id, user_id, name, geometry_geojson, area_ha,
                  aoi_unit_id, display_order, active, created_at, updated_at
                ) VALUES ($1, $2, $3, $4, $5::jsonb, $6, NULL, $7, true, $8, $8)
                ON CONFLICT (id) DO NOTHING
                """,
                pad["id"], pad["field_id"], user_id, pad["name"],
                as_jsonb(pad["geometry_geojson"]), pad.get("area_ha"),
                pad.get("display_order", 0), now,
            )
            print(f"paddock {pad['name']!r} insert: {r}")

        # Verify
        c_u = await conn.fetchval("SELECT COUNT(*) FROM app_users WHERE id=$1", user_id)
        c_e = await conn.fetchval("SELECT COUNT(*) FROM farm_establishments WHERE id=$1", est["id"])
        c_f = await conn.fetchval("SELECT COUNT(*) FROM farm_fields WHERE id=$1", field["id"])
        c_p = await conn.fetchval("SELECT COUNT(*) FROM farm_paddocks WHERE field_id=$1", field["id"])
        print(f"VERIFY -> users={c_u} establishments={c_e} fields={c_f} paddocks={c_p}")
        print(f"FIELD_ID={field['id']}")
        print(f"USER_ID={user_id}")
    finally:
        await conn.close()


asyncio.run(main())
'''

out = SCRIPT.replace('__PAYLOAD__', repr(payload))
OUT.write_text(out, encoding='utf-8')
print(f"wrote: {OUT} ({len(out)} bytes)")
