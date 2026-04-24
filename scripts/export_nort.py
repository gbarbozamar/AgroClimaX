"""Extract Campo Nort + Estancia Norte + paddocks from local SQLite into JSON payload."""
import sqlite3, json, sys
con = sqlite3.connect('apps/backend/agroclimax.db')
cur = con.cursor()
FID = 'fe51f286-f384-49c5-b0dc-2c9e59ae6319'

fkeys = ['establishment_id','user_id','name','department','padron_value','padron_source',
         'padron_lookup_payload','padron_geometry_geojson','field_geometry_geojson',
         'centroid_lat','centroid_lon','area_ha','aoi_unit_id','active']
row = cur.execute(f"SELECT {','.join(fkeys)} FROM farm_fields WHERE id=?", (FID,)).fetchone()
field = dict(zip(fkeys, row))
field['id'] = FID

est_row = cur.execute(
    'SELECT id, user_id, name, description, active FROM farm_establishments WHERE id=?',
    (field['establishment_id'],)
).fetchone()
establishment = dict(zip(['id','user_id','name','description','active'], est_row))

pad_rows = cur.execute(
    'SELECT id, field_id, user_id, name, geometry_geojson, area_ha, aoi_unit_id, display_order, active '
    'FROM farm_paddocks WHERE field_id=?', (FID,)
).fetchall()
paddocks = [dict(zip(
    ['id','field_id','user_id','name','geometry_geojson','area_ha','aoi_unit_id','display_order','active'],
    r
)) for r in pad_rows]

payload = {
    'user_id': field['user_id'],
    'establishment': establishment,
    'field': field,
    'paddocks': paddocks,
}
out = sys.argv[1] if len(sys.argv) > 1 else 'scripts/nort_payload.json'
with open(out,'w',encoding='utf-8') as f:
    json.dump(payload, f, ensure_ascii=False)
est_short = establishment['id'][:8]
print(f"ok: establishment={est_short}... '{establishment['name']}', field='{field['name']}' ({field['area_ha']}ha), paddocks={len(paddocks)}")
print(f"wrote: {out} ({len(open(out,'rb').read())} bytes)")
