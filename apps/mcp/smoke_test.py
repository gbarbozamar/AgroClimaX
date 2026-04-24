"""Smoke test: llama los 7 endpoints MCP via HTTP directo.

Uso: python apps/mcp/smoke_test.py
Requiere: backend corriendo en BACKEND_URL con MCP_SERVICE_TOKEN seteado.
"""
import os
import sys
import httpx

BACKEND = os.environ.get("AGROCLIMAX_BACKEND_URL", "http://127.0.0.1:8001")
TOKEN = os.environ.get("MCP_SERVICE_TOKEN", "dev-test-token")
FIELD_ID = os.environ.get("FIELD_ID", "fe51f286-f384-49c5-b0dc-2c9e59ae6319")

# Campo Nort test fixture
PADDOCK_ID = os.environ.get("PADDOCK_ID", "a7c7d499-6dbe-408d-b6b3-4cbdfddb0ac8")
EST_ID = os.environ.get("ESTABLISHMENT_ID", "153cbad7-3051-4678-9ef9-f92a3dcdac55")

h = {"X-Service-Token": TOKEN, "X-User-Id": "test-user"}

def _hit(label, method, path, **kw):
    url = f"{BACKEND}{path}"
    try:
        r = httpx.request(method, url, headers=h, timeout=15, **kw)
        body_snippet = r.text[:160].replace("\n", " ")
        status_icon = "OK" if r.status_code < 400 else "FAIL"
        print(f"{status_icon} {label:30} {method} {path[:55]:55} {r.status_code}  {body_snippet}")
        return r
    except Exception as e:
        print(f"FAIL {label:30} {method} {path[:55]:55} ERROR {e}")
        return None

def main():
    print(f"Backend: {BACKEND}  Token: {'set' if TOKEN else 'MISSING'}")
    print("=" * 100)
    _hit("1 field snapshot",     "GET",  f"/api/v1/mcp/fields/{FIELD_ID}/snapshot?layer=ndvi")
    _hit("2 field timeline",     "GET",  f"/api/v1/mcp/fields/{FIELD_ID}/timeline?layer=ndvi&days=14")
    _hit("3 field video",        "POST", f"/api/v1/mcp/fields/{FIELD_ID}/video", json={"layer_key":"ndvi","duration_days":14})
    _hit("4 fields by-alert",    "GET",  "/api/v1/mcp/fields/by-alert?min_level=0")
    _hit("5 paddock metrics",    "GET",  f"/api/v1/mcp/paddocks/{PADDOCK_ID}/metrics?date_range_days=14")
    _hit("6 estab summary",      "GET",  f"/api/v1/mcp/establishments/{EST_ID}/summary")
    _hit("7 crop prediction",    "GET",  f"/api/v1/mcp/fields/{FIELD_ID}/crop-prediction?horizon_days=30")
    print("=" * 100)
    print("Tests: token invalido y sin token (deben fallar 401/422):")
    try:
        r = httpx.get(f"{BACKEND}/api/v1/mcp/fields/by-alert", timeout=10)
        print(f"  sin token      -> {r.status_code} (esperado 422)")
    except Exception as e:
        print(f"  sin token      -> ERROR {e}")
    try:
        r = httpx.get(f"{BACKEND}/api/v1/mcp/fields/by-alert", headers={"X-Service-Token": "bad"}, timeout=10)
        print(f"  token invalido -> {r.status_code} (esperado 401)")
    except Exception as e:
        print(f"  token invalido -> ERROR {e}")

if __name__ == "__main__":
    main()
