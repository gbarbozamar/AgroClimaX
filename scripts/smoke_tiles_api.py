"""
API Smoke: Timeline + Tiles Integrity
------------------------------------
Minimal, CI-friendly smoke to catch regressions where:
  - timeline declares a frame as playable/ready but tiles are actually empty
  - tiles endpoint returns transparent PNGs for frames marked as available

This does NOT require browser automation.
"""

from __future__ import annotations

import argparse
import sys
from dataclasses import dataclass
from datetime import date, timedelta
from io import BytesIO
from math import atan, cos, floor, log, pi, sinh
from typing import Any

import httpx
from PIL import Image


DEFAULT_BBOX_URUGUAY = "-59.8206,-35.1648,-52.1741,-30.3634"
DEFAULT_ZOOM = 7
DEFAULT_LAYERS = ["rgb", "alerta"]


def _tile_xy(lon: float, lat: float, z: int) -> tuple[int, int]:
    n = 2**int(z)
    x = int(floor((lon + 180.0) / 360.0 * n))
    lat_rad = lat * pi / 180.0
    y = int(floor((1.0 - log((1.0 + sinh(lat_rad)) / cos(lat_rad)) / pi) / 2.0 * n))
    return x, y


def _bbox_center(bbox: str) -> tuple[float, float]:
    parts = [p.strip() for p in (bbox or "").split(",")[:4]]
    if len(parts) != 4:
        raise ValueError(f"Invalid bbox: {bbox!r}")
    west, south, east, north = [float(p) for p in parts]
    return (west + east) / 2.0, (south + north) / 2.0


def _png_is_fully_transparent(png_bytes: bytes) -> bool:
    try:
        image = Image.open(BytesIO(png_bytes)).convert("RGBA")
    except Exception as exc:
        raise RuntimeError(f"Invalid PNG payload: {exc}") from exc
    alpha = image.getchannel("A")
    bbox = alpha.getbbox()
    return bbox is None


@dataclass(frozen=True)
class FramePick:
    layer_id: str
    display_date: str
    resolved_source_date: str | None
    frame_signature: str | None


def _pick_frame(payload: dict[str, Any], *, layers: list[str]) -> FramePick | None:
    days = payload.get("days") or []
    for day in reversed(days):
        layer_frames = (day or {}).get("layers") or {}
        for layer in layers:
            frame = layer_frames.get(layer) or {}
            if not frame.get("available"):
                continue
            if frame.get("visual_empty"):
                continue
            return FramePick(
                layer_id=layer,
                display_date=str(day.get("display_date")),
                resolved_source_date=frame.get("resolved_source_date"),
                frame_signature=frame.get("frame_signature"),
            )
    return None


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--base-url", default="http://127.0.0.1:8000", help="Backend base URL, e.g. http://localhost:8000")
    parser.add_argument("--bbox", default=DEFAULT_BBOX_URUGUAY, help="Viewport bbox in lon/lat: west,south,east,north")
    parser.add_argument("--zoom", type=int, default=DEFAULT_ZOOM)
    parser.add_argument("--layers", action="append", default=[], help="Repeatable. Example: --layers rgb --layers alerta")
    parser.add_argument("--scope", default="nacional")
    parser.add_argument("--scope-type", default="nacional")
    parser.add_argument("--scope-ref", default="Uruguay")
    parser.add_argument("--window-days", type=int, default=14)
    parser.add_argument("--timeout-seconds", type=float, default=20.0)
    args = parser.parse_args()

    base_url = str(args.base_url).rstrip("/")
    bbox = str(args.bbox)
    zoom = int(args.zoom)
    layers = [str(item).strip() for item in (args.layers or []) if str(item).strip()] or list(DEFAULT_LAYERS)

    lon, lat = _bbox_center(bbox)
    x, y = _tile_xy(lon, lat, zoom)

    date_to = date.today()
    date_from = date_to - timedelta(days=max(args.window_days, 1) - 1)

    with httpx.Client(timeout=args.timeout_seconds) as client:
        health = client.get(f"{base_url}/api/health")
        if health.status_code != 200:
            print(f"[FAIL] /api/health status={health.status_code}", file=sys.stderr)
            return 2

        frames = client.get(
            f"{base_url}/api/v1/timeline/frames",
            params={
                "layers": layers,
                "date_from": date_from.isoformat(),
                "date_to": date_to.isoformat(),
                "bbox": bbox,
                "zoom": zoom,
                "scope": args.scope,
                "scope_type": args.scope_type,
                "scope_ref": args.scope_ref,
            },
        )
        if frames.status_code != 200:
            print(f"[FAIL] /api/v1/timeline/frames status={frames.status_code} body={frames.text[:500]}", file=sys.stderr)
            return 3
        payload = frames.json()

        # Invariant: empty/missing implies non-available + skip.
        for day in payload.get("days") or []:
            for layer_id, frame in ((day.get("layers") or {}).items() if isinstance(day, dict) else []):
                if not isinstance(frame, dict):
                    continue
                state = str(frame.get("visual_state") or "")
                if state in {"missing", "empty"}:
                    if frame.get("available") is True or frame.get("skip_in_playback") is not True:
                        print(f"[FAIL] timeline invariant violated layer={layer_id} date={day.get('display_date')} state={state}", file=sys.stderr)
                        return 4

        pick = _pick_frame(payload, layers=layers)
        if pick is None:
            print("[FAIL] No playable frame found in timeline window (available + non-empty).", file=sys.stderr)
            return 5

        params = {
            "display_date": pick.display_date,
            "source_date": pick.resolved_source_date or pick.display_date,
            "frame_role": "primary",
            "frame_signature": pick.frame_signature or "",
            "scope": args.scope,
            "scope_type": args.scope_type,
            "scope_ref": args.scope_ref,
            "viewport_bbox": bbox,
            "viewport_zoom": zoom,
        }
        tile = client.get(f"{base_url}/api/v1/tiles/{pick.layer_id}/{zoom}/{x}/{y}.png", params=params)
        if tile.status_code != 200:
            print(f"[FAIL] tile status={tile.status_code} url={tile.url}", file=sys.stderr)
            return 6
        if "image/png" not in tile.headers.get("content-type", ""):
            print(f"[FAIL] tile content-type={tile.headers.get('content-type')} url={tile.url}", file=sys.stderr)
            return 7
        if not tile.content:
            print(f"[FAIL] tile empty body url={tile.url}", file=sys.stderr)
            return 8
        if _png_is_fully_transparent(tile.content):
            print(f"[FAIL] tile is fully transparent for playable frame layer={pick.layer_id} display_date={pick.display_date}", file=sys.stderr)
            return 9

    print(f"[OK] timeline+tile smoke passed layer={pick.layer_id} display_date={pick.display_date} zxy={zoom}/{x}/{y}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

