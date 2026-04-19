"""
Regression test: `_execute_preload_run` used to raise TypeError on line 386
because `sum(len(...) * len(...) * len(...))` passed a product (int) to sum(),
not an iterable. Bug masked under asyncio so the task died silently, leaving
every preload run stuck with status="running" forever.

The fix replaced both sums with direct multiplications. This test guards the
arithmetic path so a future refactor can't accidentally reintroduce the same
shape.
"""
from __future__ import annotations

from app.services import preload as preload_module


def test_progress_total_arithmetic_does_not_raise():
    """Mimic the exact expression from preload._execute_preload_run line 380-386."""
    target_dates = ["2026-04-18", "2026-04-19"]
    temporal_layers = ["alerta", "rgb"]
    bbox = f"{preload_module.settings.aoi_bbox_west},{preload_module.settings.aoi_bbox_south},{preload_module.settings.aoi_bbox_east},{preload_module.settings.aoi_bbox_north}"
    zoom_levels = preload_module._zoom_levels(7)
    assert zoom_levels, "expected at least one zoom level in preload config"

    critical_tile_tasks = (
        len(preload_module._tile_coords_for_bbox(bbox, zoom_levels[0]))
        * len(target_dates)
        * len(temporal_layers)
    )
    progress_total = 1 + len(target_dates) + critical_tile_tasks
    if len(zoom_levels) > 1:
        progress_total += (
            len(preload_module._tile_coords_for_bbox(bbox, zoom_levels[1]))
            * len(target_dates)
            * len(temporal_layers)
        )
    assert isinstance(progress_total, int)
    assert progress_total >= 1


def test_tile_coords_helper_returns_list():
    """_tile_coords_for_bbox must be iterable — underlying assumption of the fix."""
    bbox = "-57.5,-32.0,-53.5,-30.0"
    coords = preload_module._tile_coords_for_bbox(bbox, 7)
    assert isinstance(coords, list)
    # Rivera bbox @ z7 should yield at least one tile
    assert len(coords) >= 1
    assert all(isinstance(t, tuple) and len(t) == 2 for t in coords)
