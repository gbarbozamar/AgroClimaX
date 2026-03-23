import { API_BASE } from './api.js';
import { store, setStore } from './state.js';

function tileUrl(layerName) {
  return `${API_BASE}/tiles/${layerName}/{z}/{x}/{y}.png`;
}

function clearMarkers() {
  store.unitMarkers.forEach((marker) => store.map.removeLayer(marker));
  setStore({ unitMarkers: [] });
}

export async function initMap(onPolygonDraw, onDepartmentSelect) {
  const map = window.L.map('map', { zoomControl: true, doubleClickZoom: false }).setView([-32.8, -56.0], 7);
  map.createPane('satellitePane');
  map.getPane('satellitePane').style.zIndex = 380;
  window.L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {
    attribution: '© OpenStreetMap',
    opacity: 0.75,
  }).addTo(map);

  setStore({ map, onPolygonDraw });
  exposeMapControls(onDepartmentSelect);
  setLayer('alerta');
  return map;
}

function exposeMapControls(onDepartmentSelect) {
  window._drawingMode = false;
  window._drawingPoints = [];
  window._previewPoly = null;
  window._previewMarkers = [];
  window._savedPoly = null;

  window.startDrawing = () => {
    window._drawingMode = true;
    window._drawingPoints = [];
    document.getElementById('btn-limpiar').style.display = 'inline-flex';
    document.getElementById('scope-badge-value').textContent = 'Dibujando parcela';
    store.map.getContainer().style.cursor = 'crosshair';
  };

  window.finishDrawing = () => {
    if (window._drawingPoints.length < 3) return;
    const coords = window._drawingPoints.map((point) => [point.lng, point.lat]);
    coords.push(coords[0]);
    const geojson = { type: 'Polygon', coordinates: [coords] };
    if (window._savedPoly) store.map.removeLayer(window._savedPoly);
    window._savedPoly = window.L.polygon(window._drawingPoints.map((point) => [point.lat, point.lng]), {
      color: '#2ecc71',
      weight: 2,
      fillColor: '#4a90d9',
      fillOpacity: 0.2,
    }).addTo(store.map);
    window._drawingMode = false;
    store.map.getContainer().style.cursor = '';
    if (store.onPolygonDraw) store.onPolygonDraw(geojson);
  };

  window.clearDrawing = () => {
    window._drawingMode = false;
    window._drawingPoints = [];
    document.getElementById('btn-limpiar').style.display = 'none';
    document.getElementById('scope-badge-value').textContent = 'Uruguay';
    if (window._savedPoly) store.map.removeLayer(window._savedPoly);
    window._savedPoly = null;
    window._previewMarkers.forEach((marker) => store.map.removeLayer(marker));
    window._previewMarkers = [];
    if (window._previewPoly) store.map.removeLayer(window._previewPoly);
    window._previewPoly = null;
  };

  window.setLayer = (name, btn) => setLayer(name, btn);
  window.setTileOpacity = (value) => setTileOpacity(value);

  store.map.on('click', (event) => {
    if (!window._drawingMode) return;
    window._drawingPoints.push(event.latlng);
    const marker = window.L.circleMarker(event.latlng, { radius: 4, color: '#4a90d9', weight: 1 }).addTo(store.map);
    window._previewMarkers.push(marker);
    if (window._previewPoly) store.map.removeLayer(window._previewPoly);
    if (window._drawingPoints.length > 1) {
      window._previewPoly = window.L.polygon(window._drawingPoints.map((point) => [point.lat, point.lng]), {
        color: '#4a90d9',
        weight: 2,
        fillOpacity: 0.08,
        dashArray: '6 4',
      }).addTo(store.map);
    }
  });

  store.map.on('dblclick', () => {
    if (window._drawingMode) window.finishDrawing();
  });

  store.map.on('zoomend', () => {
    if (store.focusMarker && store.focusMarker.openTooltip) store.focusMarker.openTooltip();
  });

  store.map.on('load', () => {
    if (onDepartmentSelect) onDepartmentSelect();
  });
}

export function setUnitsOnMap(units, onDepartmentSelect) {
  if (!store.map) return;
  clearMarkers();
  const markers = units.map((unit) => {
    const color = unit.state === 'Emergencia' ? '#e74c3c' : unit.state === 'Alerta' ? '#e67e22' : unit.state === 'Vigilancia' ? '#f1c40f' : '#2ecc71';
    const marker = window.L.circleMarker([unit.centroid_lat, unit.centroid_lon], {
      radius: 8,
      color,
      fillColor: color,
      fillOpacity: 0.7,
      weight: 2,
    })
      .bindTooltip(`${unit.department} · ${unit.state || 'Sin dato'} · riesgo ${unit.risk_score ?? '—'}`)
      .on('click', () => onDepartmentSelect(unit.department))
      .addTo(store.map);
    return marker;
  });
  setStore({ unitMarkers: markers });
}

export function updateFocus(model) {
  if (!store.map) return;
  if (store.focusMarker) store.map.removeLayer(store.focusMarker);
  if (model.unitLat && model.unitLon) {
    store.focusMarker = window.L.marker([model.unitLat, model.unitLon]).addTo(store.map);
    store.focusMarker.bindPopup(`<strong>${model.scopeLabel}</strong><br>${model.title}<br>Risk ${model.riskScore ?? '—'}`);
    store.map.setView([model.unitLat, model.unitLon], model.scope === 'nacional' ? 7 : 9);
  } else {
    store.map.setView([-32.8, -56.0], 7);
  }
}

export function setLayer(name, btn) {
  if (!store.map) return;
  store.currentLayer = name;
  document.querySelectorAll('.map-btn').forEach((node) => node.classList.remove('active'));
  if (btn) btn.classList.add('active');
  if (store.activeTileLayer) {
    store.map.removeLayer(store.activeTileLayer);
    setStore({ activeTileLayer: null });
  }

  const opacity = parseFloat(document.getElementById('opacity-slider').value) / 100;
  let layer;
  if (name === 'coneat') {
    layer = window.L.tileLayer.wms(`${API_BASE}/proxy/coneat`, {
      layers: '2,5',
      format: 'image/png',
      transparent: true,
      version: '1.1.1',
      srs: 'EPSG:4326',
      opacity,
    });
  } else if (name === 'judicial') {
    layer = window.L.tileLayer.wms('https://mapas.ide.uy/geoservicios/WMS/WMS_Uruguay', {
      layers: 'sec_judicial_poligono',
      format: 'image/png',
      transparent: true,
      version: '1.1.1',
      opacity,
    });
  } else {
    const tileLayerName = name === 'alerta' ? 'alerta_fusion' : name;
    layer = window.L.tileLayer(tileUrl(tileLayerName), {
      pane: 'satellitePane',
      maxZoom: 13,
      minZoom: 7,
      tileSize: 256,
      opacity,
    });
  }

  layer.addTo(store.map);
  setStore({ activeTileLayer: layer });
  document.getElementById('opacity-wrap').style.display = 'flex';
}

export function setTileOpacity(value) {
  document.getElementById('opacity-label').textContent = `${Math.round(value * 100)}%`;
  if (store.activeTileLayer) store.activeTileLayer.setOpacity(value);
}
