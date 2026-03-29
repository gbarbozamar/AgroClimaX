import { API_BASE } from './api.js?v=20260329-1';
import { store, setStore } from './state.js?v=20260329-1';

const CONEAT_MIN_VISIBLE_ZOOM = 11;

function tileUrl(layerName) {
  return `${API_BASE}/tiles/${layerName}/{z}/{x}/{y}.png`;
}

function clearMarkers() {
  store.unitMarkers.forEach((marker) => store.map.removeLayer(marker));
  setStore({ unitMarkers: [] });
}

function clearDepartmentLayer() {
  if (store.departmentsLayer) store.map.removeLayer(store.departmentsLayer);
  setStore({ departmentsLayer: null, departmentsLookup: {} });
}

function currentOpacity() {
  const slider = document.getElementById('opacity-slider');
  return slider ? parseFloat(slider.value) / 100 : 0.85;
}

function showMapStatus(message, timeoutMs = 0) {
  const loading = document.getElementById('map-tile-loading');
  if (!loading) return;
  loading.textContent = message;
  loading.style.display = 'block';
  if (timeoutMs > 0) {
    window.setTimeout(() => {
      if (loading.textContent === message) loading.style.display = 'none';
    }, timeoutMs);
  }
}

function hideMapStatus(expectedMessage = null) {
  const loading = document.getElementById('map-tile-loading');
  if (!loading) return;
  if (!expectedMessage || loading.textContent === expectedMessage) loading.style.display = 'none';
}

function clearSectionsLayer() {
  if (store.sectionsLayer) store.map.removeLayer(store.sectionsLayer);
  setStore({ sectionsLayer: null, sectionsLookup: {} });
}

function clearProductiveLayer() {
  if (store.productiveLayer) store.map.removeLayer(store.productiveLayer);
  setStore({ productiveLayer: null, productiveLookup: {} });
}

function clearHexLayer() {
  if (store.hexLayer) store.map.removeLayer(store.hexLayer);
  setStore({ hexLayer: null, hexLookup: {} });
}

function syncConeatVisibilityHint() {
  if (!store.map || store.currentLayer !== 'coneat') return;
  if (store.map.getZoom() < CONEAT_MIN_VISIBLE_ZOOM) {
    showMapStatus(`CONEAT se visualiza desde zoom ${CONEAT_MIN_VISIBLE_ZOOM}. Acerque el mapa para ver los suelos.`, 3200);
  }
}

function ensureConeatVisibleZoom() {
  if (!store.map || store.map.getZoom() >= CONEAT_MIN_VISIBLE_ZOOM) return;

  const selectedDepartmentLayer = store.selectedDepartment
    ? store.departmentsLookup?.[store.selectedDepartment]
    : null;

  if (selectedDepartmentLayer?.getBounds) {
    store.map.fitBounds(selectedDepartmentLayer.getBounds(), { padding: [24, 24] });
    if (store.map.getZoom() < CONEAT_MIN_VISIBLE_ZOOM) {
      store.map.setZoom(CONEAT_MIN_VISIBLE_ZOOM);
    }
  } else {
    store.map.setZoom(CONEAT_MIN_VISIBLE_ZOOM);
  }

  showMapStatus(`CONEAT requiere mayor detalle. Acerqué el mapa a zoom ${CONEAT_MIN_VISIBLE_ZOOM}.`, 3400);
}

function departmentColor(props) {
  return sectionColor(props);
}

function departmentStyle(props, selected = false) {
  const opacity = currentOpacity();
  const color = departmentColor(props);
  return {
    color: selected ? '#7dc7ff' : color,
    weight: selected ? 3 : 1.5,
    opacity: selected ? 1 : Math.max(0.55, opacity),
    fillColor: color,
    fillOpacity: selected ? Math.min(0.45, opacity * 0.45) : Math.min(0.22, opacity * 0.22),
  };
}

function sectionColor(props) {
  return props?.color
    || (props?.state === 'Emergencia' ? '#e74c3c'
      : props?.state === 'Alerta' ? '#e67e22'
        : props?.state === 'Vigilancia' ? '#f1c40f'
          : '#2ecc71');
}

function sectionStyle(props, selected = false) {
  const opacity = currentOpacity();
  const color = sectionColor(props);
  return {
    color: selected ? '#4a90d9' : color,
    weight: selected ? 3 : 1.2,
    opacity: selected ? 1 : Math.max(0.55, opacity),
    fillColor: color,
    fillOpacity: selected ? Math.min(0.6, opacity * 0.7) : Math.min(0.42, opacity * 0.42),
  };
}

function hexStyle(props, selected = false) {
  const opacity = currentOpacity();
  const color = sectionColor(props);
  return {
    color: selected ? '#9ad8ff' : color,
    weight: selected ? 2.2 : 0.8,
    opacity: selected ? 1 : Math.max(0.45, opacity),
    fillColor: color,
    fillOpacity: selected ? Math.min(0.68, opacity * 0.75) : Math.min(0.5, opacity * 0.5),
  };
}

function productiveStyle(props, selected = false) {
  const opacity = currentOpacity();
  const color = sectionColor(props);
  return {
    color: selected ? '#ffffff' : color,
    weight: selected ? 3.5 : 2,
    opacity: selected ? 1 : Math.max(0.7, opacity),
    fillColor: color,
    fillOpacity: selected ? Math.min(0.55, opacity * 0.6) : Math.min(0.28, opacity * 0.3),
  };
}

function sectionPopup(props) {
  const raw = props.raw_metrics || {};
  return `
    <div style="min-width:220px">
      <strong>${props.unit_name || 'Seccion'}</strong><br>
      <span style="color:#9fb0c7">${props.department || ''} · ${props.state || 'Sin dato'}</span><br>
      <div style="margin-top:6px">Risk: <strong>${props.risk_score ?? '—'}</strong> · Confianza: <strong>${props.confidence_score ?? '—'}</strong></div>
      <div>Humedad S1: <strong>${raw.s1_humidity_mean_pct ?? '—'}%</strong></div>
      <div>NDMI: <strong>${raw.s2_ndmi_mean ?? raw.estimated_ndmi ?? '—'}</strong></div>
      <div>SPI-30: <strong>${raw.spi_30d ?? '—'}</strong></div>
    </div>
  `;
}

function departmentPopup(props) {
  const raw = props.raw_metrics || {};
  return `
    <div style="min-width:230px">
      <strong>${props.unit_name || props.department || 'Departamento'}</strong><br>
      <span style="color:#9fb0c7">${props.state || 'Sin dato'} · ${props.cache_status || 'cache'}</span><br>
      <div style="margin-top:6px">Risk: <strong>${props.risk_score ?? '—'}</strong> · Confianza: <strong>${props.confidence_score ?? '—'}</strong></div>
      <div>Humedad S1: <strong>${raw.s1_humidity_mean_pct ?? '—'}%</strong></div>
      <div>NDMI: <strong>${raw.s2_ndmi_mean ?? raw.estimated_ndmi ?? '—'}</strong></div>
      <div>SPI-30: <strong>${raw.spi_30d ?? '—'}</strong></div>
      <div>Área afectada: <strong>${props.affected_pct ?? '—'}%</strong></div>
    </div>
  `;
}

function hexPopup(props) {
  const raw = props.raw_metrics || {};
  return `
    <div style="min-width:240px">
      <strong>${props.unit_name || 'Hexagono H3'}</strong><br>
      <span style="color:#9fb0c7">${props.department || ''} Â· ${props.state || 'Sin dato'} Â· r${props.h3_resolution ?? 'N/D'}</span><br>
      <div style="margin-top:6px">Risk: <strong>${props.risk_score ?? 'â€”'}</strong> Â· Confianza: <strong>${props.confidence_score ?? 'â€”'}</strong></div>
      <div>Humedad S1: <strong>${raw.s1_humidity_mean_pct ?? 'â€”'}%</strong></div>
      <div>NDMI: <strong>${raw.s2_ndmi_mean ?? raw.estimated_ndmi ?? 'â€”'}</strong></div>
      <div>SPI-30: <strong>${raw.spi_30d ?? 'â€”'}</strong></div>
      <div>Ãrea afectada: <strong>${props.affected_pct ?? 'â€”'}%</strong></div>
    </div>
  `;
}

function productivePopup(props) {
  const raw = props.raw_metrics || {};
  return `
    <div style="min-width:240px">
      <strong>${props.unit_name || 'Unidad productiva'}</strong><br>
      <span style="color:#9fb0c7">${props.department || ''} · ${props.unit_category || 'predio'} · ${props.state || 'Sin dato'}</span><br>
      <div style="margin-top:6px">Risk: <strong>${props.risk_score ?? '—'}</strong> · Confianza: <strong>${props.confidence_score ?? '—'}</strong></div>
      <div>Humedad S1: <strong>${raw.s1_humidity_mean_pct ?? '—'}%</strong></div>
      <div>NDMI: <strong>${raw.s2_ndmi_mean ?? raw.estimated_ndmi ?? '—'}</strong></div>
      <div>SPI-30: <strong>${raw.spi_30d ?? '—'}</strong></div>
    </div>
  `;
}

function applyDepartmentOpacity() {
  if (!store.departmentsLookup) return;
  Object.entries(store.departmentsLookup).forEach(([departmentName, layer]) => {
    layer.setStyle(departmentStyle(layer.feature?.properties, departmentName === store.selectedDepartment));
  });
}

function applySectionOpacity() {
  if (!store.sectionsLookup) return;
  Object.entries(store.sectionsLookup).forEach(([unitId, layer]) => {
    layer.setStyle(sectionStyle(layer.feature?.properties, unitId === store.selectedSectionId));
  });
}

function applyProductiveOpacity() {
  if (!store.productiveLookup) return;
  Object.entries(store.productiveLookup).forEach(([unitId, layer]) => {
    layer.setStyle(productiveStyle(layer.feature?.properties, unitId === store.selectedProductiveId));
  });
}

function applyHexOpacity() {
  if (!store.hexLookup) return;
  Object.entries(store.hexLookup).forEach(([unitId, layer]) => {
    layer.setStyle(hexStyle(layer.feature?.properties, unitId === store.selectedHexId));
  });
}

export async function initMap(onPolygonDraw, onDepartmentSelect, onSectionSelect) {
  const map = window.L.map('map', { zoomControl: true, doubleClickZoom: false }).setView([-32.8, -56.0], 7);
  map.createPane('satellitePane');
  map.getPane('satellitePane').style.zIndex = 380;
  const baseTileLayer = window.L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {
    attribution: '© OpenStreetMap',
    opacity: 0.75,
  }).addTo(map);

  setStore({ map, onPolygonDraw, onSectionSelect, baseTileLayer });
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
    syncConeatVisibilityHint();
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

export function setDepartmentsOnMap(featureCollection, onDepartmentSelect, selectedDepartment = null) {
  if (!store.map) return;
  clearDepartmentLayer();
  const cacheStatus = featureCollection?.metadata?.cache_status || null;
  const departmentsLookup = {};
  const layer = window.L.geoJSON(featureCollection, {
    style: (feature) => departmentStyle(feature.properties, feature.properties.department === selectedDepartment),
    onEachFeature: (feature, featureLayer) => {
      const props = { ...(feature.properties || {}), cache_status: (feature.properties || {}).cache_status || cacheStatus };
      departmentsLookup[props.department] = featureLayer;
      featureLayer.bindPopup(departmentPopup(props));
      featureLayer.on('click', () => {
        highlightDepartment(props.department, true);
        if (onDepartmentSelect) onDepartmentSelect(props.department);
      });
    },
  }).addTo(store.map);

  setStore({ departmentsLayer: layer, departmentsLookup, selectedDepartment });
}

export function setSectionsOnMap(featureCollection, onSectionSelect, selectedSectionId = null) {
  if (!store.map) return;
  clearSectionsLayer();
  const sectionsLookup = {};
  const layer = window.L.geoJSON(featureCollection, {
    style: (feature) => sectionStyle(feature.properties, feature.properties.unit_id === selectedSectionId),
    onEachFeature: (feature, featureLayer) => {
      const props = feature.properties || {};
      sectionsLookup[props.unit_id] = featureLayer;
      featureLayer.bindPopup(sectionPopup(props));
      featureLayer.on('click', () => {
        highlightSection(props.unit_id, true);
        if (onSectionSelect) onSectionSelect(props);
      });
    },
  }).addTo(store.map);

  setStore({ sectionsLayer: layer, sectionsLookup, selectedSectionId });
}

export function setProductivesOnMap(featureCollection, onProductiveSelect, selectedProductiveId = null) {
  if (!store.map) return;
  clearProductiveLayer();
  const productiveLookup = {};
  const layer = window.L.geoJSON(featureCollection, {
    style: (feature) => productiveStyle(feature.properties, feature.properties.unit_id === selectedProductiveId),
    onEachFeature: (feature, featureLayer) => {
      const props = feature.properties || {};
      productiveLookup[props.unit_id] = featureLayer;
      featureLayer.bindPopup(productivePopup(props));
      featureLayer.on('click', () => {
        highlightProductive(props.unit_id, true);
        if (onProductiveSelect) onProductiveSelect(props);
      });
    },
  }).addTo(store.map);

  setStore({ productiveLayer: layer, productiveLookup, selectedProductiveId });
}

export function setHexesOnMap(featureCollection, onHexSelect, selectedHexId = null) {
  if (!store.map) return;
  clearHexLayer();
  const hexLookup = {};
  const layer = window.L.geoJSON(featureCollection, {
    style: (feature) => hexStyle(feature.properties, feature.properties.unit_id === selectedHexId),
    onEachFeature: (feature, featureLayer) => {
      const props = feature.properties || {};
      hexLookup[props.unit_id] = featureLayer;
      featureLayer.bindPopup(hexPopup(props));
      featureLayer.on('click', () => {
        highlightHex(props.unit_id, true);
        if (onHexSelect) onHexSelect(props);
      });
    },
  }).addTo(store.map);

  setStore({ hexLayer: layer, hexLookup, selectedHexId });
}

export function highlightDepartment(departmentName, fitBounds = false) {
  if (!store.departmentsLookup || !Object.keys(store.departmentsLookup).length) return;
  setStore({ selectedDepartment: departmentName });
  applyDepartmentOpacity();
  const layer = store.departmentsLookup[departmentName];
  if (!layer) return;
  if (layer.bringToFront) layer.bringToFront();
  if (fitBounds && layer.getBounds) {
    store.map.fitBounds(layer.getBounds(), { padding: [20, 20], maxZoom: 8 });
  }
  if (layer.openPopup) layer.openPopup();
}

export function highlightSection(sectionId, fitBounds = false) {
  if (!store.sectionsLookup || !Object.keys(store.sectionsLookup).length) return;
  setStore({ selectedSectionId: sectionId });
  applySectionOpacity();
  const layer = store.sectionsLookup[sectionId];
  if (!layer) return;
  if (layer.bringToFront) layer.bringToFront();
  if (fitBounds && layer.getBounds) {
    store.map.fitBounds(layer.getBounds(), { padding: [20, 20], maxZoom: 10 });
  }
  if (layer.openPopup) layer.openPopup();
}

export function highlightProductive(unitId, fitBounds = false) {
  if (!store.productiveLookup || !Object.keys(store.productiveLookup).length) return;
  setStore({ selectedProductiveId: unitId });
  applyProductiveOpacity();
  const layer = store.productiveLookup[unitId];
  if (!layer) return;
  if (layer.bringToFront) layer.bringToFront();
  if (fitBounds && layer.getBounds) {
    store.map.fitBounds(layer.getBounds(), { padding: [20, 20], maxZoom: 13 });
  }
  if (layer.openPopup) layer.openPopup();
}

export function highlightHex(hexId, fitBounds = false) {
  if (!store.hexLookup || !Object.keys(store.hexLookup).length) return;
  setStore({ selectedHexId: hexId });
  applyHexOpacity();
  const layer = store.hexLookup[hexId];
  if (!layer) return;
  if (layer.bringToFront) layer.bringToFront();
  if (fitBounds && layer.getBounds) {
    store.map.fitBounds(layer.getBounds(), { padding: [20, 20], maxZoom: 10 });
  }
  if (layer.openPopup) layer.openPopup();
}

export function updateFocus(model) {
  if (!store.map) return;
  if (store.focusMarker) store.map.removeLayer(store.focusMarker);
  if (model.unitLat && model.unitLon) {
    store.focusMarker = window.L.marker([model.unitLat, model.unitLon]).addTo(store.map);
    store.focusMarker.bindPopup(`<strong>${model.scopeLabel}</strong><br>${model.title}<br>Risk ${model.riskScore ?? '—'}`);
    if (!((store.currentLayer === 'judicial' && store.selectedSectionId) || (store.currentLayer === 'productiva' && store.selectedProductiveId) || (store.currentLayer === 'hex' && store.selectedHexId))) {
      store.map.setView([model.unitLat, model.unitLon], model.scope === 'nacional' ? 7 : 9);
    }
  } else {
    store.map.setView([-32.8, -56.0], 7);
  }
}

export function setLayer(name, btn) {
  if (!store.map) return;
  store.currentLayer = name;
  if (store.baseTileLayer) {
    store.baseTileLayer.setOpacity(name === 'coneat' ? 0.18 : 0.75);
  }
  document.querySelectorAll('.map-btn').forEach((node) => node.classList.remove('active'));
  if (btn) btn.classList.add('active');
  if (store.activeTileLayer) {
    store.map.removeLayer(store.activeTileLayer);
    setStore({ activeTileLayer: null });
  }
  if (name === 'coneat') {
    clearDepartmentLayer();
    clearSectionsLayer();
    clearProductiveLayer();
    clearHexLayer();
  } else if (name === 'productiva') {
    clearDepartmentLayer();
    clearSectionsLayer();
    clearHexLayer();
  } else if (name === 'hex') {
    clearDepartmentLayer();
    clearSectionsLayer();
    clearProductiveLayer();
  } else if (name === 'judicial') {
    clearDepartmentLayer();
    clearProductiveLayer();
    clearHexLayer();
  } else {
    clearSectionsLayer();
    clearProductiveLayer();
    clearHexLayer();
  }

  const opacity = parseFloat(document.getElementById('opacity-slider').value) / 100;
  const loading = document.getElementById('map-tile-loading');
  let layer;
  if (name === 'coneat') {
    ensureConeatVisibleZoom();
    layer = window.L.tileLayer.wms(`${API_BASE}/proxy/coneat`, {
      layers: '2',
      styles: 'default',
      format: 'image/png32',
      transparent: true,
      version: '1.1.1',
      crs: window.L.CRS.EPSG4326,
      uppercase: true,
      attribution: '© MGAP/DGRN Uruguay',
      tileSize: 512,
      updateWhenIdle: true,
      keepBuffer: 1,
      opacity: Math.max(opacity, 0.96),
    });
  } else if (name === 'judicial') {
    document.getElementById('opacity-wrap').style.display = 'flex';
    return;
  } else if (name === 'productiva') {
    document.getElementById('opacity-wrap').style.display = 'flex';
    return;
  } else if (name === 'hex') {
    document.getElementById('opacity-wrap').style.display = 'flex';
    return;
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

  if (loading) {
    layer.on('loading', () => {
      loading.textContent = name === 'coneat'
        ? `Cargando CONEAT desde MGAP... visible desde zoom ${CONEAT_MIN_VISIBLE_ZOOM}+`
        : 'Cargando capa...';
      loading.style.display = 'block';
    });
    layer.on('load', () => {
      if (name === 'coneat' && store.map.getZoom() < CONEAT_MIN_VISIBLE_ZOOM) {
        syncConeatVisibilityHint();
        return;
      }
      hideMapStatus();
    });
  }
  layer.on('tileerror', (event) => {
    if (loading) {
      loading.textContent = name === 'coneat' ? 'No se pudo cargar CONEAT' : 'No se pudo cargar la capa';
      loading.style.display = 'block';
      window.setTimeout(() => {
        if (loading.textContent === 'No se pudo cargar CONEAT' || loading.textContent === 'No se pudo cargar la capa') {
          loading.style.display = 'none';
        }
      }, 2500);
    }
    console.warn(`Tile error en capa ${name}`, event);
  });

  layer.addTo(store.map);
  setStore({ activeTileLayer: layer });
  document.getElementById('opacity-wrap').style.display = 'flex';
}

export function setTileOpacity(value) {
  document.getElementById('opacity-label').textContent = `${Math.round(value * 100)}%`;
  if (store.currentLayer === 'judicial') {
    applySectionOpacity();
    return;
  }
  if (store.currentLayer === 'productiva') {
    applyProductiveOpacity();
    return;
  }
  if (store.currentLayer === 'hex') {
    applyHexOpacity();
    return;
  }
  applyDepartmentOpacity();
  if (store.activeTileLayer) store.activeTileLayer.setOpacity(value);
}
