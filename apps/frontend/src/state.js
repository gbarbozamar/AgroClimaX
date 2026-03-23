export const store = {
  apiBase: null,
  apiV1: null,
  units: [],
  selectedScope: 'nacional',
  selectedDepartment: null,
  selectedUnitId: null,
  customGeojson: null,
  map: null,
  chart: null,
  currentLayer: 'alerta',
  activeTileLayer: null,
  unitMarkers: [],
  focusMarker: null,
  onPolygonDraw: null,
};

export function setStore(patch) {
  Object.assign(store, patch);
  return store;
}
