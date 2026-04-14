function textOrDash(value, suffix = '') {
  if (value === null || value === undefined || Number.isNaN(value)) return '-';
  return `${value}${suffix}`;
}

function fixed(value, digits = 1, suffix = '') {
  if (value === null || value === undefined || Number.isNaN(value)) return '-';
  return `${Number(value).toFixed(digits)}${suffix}`;
}

function resolveRoot(root) {
  if (!root) return document;
  return root;
}

function getNode(root, roleOrId) {
  const resolvedRoot = resolveRoot(root);
  if (!roleOrId) return null;

  if (resolvedRoot && typeof resolvedRoot.getElementById === 'function') {
    return resolvedRoot.getElementById(roleOrId) || resolvedRoot.querySelector?.(`[data-role="${roleOrId}"]`) || null;
  }
  if (resolvedRoot && typeof resolvedRoot.querySelector === 'function') {
    return resolvedRoot.querySelector(`[data-role="${roleOrId}"]`) || resolvedRoot.querySelector(`#${roleOrId}`);
  }
  return null;
}

function setText(root, roleOrId, value) {
  const node = getNode(root, roleOrId);
  if (!node) return;
  node.textContent = value ?? '';
}

function setStyle(root, roleOrId, key, value) {
  const node = getNode(root, roleOrId);
  if (!node) return;
  node.style[key] = value;
}

const METRIC_METADATA = {
  humidity_s1: {
    valueId: 'kpi-humedad',
    subtext: 'Sentinel-1 SAR | proxy superficial (%)',
    tooltipTitle: 'Humedad Suelo (S1)',
    unit: '%',
    description: 'Proxy de humedad superficial derivado desde retrodispersión VV de Sentinel-1 SAR y calibracion local AgroClimaX. No equivale a una medicion in situ de humedad volumetrica.',
    references: [
      { label: 'ESA Sentinel-1 Facts and Figures', url: 'https://www.esa.int/Applications/Observing_the_Earth/Copernicus/Sentinel-1/Facts_and_figures' },
      { label: 'Copernicus Data Space Ecosystem', url: 'https://dataspace.copernicus.eu/' },
      { label: 'AgroClimaX Settings API', url: '/api/v1/settings' },
    ],
  },
  ndmi_s2: {
    valueId: 'kpi-ndmi',
    subtext: 'Sentinel-2 | indice adimensional (idx)',
    tooltipTitle: 'NDMI Vegetacion (S2)',
    unit: 'idx',
    description: 'Indice NDMI calculado como (NIR - SWIR) / (NIR + SWIR). En AgroClimaX se usa Sentinel-2 con B08 (842 nm) y B11 (1610 nm).',
    references: [
      { label: 'USGS NDMI', url: 'https://www.usgs.gov/landsat-missions/normalized-difference-moisture-index' },
      { label: 'Copernicus Sentinel-2 bands B08/B11', url: 'https://land.copernicus.eu/en/technical-library/sentinel-2-global-mosaic-product-user-manual/@@download/file' },
    ],
  },
  spi_30d: {
    valueId: 'kpi-spi',
    subtext: 'ERA5 | indice estandarizado (std)',
    tooltipTitle: 'SPI-30 (ERA5)',
    unit: 'std',
    description: 'SPI a 30 dias calculado sobre precipitacion derivada de ERA5. Es un indice estandarizado sin unidad fisica.',
    references: [
      { label: 'NOAA SPI background', url: 'https://www.weather.gov/hfo/spi_info' },
      { label: 'Copernicus CDS ERA5', url: 'https://cds.climate.copernicus.eu/datasets/reanalysis-era5-single-levels-timeseries?tab=overview' },
      { label: 'ECMWF ERA5', url: 'https://www.ecmwf.int/en/forecasts/dataset/ecmwf-reanalysis-v5' },
    ],
  },
  area_alerta: {
    valueId: 'kpi-area',
    subtext: '% del AOI/unidad afectada',
    tooltipTitle: 'Area en Alerta',
    unit: '%',
    description: 'Porcentaje del AOI o unidad operativa cuyo risk score supera el umbral espacial configurable del motor.',
    references: [
      { label: 'AgroClimaX Settings schema', url: '/api/v1/settings/schema' },
      { label: 'AgroClimaX estado actual API', url: '/api/v1/alertas/estado-actual?scope=nacional' },
    ],
  },
  risk_score: {
    valueId: 'kpi-risk',
    subtext: 'score 0-100 | motor compuesto',
    tooltipTitle: 'Risk Score',
    unit: 'score /100',
    description: 'Score compuesto interno de 0 a 100. Combina magnitud, persistencia, anomalia temporal, confirmacion meteorologica y vulnerabilidad de suelo.',
    references: [
      { label: 'AgroClimaX Settings API', url: '/api/v1/settings' },
      { label: 'AgroClimaX Settings schema', url: '/api/v1/settings/schema' },
    ],
  },
  confidence: {
    valueId: 'kpi-confidence',
    subtext: 'score 0-100 | calidad del dato',
    tooltipTitle: 'Confianza',
    unit: 'score /100',
    description: 'Score interno de calidad y consistencia del dato. Combina frescura, acuerdo S1-S2, aplicabilidad por cobertura, calidad de calibracion y validacion de campo.',
    references: [
      { label: 'AgroClimaX Settings API', url: '/api/v1/settings' },
      { label: 'AgroClimaX Settings schema', url: '/api/v1/settings/schema' },
    ],
  },
  persistence_days: {
    valueId: 'kpi-dias',
    subtext: 'dias consecutivos en estado actual',
    tooltipTitle: 'Persistencia',
    unit: 'dias',
    description: 'Cantidad de dias consecutivos en el estado actual segun el historico de eventos del motor de alertas.',
    references: [
      { label: 'AgroClimaX historico API', url: '/api/v1/alertas/historico?scope=nacional&limit=30' },
      { label: 'AgroClimaX Settings API', url: '/api/v1/settings' },
    ],
  },
};

function buildTooltipHtml(metadata) {
  if (!metadata) return '';
  const references = (metadata.references || [])
    .map((reference) => `<a href="${reference.url}" target="_blank" rel="noopener noreferrer">${reference.label}</a>`)
    .join('<br>');
  return `
    <div class="metric-tooltip-title">${metadata.tooltipTitle}</div>
    <div class="metric-tooltip-unit"><strong>Unidad:</strong> ${metadata.unit}</div>
    <div class="metric-tooltip-copy">${metadata.description}</div>
    <div class="metric-tooltip-copy"><strong>Referencia:</strong><br>${references}</div>
  `;
}

function applyMetricMetadata(root = document) {
  Object.entries(METRIC_METADATA).forEach(([key, metadata]) => {
    const valueNode = getNode(root, metadata.valueId);
    const card = valueNode?.closest('.kpi-card');
    if (!card) return;

    let labelRow = card.querySelector('.kpi-label-row');
    if (!labelRow) {
      labelRow = document.createElement('div');
      labelRow.className = 'kpi-label-row';
      const labelNode = card.querySelector('.kpi-label');
      if (labelNode) {
        card.insertBefore(labelRow, labelNode);
        labelRow.appendChild(labelNode);
      } else {
        card.insertBefore(labelRow, card.firstChild);
        const labelNodeFallback = document.createElement('div');
        labelNodeFallback.className = 'kpi-label';
        labelRow.appendChild(labelNodeFallback);
      }
    }

    const labelNode = labelRow.querySelector('.kpi-label');
    if (labelNode) labelNode.textContent = metadata.tooltipTitle;

    let tooltipWrap = labelRow.querySelector('.metric-tooltip-wrap');
    if (!tooltipWrap) {
      tooltipWrap = document.createElement('div');
      tooltipWrap.className = 'metric-tooltip-wrap';
      labelRow.appendChild(tooltipWrap);
    }

    let infoButton = tooltipWrap.querySelector('.metric-info-btn');
    if (!infoButton) {
      infoButton = document.createElement('button');
      infoButton.type = 'button';
      infoButton.className = 'metric-info-btn';
      infoButton.textContent = 'i';
      tooltipWrap.appendChild(infoButton);
    }
    infoButton.setAttribute('aria-label', `Unidad y fuente de ${metadata.tooltipTitle}`);

    let tooltipNode = tooltipWrap.querySelector('.metric-tooltip-panel');
    if (!tooltipNode) {
      tooltipNode = document.createElement('div');
      tooltipNode.className = 'metric-tooltip-panel';
      tooltipWrap.appendChild(tooltipNode);
    }
    tooltipNode.dataset.metricTooltip = key;
    tooltipNode.innerHTML = buildTooltipHtml(metadata);

    const subNode = card.querySelector('.kpi-sub');
    if (subNode) subNode.textContent = metadata.subtext;
  });
}

function formatMetricValue(key, value) {
  switch (key) {
    case 'humidity_s1':
      return fixed(value, 1, ' %');
    case 'ndmi_s2':
      return fixed(value, 3, ' idx');
    case 'spi_30d':
      return fixed(value, 2, ' std');
    case 'area_alerta':
      return fixed(value, 1, ' %');
    case 'risk_score':
      return fixed(value, 1, ' /100');
    case 'confidence':
      return fixed(value, 1, ' /100');
    case 'persistence_days':
      return textOrDash(value, ' d');
    default:
      return textOrDash(value);
  }
}

const VIEWER_METRIC_IDS = {
  humidity_s1: 'viewer-kpi-humedad',
  ndmi_s2: 'viewer-kpi-ndmi',
  spi_30d: 'viewer-kpi-spi',
  area_alerta: 'viewer-kpi-area',
  risk_score: 'viewer-kpi-risk',
  confidence: 'viewer-kpi-confidence',
  persistence_days: 'viewer-kpi-dias',
};

export function clearEstablishmentViewerDashboard(message = 'Selecciona un campo para ver sus metricas.') {
  const statusNode = document.getElementById('establishment-viewer-kpi-status');
  if (statusNode) statusNode.textContent = message;
  Object.entries(VIEWER_METRIC_IDS).forEach(([metricKey, nodeId]) => {
    const valueNode = document.getElementById(nodeId);
    if (valueNode) valueNode.textContent = formatMetricValue(metricKey, null);
  });
}

export function renderEstablishmentViewerDashboard(model) {
  if (!model) {
    clearEstablishmentViewerDashboard();
    return;
  }
  const statusNode = document.getElementById('establishment-viewer-kpi-status');
  if (statusNode) {
    statusNode.textContent = `${model.scopeLabel || 'Campo'} · ${model.title || model.state || 'Sin estado'} · modo ${model.dataMode || 'N/D'}`;
  }
  const metricValues = {
    humidity_s1: model.humidity,
    ndmi_s2: model.ndmi,
    spi_30d: model.spi,
    area_alerta: model.affectedPct,
    risk_score: model.riskScore,
    confidence: model.confidenceScore,
    persistence_days: model.daysInState,
  };
  Object.entries(VIEWER_METRIC_IDS).forEach(([metricKey, nodeId]) => {
    const valueNode = document.getElementById(nodeId);
    if (!valueNode) return;
    valueNode.textContent = formatMetricValue(metricKey, metricValues[metricKey]);
  });
}

export function populateDepartmentSelect(units, selected = 'nacional') {
  const select = getNode(document, 'department-select');
  if (!select) return;
  select.innerHTML = '<option value="nacional">Uruguay (nacional)</option>';
  units.forEach((unit) => {
    const option = document.createElement('option');
    option.value = unit.department;
    option.textContent = unit.department;
    if (selected === unit.department) option.selected = true;
    select.appendChild(option);
  });
}

export function normalizeState(data, context = {}) {
  if (data && data.alerta) {
    return {
      scope: 'unidad',
      state: data.alerta.nivel || 'SIN_DATOS',
      color: data.alerta.color || '#4a90d9',
      title: data.alerta.nivel || 'SIN DATOS',
      explanation: data.alerta.descripcion || '',
      action: data.alerta.accion || '',
      extra: 'Analisis por parcela custom',
      humidity: data.sentinel_1?.humedad_media ?? data.resumen?.humedad_s1_pct ?? null,
      ndmi: data.sentinel_2?.ndmi_media ?? data.resumen?.ndmi_s2 ?? null,
      spi: data.era5?.spi_30d ?? data.resumen?.spi_30d ?? null,
      affectedPct: data.sentinel_1?.pct_area_bajo_estres ?? 0,
      riskScore: null,
      confidenceScore: null,
      daysInState: data.dias_deficit ?? 0,
      drivers: [],
      forecast: [],
      calibrationRef: 'legacy',
      dataMode: data.advertencia ? 'simulado' : 'legacy/live',
      largestClusterPct: null,
      scopeLabel: 'Parcela custom',
      technical: {
        calibration: 'Legacy wrapper',
        unit: 'Parcela / H3 r9',
        soil: 'Pendiente de agregacion',
        forecast: 'ERA5 + Open-Meteo',
        mode: data.advertencia ? 'Simulado' : 'Live/legacy',
      },
      chartSeries: [],
      alertHistory: [],
      unitLat: context.unitLat ?? null,
      unitLon: context.unitLon ?? null,
    };
  }

  const raw = data.raw_metrics || {};
  const topRiskDepartments = raw.top_risk_departments || [];
  const aggregateFallback = (field) => {
    const values = topRiskDepartments
      .map((item) => item?.raw_metrics?.[field])
      .filter((value) => value !== null && value !== undefined && !Number.isNaN(value));
    if (!values.length) return null;
    const total = values.reduce((sum, value) => sum + Number(value), 0);
    return total / values.length;
  };
  return {
    scope: data.scope,
    state: data.state,
    color: data.color || '#4a90d9',
    title: data.legacy_level || data.state,
    explanation: data.explanation || '',
    action: data.actionable ? 'Senal accionable por cobertura y conectividad.' : 'Seguir monitoreando evolucion.',
    extra: `Calibracion ${data.calibration_ref || 'N/D'} | ${data.data_mode || 'sin modo'} | cache ${data.cache_status || 'N/D'} | cluster ${fixed(data.largest_cluster_pct, 1, '%')}`,
    humidity: raw.s1_humidity_mean_pct ?? aggregateFallback('s1_humidity_mean_pct') ?? null,
    ndmi: raw.s2_ndmi_mean ?? raw.estimated_ndmi ?? aggregateFallback('s2_ndmi_mean') ?? aggregateFallback('estimated_ndmi') ?? null,
    spi: raw.spi_30d ?? aggregateFallback('spi_30d') ?? null,
    affectedPct: data.affected_pct ?? null,
    riskScore: data.risk_score ?? null,
    confidenceScore: data.confidence_score ?? null,
    daysInState: data.days_in_state ?? 0,
    drivers: data.drivers || [],
    forecast: data.forecast || [],
    calibrationRef: data.calibration_ref || 'N/D',
    dataMode: data.data_mode || 'N/D',
    largestClusterPct: data.largest_cluster_pct ?? null,
    scopeLabel: context.scopeLabel || data.department || data.unit_name || 'Uruguay',
    technical: {
      calibration: data.calibration_ref || 'N/D',
      unit: data.scope === 'nacional'
        ? 'Departamentos + H3 operativo'
        : (data.unit_type === 'productive_unit'
          ? `${data.unit_category || 'Predio'} importado`
        : (data.unit_type === 'h3_cell'
          ? `Hexagono H3 r${data.h3_resolution ?? 'N/D'}`
          : (data.scope === 'seccion' ? 'Seccion policial' : 'Departamento / H3 fallback'))),
      soil: data.soil_context?.texture ? `${data.soil_context.texture} | AWC ${fixed(data.soil_context.water_holding_capacity_mm, 0, ' mm')}` : 'Sin detalle',
      forecast: data.forecast?.length ? `7 dias | max riesgo ${fixed(Math.max(...data.forecast.map((item) => item.expected_risk || 0)), 0)}` : 'Sin pronostico',
      mode: `${data.data_mode || 'N/D'} | ${data.served_from || 'runtime'}`,
    },
    chartSeries: context.history || [],
    alertHistory: context.history || [],
    unitLat: context.unitLat ?? null,
    unitLon: context.unitLon ?? null,
  };
}

export function renderLoading(message = 'Cargando tablero...', root = document) {
  const banner = getNode(root, 'alerta-banner');
  if (!banner) return;
  banner.innerHTML = `<div class="banner-dot" style="background:#4a90d9;animation:pulse 1s infinite"></div><strong style="color:#4a90d9">${message}</strong>`;
  banner.style.background = '#4a90d922';
  banner.style.borderBottom = '1px solid #4a90d944';
}

export function renderError(message, root = document) {
  const banner = getNode(root, 'alerta-banner');
  if (!banner) return;
  banner.innerHTML = `<span style="color:#e74c3c">${message}</span>`;
  banner.style.background = '#e74c3c22';
  banner.style.borderBottom = '1px solid #e74c3c44';
}

export function renderDashboard(model, root = document) {
  applyMetricMetadata(root);
  const banner = getNode(root, 'alerta-banner');
  if (banner) {
    banner.style.background = `${model.color}22`;
    banner.style.borderBottom = `1px solid ${model.color}55`;
    banner.innerHTML = `<div class="banner-dot" style="background:${model.color}"></div><strong>${model.title}</strong> - ${model.explanation}<span style="margin-left:auto;color:var(--text-muted);font-size:0.78rem">${model.scopeLabel}</span>`;
  }

  setText(root, 'scope-badge-value', model.scopeLabel);
  setText(root, 'alerta-nivel-text', `• ${model.title}`);
  setText(root, 'alerta-tipo-badge', `${model.scope || 'unidad'} | ${model.dataMode}`);
  setText(root, 'alerta-descripcion', model.explanation);
  setText(root, 'alerta-accion', model.action);
  setText(root, 'alerta-extra', model.extra);
  setText(root, 'last-update', `Actualizado: ${new Date().toLocaleString()} | Fuente API ${model.scope === 'unidad' ? 'legacy/custom' : 'v1'}`);

  setText(root, 'kpi-humedad', formatMetricValue('humidity_s1', model.humidity));
  setText(root, 'kpi-ndmi', formatMetricValue('ndmi_s2', model.ndmi));
  setText(root, 'kpi-spi', formatMetricValue('spi_30d', model.spi));
  setText(root, 'kpi-area', formatMetricValue('area_alerta', model.affectedPct));
  setText(root, 'kpi-risk', formatMetricValue('risk_score', model.riskScore));
  setText(root, 'kpi-confidence', formatMetricValue('confidence', model.confidenceScore));
  setText(root, 'kpi-dias', formatMetricValue('persistence_days', model.daysInState));

  setText(root, 'hum-s1-pct', formatMetricValue('humidity_s1', model.humidity));
  setText(root, 'hum-ndmi-pct', formatMetricValue('ndmi_s2', model.ndmi));
  setStyle(root, 'hum-s1-bar', 'width', `${Math.max(0, Math.min(100, model.humidity || 0))}%`);
  setStyle(root, 'hum-ndmi-bar', 'width', `${Math.max(0, Math.min(100, ((model.ndmi ?? -0.5) + 0.5) * 100))}%`);

  setText(root, 'spi-big', formatMetricValue('spi_30d', model.spi));
  setText(root, 'spi-cat', model.spi === null ? 'Sin datos' : (model.spi < -1.5 ? 'Seco severo' : model.spi < -1 ? 'Seco' : model.spi < 1 ? 'Normal' : 'Humedo'));
  const spiMarker = getNode(root, 'spi-marker');
  if (spiMarker) {
    const spiPosition = model.spi === null ? 50 : Math.max(0, Math.min(100, ((model.spi + 3) / 6) * 100));
    spiMarker.style.left = `${spiPosition}%`;
  }

  setText(root, 'indicador-calibracion', model.calibrationRef);
  setText(root, 'indicador-suelo', model.technical?.soil || '');
  setText(root, 'indicador-forecast', model.technical?.forecast || '');
  setText(root, 'indicador-mode', model.technical?.mode || '');
}

export function renderDrivers(model, root = document) {
  const container = getNode(root, 'drivers-list');
  if (!container) return;
  if (!model.drivers.length) {
    container.innerHTML = '<div style="color:var(--text-muted)">Sin drivers disponibles para esta vista.</div>';
    return;
  }
  container.innerHTML = model.drivers
    .map((driver) => `<div style="padding:10px;border:1px solid var(--border);border-radius:10px;background:rgba(17,23,35,0.55)"><div style="display:flex;justify-content:space-between"><strong>${driver.name}</strong><span style="color:var(--accent)">${fixed(driver.score, 1)}</span></div><div style="margin-top:4px;color:var(--text-muted)">${driver.detail || ''}</div></div>`)
    .join('');
}

function toCompass(degrees) {
  if (degrees === null || degrees === undefined || Number.isNaN(degrees)) return 'N/D';
  const directions = ['N', 'NE', 'E', 'SE', 'S', 'SO', 'O', 'NO'];
  const index = Math.round((((Number(degrees) % 360) + 360) % 360) / 45) % directions.length;
  return directions[index];
}

export function renderWeatherCards(model, selectionLabel = 'Seleccion actual', root = document) {
  const selectionNode = getNode(root, 'weather-filter-label');
  if (selectionNode) selectionNode.textContent = selectionLabel;

  const forecast = model?.forecast || [];
  const day = forecast[0] || {};
  const precipProb = day.precip_probability_pct ?? null;
  const humidity = day.humidity_mean_pct ?? null;
  const tempMin = day.temp_min_c ?? null;
  const windKmh = day.wind_mps !== undefined && day.wind_mps !== null ? Number(day.wind_mps) * 3.6 : null;
  const gustKmh = day.wind_gust_mps !== undefined && day.wind_gust_mps !== null ? Number(day.wind_gust_mps) * 3.6 : null;
  const balance = day.precip_mm !== undefined && day.et0_mm !== undefined
    ? Number(day.precip_mm || 0) - Number(day.et0_mm || 0)
    : null;

  const cards = [
    {
      title: 'Lluvia 24h',
      value: fixed(day.precip_mm, 1, ' mm'),
      sub: `Prob. ${fixed(precipProb, 0, ' %')}`,
    },
    {
      title: 'Temperatura',
      value: fixed(day.temp_max_c, 1, ' C'),
      sub: `Min ${fixed(tempMin, 1, ' C')} | HR ${fixed(humidity, 0, ' %')}`,
    },
    {
      title: 'Viento',
      value: fixed(windKmh, 0, ' km/h'),
      sub: `${toCompass(day.wind_direction_deg)} | rachas ${fixed(gustKmh, 0, ' km/h')}`,
    },
    {
      title: 'ET0 / Balance',
      value: fixed(day.et0_mm, 1, ' mm'),
      sub: `Balance ${fixed(balance, 1, ' mm')} | riesgo ${fixed(day.expected_risk, 0)}`,
    },
  ];

  cards.forEach((card, index) => {
    const n = index + 1;
    const titleNode = getNode(root, `weather-card-title-${n}`);
    const valueNode = getNode(root, `weather-card-value-${n}`);
    const subNode = getNode(root, `weather-card-sub-${n}`);
    if (titleNode) titleNode.textContent = card.title;
    if (valueNode) valueNode.textContent = card.value;
    if (subNode) subNode.textContent = card.sub;
  });
}

export function renderForecast(model, root = document) {
  const container = getNode(root, 'forecast-list');
  if (!container) return;
  if (!model.forecast.length) {
    container.innerHTML = '<div style="color:var(--text-muted)">Sin forecast disponible.</div>';
    return;
  }
  container.innerHTML = model.forecast
    .slice(0, 7)
    .map((day) => `<div style="display:grid;grid-template-columns:82px 1fr auto;gap:10px;align-items:center;padding:8px 10px;border-radius:10px;background:rgba(17,23,35,0.55);border:1px solid var(--border)"><strong>${day.date.slice(5)}</strong><span style="color:var(--text-muted)">Lluvia ${fixed(day.precip_mm, 1, ' mm')} | ET0 ${fixed(day.et0_mm, 1, ' mm')}</span><span style="color:${(day.expected_risk || 0) >= 60 ? '#e74c3c' : (day.expected_risk || 0) >= 40 ? '#e67e22' : '#2ecc71'}">${fixed(day.expected_risk, 0)}</span></div>`)
    .join('');
}

export function renderHistory(model, root = document) {
  const history = model.alertHistory || [];
  const alertsContainer = getNode(root, 'alertas-recientes');
  if (!alertsContainer) return;
  if (!history.length) {
    alertsContainer.innerHTML = '<div style="color:var(--text-muted);font-size:0.8rem">Sin historial reciente.</div>';
    return;
  }
  alertsContainer.innerHTML = history
    .slice(0, 5)
    .map((item) => `<div class="alerta-item"><div class="alerta-item-dot" style="background:${item.state_level >= 3 ? '#e74c3c' : item.state_level >= 2 ? '#e67e22' : '#f1c40f'}"></div><span>${item.state || 'Dato'} | riesgo ${fixed(item.risk_score, 0)}</span><span class="alerta-item-fecha">${(item.fecha || '').slice(5)}</span></div>`)
    .join('');
}

export function renderChart(model, chartRef, root = document) {
  if (!window.Chart) return chartRef;
  const history = model.chartSeries || [];
  const canvas = getNode(root, 'chart-humedad');
  if (!canvas?.getContext) return chartRef;
  const ctx = canvas.getContext('2d');
  if (chartRef) chartRef.destroy();

  return new window.Chart(ctx, {
    type: 'line',
    data: {
      labels: history.map((item) => (item.fecha || '').slice(5)).reverse(),
      datasets: [
        {
          label: 'Risk Score',
          data: history.map((item) => item.risk_score ?? null).reverse(),
          borderColor: '#e67e22',
          backgroundColor: 'rgba(230,126,34,0.15)',
          tension: 0.3,
          fill: true,
        },
        {
        label: 'Area afectada %',
          data: history.map((item) => item.affected_pct ?? item.humedad_pct ?? null).reverse(),
          borderColor: '#4a90d9',
          backgroundColor: 'rgba(74,144,217,0.08)',
          tension: 0.3,
          fill: false,
        },
      ],
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      plugins: { legend: { labels: { color: '#d6deea' } } },
      scales: {
        x: { ticks: { color: '#9fb0c7' }, grid: { color: 'rgba(255,255,255,0.06)' } },
        y: { ticks: { color: '#9fb0c7' }, grid: { color: 'rgba(255,255,255,0.06)' } },
      },
    },
  });
}

