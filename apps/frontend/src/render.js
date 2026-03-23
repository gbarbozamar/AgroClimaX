function textOrDash(value, suffix = '') {
  if (value === null || value === undefined || Number.isNaN(value)) return '—';
  return `${value}${suffix}`;
}

function fixed(value, digits = 1, suffix = '') {
  if (value === null || value === undefined || Number.isNaN(value)) return '—';
  return `${Number(value).toFixed(digits)}${suffix}`;
}

export function populateDepartmentSelect(units, selected = 'nacional') {
  const select = document.getElementById('department-select');
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
      extra: 'Análisis por parcela custom',
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
        soil: 'Pendiente de agregación',
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
    action: data.actionable ? 'Señal accionable por cobertura y conectividad.' : 'Seguir monitoreando evolución.',
    extra: `Calibración ${data.calibration_ref || 'N/D'} · ${data.data_mode || 'sin modo'} · cluster ${fixed(data.largest_cluster_pct, 1, '%')}`,
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
      unit: data.scope === 'nacional' ? 'Departamentos + H3 r9' : 'Departamento / H3 r9',
      soil: data.soil_context?.texture ? `${data.soil_context.texture} · AWC ${fixed(data.soil_context.water_holding_capacity_mm, 0, ' mm')}` : 'Sin detalle',
      forecast: data.forecast?.length ? `7 dias · max riesgo ${fixed(Math.max(...data.forecast.map((item) => item.expected_risk || 0)), 0)}` : 'Sin pronóstico',
      mode: data.data_mode || 'N/D',
    },
    chartSeries: context.history || [],
    alertHistory: context.history || [],
    unitLat: context.unitLat ?? null,
    unitLon: context.unitLon ?? null,
  };
}

export function renderLoading(message = 'Cargando tablero...') {
  const banner = document.getElementById('alerta-banner');
  banner.innerHTML = `<div class="banner-dot" style="background:#4a90d9;animation:pulse 1s infinite"></div><strong style="color:#4a90d9">${message}</strong>`;
  banner.style.background = '#4a90d922';
  banner.style.borderBottom = '1px solid #4a90d944';
}

export function renderError(message) {
  const banner = document.getElementById('alerta-banner');
  banner.innerHTML = `<span style="color:#e74c3c">${message}</span>`;
  banner.style.background = '#e74c3c22';
  banner.style.borderBottom = '1px solid #e74c3c44';
}

export function renderDashboard(model) {
  const banner = document.getElementById('alerta-banner');
  banner.style.background = `${model.color}22`;
  banner.style.borderBottom = `1px solid ${model.color}55`;
  banner.innerHTML = `<div class="banner-dot" style="background:${model.color}"></div><strong>${model.title}</strong> — ${model.explanation}<span style="margin-left:auto;color:var(--text-muted);font-size:0.78rem">${model.scopeLabel}</span>`;

  document.getElementById('scope-badge-value').textContent = model.scopeLabel;
  document.getElementById('alerta-nivel-text').textContent = `● ${model.title}`;
  document.getElementById('alerta-tipo-badge').textContent = `${model.scope || 'unidad'} · ${model.dataMode}`;
  document.getElementById('alerta-descripcion').textContent = model.explanation;
  document.getElementById('alerta-accion').textContent = model.action;
  document.getElementById('alerta-extra').textContent = model.extra;
  document.getElementById('last-update').textContent = `Actualizado: ${new Date().toLocaleString()} · Fuente API ${model.scope === 'unidad' ? 'legacy/custom' : 'v1'}`;

  document.getElementById('kpi-humedad').textContent = fixed(model.humidity, 1, '%');
  document.getElementById('kpi-ndmi').textContent = fixed(model.ndmi, 3);
  document.getElementById('kpi-spi').textContent = fixed(model.spi, 2);
  document.getElementById('kpi-area').textContent = fixed(model.affectedPct, 1, '%');
  document.getElementById('kpi-risk').textContent = fixed(model.riskScore, 1);
  document.getElementById('kpi-confidence').textContent = fixed(model.confidenceScore, 1);
  document.getElementById('kpi-dias').textContent = textOrDash(model.daysInState);

  document.getElementById('hum-s1-pct').textContent = fixed(model.humidity, 1, '%');
  document.getElementById('hum-ndmi-pct').textContent = fixed(model.ndmi, 3);
  document.getElementById('hum-s1-bar').style.width = `${Math.max(0, Math.min(100, model.humidity || 0))}%`;
  document.getElementById('hum-ndmi-bar').style.width = `${Math.max(0, Math.min(100, ((model.ndmi ?? -0.5) + 0.5) * 100))}%`;

  document.getElementById('spi-big').textContent = fixed(model.spi, 2);
  document.getElementById('spi-cat').textContent = model.spi === null ? 'Sin datos' : (model.spi < -1.5 ? 'Seco severo' : model.spi < -1 ? 'Seco' : model.spi < 1 ? 'Normal' : 'Humedo');
  const spiMarker = document.getElementById('spi-marker');
  const spiPosition = model.spi === null ? 50 : Math.max(0, Math.min(100, ((model.spi + 3) / 6) * 100));
  spiMarker.style.left = `${spiPosition}%`;

  document.getElementById('indicador-calibracion').textContent = model.calibrationRef;
  document.getElementById('indicador-suelo').textContent = model.technical.soil;
  document.getElementById('indicador-forecast').textContent = model.technical.forecast;
  document.getElementById('indicador-mode').textContent = model.technical.mode;
}

export function renderDrivers(model) {
  const container = document.getElementById('drivers-list');
  if (!container) return;
  if (!model.drivers.length) {
    container.innerHTML = '<div style="color:var(--text-muted)">Sin drivers disponibles para esta vista.</div>';
    return;
  }
  container.innerHTML = model.drivers
    .map((driver) => `<div style="padding:10px;border:1px solid var(--border);border-radius:10px;background:rgba(17,23,35,0.55)"><div style="display:flex;justify-content:space-between"><strong>${driver.name}</strong><span style="color:var(--accent)">${fixed(driver.score, 1)}</span></div><div style="margin-top:4px;color:var(--text-muted)">${driver.detail || ''}</div></div>`)
    .join('');
}

export function renderForecast(model) {
  const container = document.getElementById('forecast-list');
  if (!container) return;
  if (!model.forecast.length) {
    container.innerHTML = '<div style="color:var(--text-muted)">Sin forecast disponible.</div>';
    return;
  }
  container.innerHTML = model.forecast
    .slice(0, 7)
    .map((day) => `<div style="display:grid;grid-template-columns:82px 1fr auto;gap:10px;align-items:center;padding:8px 10px;border-radius:10px;background:rgba(17,23,35,0.55);border:1px solid var(--border)"><strong>${day.date.slice(5)}</strong><span style="color:var(--text-muted)">Lluvia ${fixed(day.precip_mm, 1, ' mm')} · ET0 ${fixed(day.et0_mm, 1, ' mm')}</span><span style="color:${(day.expected_risk || 0) >= 60 ? '#e74c3c' : (day.expected_risk || 0) >= 40 ? '#e67e22' : '#2ecc71'}">${fixed(day.expected_risk, 0)}</span></div>`)
    .join('');
}

export function renderHistory(model) {
  const history = model.alertHistory || [];
  const alertsContainer = document.getElementById('alertas-recientes');
  if (!history.length) {
    alertsContainer.innerHTML = '<div style="color:var(--text-muted);font-size:0.8rem">Sin historial reciente.</div>';
    return;
  }
  alertsContainer.innerHTML = history
    .slice(0, 5)
    .map((item) => `<div class="alerta-item"><div class="alerta-item-dot" style="background:${item.state_level >= 3 ? '#e74c3c' : item.state_level >= 2 ? '#e67e22' : '#f1c40f'}"></div><span>${item.state || 'Dato'} · riesgo ${fixed(item.risk_score, 0)}</span><span class="alerta-item-fecha">${(item.fecha || '').slice(5)}</span></div>`)
    .join('');
}

export function renderChart(model, chartRef) {
  if (!window.Chart) return chartRef;
  const history = model.chartSeries || [];
  const ctx = document.getElementById('chart-humedad').getContext('2d');
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
          label: 'Área afectada %',
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
