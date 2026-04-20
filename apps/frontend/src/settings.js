import {
  clearCoverageOverride,
  fetchSettings,
  fetchSettingsAudit,
  fetchSettingsSchema,
  resetGlobalSettings,
  saveCoverageSettings,
  saveGlobalSettings,
} from './api.js?v=20260420-4';
import { setStore, store } from './state.js?v=20260420-4';

function clone(value) {
  return JSON.parse(JSON.stringify(value ?? {}));
}

function getNode(id) {
  return document.getElementById(id);
}

function setStatus(message, tone = 'muted') {
  const node = getNode('settings-status');
  if (!node) return;
  node.textContent = message;
  node.dataset.tone = tone;
}

function formatDate(value) {
  if (!value) return '—';
  try {
    return new Date(value).toLocaleString('es-UY');
  } catch {
    return value;
  }
}

function getByPath(payload, path) {
  return path.split('.').reduce((current, key) => (current && typeof current === 'object' ? current[key] : undefined), payload);
}

function setByPath(payload, path, value) {
  const parts = path.split('.');
  let cursor = payload;
  for (let index = 0; index < parts.length - 1; index += 1) {
    const key = parts[index];
    if (!cursor[key] || typeof cursor[key] !== 'object') cursor[key] = {};
    cursor = cursor[key];
  }
  cursor[parts[parts.length - 1]] = value;
}

function activeCoverageClass() {
  const configured = store.settingsCoverageClass;
  if (configured) return configured;
  const first = store.settingsSchema?.coverage_classes?.[0]?.key || 'pastura_cultivo';
  setStore({ settingsCoverageClass: first });
  return first;
}

function currentBaseRules() {
  return store.settingsPayload?.global || {};
}

function buildDraft(payload = store.settingsPayload) {
  if (!payload) return {};
  if (store.settingsMode === 'coverage') {
    const coverageClass = activeCoverageClass();
    return clone(payload.effective_by_coverage?.[coverageClass] || payload.global || {});
  }
  return clone(payload.global || {});
}

function ensureDraft() {
  if (!store.settingsDraft) {
    setStore({ settingsDraft: buildDraft() });
  }
  return store.settingsDraft;
}

export function syncSidebarView() {
  const monitorView = getNode('sidebar-monitor-view');
  const settingsView = getNode('sidebar-settings-view');
  const profileView = getNode('sidebar-profile-view');
  const fieldsView = getNode('sidebar-fields-view');
  const monitorTab = getNode('sidebar-monitor-tab');
  const settingsTab = getNode('sidebar-settings-tab');
  const profileTab = getNode('sidebar-profile-tab');
  const fieldsTab = getNode('sidebar-fields-tab');
  const activeView = store.sidebarView || 'monitor';
  monitorView?.classList.toggle('hidden', activeView !== 'monitor');
  settingsView?.classList.toggle('hidden', activeView !== 'settings');
  profileView?.classList.toggle('hidden', activeView !== 'profile');
  fieldsView?.classList.toggle('hidden', activeView !== 'fields');
  monitorTab?.classList.toggle('active', activeView === 'monitor');
  settingsTab?.classList.toggle('active', activeView === 'settings');
  profileTab?.classList.toggle('active', activeView === 'profile');
  fieldsTab?.classList.toggle('active', activeView === 'fields');
}

export function setSidebarView(view) {
  setStore({ sidebarView: view });
  syncSidebarView();
}

function renderCoverageOptions() {
  const select = getNode('settings-coverage-select');
  if (!select || !store.settingsSchema) return;
  const currentValue = activeCoverageClass();
  select.innerHTML = (store.settingsSchema.coverage_classes || [])
    .map((item) => `<option value="${item.key}">${item.label}</option>`)
    .join('');
  select.value = currentValue;
  select.disabled = store.settingsMode !== 'coverage';
}

function renderMeta() {
  const meta = getNode('settings-meta');
  const operatorNode = getNode('settings-authenticated-operator');
  if (!meta || !store.settingsPayload) return;
  const coverageClass = activeCoverageClass();
  const override = store.settingsPayload.overrides?.[coverageClass];
  if (operatorNode) {
    operatorNode.textContent = store.authUser?.email || 'Sin sesion activa';
  }
  meta.innerHTML = `
    <div><strong>Version global:</strong> ${store.settingsPayload.global_version ?? 0}</div>
    <div><strong>Actualizado:</strong> ${formatDate(store.settingsPayload.global_updated_at)}</div>
    <div><strong>Operador:</strong> ${store.settingsPayload.global_updated_by_label || '-'}</div>
    <div><strong>Usuario activo:</strong> ${store.authUser?.email || '-'}</div>
    <div><strong>Modo:</strong> ${store.settingsMode === 'coverage' ? `Override ${coverageClass}` : 'Global'}</div>
    <div><strong>Version efectiva:</strong> ${store.settingsPayload.rules_version || '-'}</div>
    ${store.settingsMode === 'coverage' ? `<div><strong>Override actual:</strong> ${override?.version ? `v${override.version}` : 'heredado'}</div>` : ''}
  `;
}

function fieldValueMarkup(field, value) {
  if (field.type === 'json') {
    return `<textarea class="settings-input settings-json" data-path="${field.path}" data-type="${field.type}">${JSON.stringify(value ?? {}, null, 2)}</textarea>`;
  }
  const safeValue = value ?? '';
  const min = field.min !== undefined ? `min="${field.min}"` : '';
  const max = field.max !== undefined ? `max="${field.max}"` : '';
  const step = field.step !== undefined ? `step="${field.step}"` : '';
  const type = field.type === 'number' ? 'number' : 'text';
  return `<input class="settings-input" data-path="${field.path}" data-type="${field.type}" type="${type}" value="${safeValue}" ${min} ${max} ${step} />`;
}

function renderForm() {
  const container = getNode('settings-form');
  if (!container || !store.settingsSchema || !store.settingsPayload) return;
  const draft = ensureDraft();
  const baseRules = currentBaseRules();
  const isCoverageMode = store.settingsMode === 'coverage';

  container.innerHTML = (store.settingsSchema.schema.sections || []).map((section) => {
    const fields = (section.fields || []).map((field) => {
      const value = getByPath(draft, field.path);
      const inherited = isCoverageMode && JSON.stringify(value) === JSON.stringify(getByPath(baseRules, field.path));
      return `
        <label class="settings-field ${field.type === 'json' ? 'settings-field-wide' : ''}">
          <span class="settings-field-label">
            ${field.label}
            <span class="${inherited ? 'settings-inherited' : 'settings-override'}">${isCoverageMode ? (inherited ? 'heredado' : 'override') : 'global'}</span>
          </span>
          ${fieldValueMarkup(field, value)}
          <span class="settings-field-help">${field.help || ''}${field.unit ? ` · ${field.unit}` : ''}</span>
        </label>
      `;
    }).join('');

    return `
      <section class="settings-section-card">
        <div class="settings-section-title">${section.title}</div>
        <div class="settings-section-copy">${section.description || ''}</div>
        <div class="settings-grid">${fields}</div>
      </section>
    `;
  }).join('');

  container.querySelectorAll('[data-path]').forEach((fieldNode) => {
    fieldNode.addEventListener('change', () => {
      const path = fieldNode.dataset.path;
      const type = fieldNode.dataset.type;
      const draftPayload = clone(ensureDraft());
      try {
        let parsedValue = fieldNode.value;
        if (type === 'number') {
          parsedValue = fieldNode.value === '' ? null : Number(fieldNode.value);
          if (Number.isNaN(parsedValue)) throw new Error('Valor numerico invalido');
        } else if (type === 'json') {
          parsedValue = fieldNode.value.trim() ? JSON.parse(fieldNode.value) : {};
        }
        setByPath(draftPayload, path, parsedValue);
        setStore({ settingsDraft: draftPayload });
        fieldNode.classList.remove('invalid');
        setStatus('Borrador actualizado. Recorda guardar para aplicar los cambios.', 'info');
      } catch (error) {
        fieldNode.classList.add('invalid');
        setStatus(`No se pudo interpretar ${path}: ${error.message}`, 'error');
      }
    });
  });
}

function renderAudit() {
  const container = getNode('settings-audit');
  if (!container) return;
  const rows = store.settingsAudit || [];
  if (!rows.length) {
    container.innerHTML = '<div class="settings-audit-empty">Todavia no hay cambios auditados.</div>';
    return;
  }
  container.innerHTML = rows.map((row) => `
    <article class="settings-audit-item">
      <div class="settings-audit-head">
        <strong>${row.scope_type}:${row.scope_key}</strong>
        <span>${row.action}</span>
      </div>
      <div class="settings-audit-meta">
        v${row.version_before ?? '—'} -> v${row.version_after ?? '—'} · ${row.updated_by_label || 'sin operador'} · ${formatDate(row.created_at)}
      </div>
    </article>
  `).join('');
}

function syncControls() {
  const modeSelect = getNode('settings-mode-select');
  const coverageSelect = getNode('settings-coverage-select');
  const clearOverrideButton = getNode('settings-clear-override-btn');
  if (modeSelect) modeSelect.value = store.settingsMode;
  if (coverageSelect) coverageSelect.value = activeCoverageClass();
  renderCoverageOptions();
  if (clearOverrideButton) clearOverrideButton.disabled = store.settingsMode !== 'coverage';
  renderMeta();
  renderForm();
  renderAudit();
  syncSidebarView();
}

async function refreshSettingsData() {
  setStatus('Cargando configuracion de negocio...', 'info');
  const [schema, payload, audit] = await Promise.all([
    fetchSettingsSchema(),
    fetchSettings(store.settingsMode === 'coverage' ? activeCoverageClass() : null),
    fetchSettingsAudit(20),
  ]);
  setStore({
    settingsSchema: schema,
    settingsPayload: payload,
    settingsAudit: audit?.datos || [],
  });
  setStore({ settingsDraft: buildDraft(payload) });
  syncControls();
  setStatus('Configuracion cargada.', 'success');
}

async function saveCurrentSettings(onRefreshSelection, onRefreshLayers) {
  const draft = ensureDraft();
  setStatus('Guardando cambios y recalculando reglas recientes...', 'info');
  if (store.settingsMode === 'coverage') {
    await saveCoverageSettings(activeCoverageClass(), draft);
  } else {
    await saveGlobalSettings(draft);
  }
  await refreshSettingsData();
  await onRefreshSelection?.();
  await onRefreshLayers?.();
  setStatus('Cambios aplicados y dashboard refrescado.', 'success');
}

async function resetCurrentSettings(onRefreshSelection, onRefreshLayers) {
  if (!window.confirm('Esto restaurara la configuracion global por defecto. ¿Continuar?')) return;
  setStatus('Restaurando defaults globales...', 'info');
  await resetGlobalSettings();
  setStore({ settingsMode: 'global' });
  await refreshSettingsData();
  await onRefreshSelection?.();
  await onRefreshLayers?.();
  setStatus('Defaults globales restaurados.', 'success');
}

async function clearCurrentOverride(onRefreshSelection, onRefreshLayers) {
  if (!window.confirm(`Se eliminara el override para ${activeCoverageClass()}. ¿Continuar?`)) return;
  setStatus('Eliminando override de cobertura...', 'info');
  await clearCoverageOverride(activeCoverageClass());
  await refreshSettingsData();
  await onRefreshSelection?.();
  await onRefreshLayers?.();
  setStatus('Override eliminado. La cobertura vuelve a heredar del global.', 'success');
}

export async function refreshSettingsPanel() {
  await refreshSettingsData();
}

export function initSettingsPanel({ onRefreshSelection, onRefreshLayers } = {}) {
  const monitorTab = getNode('sidebar-monitor-tab');
  const settingsTab = getNode('sidebar-settings-tab');
  const modeSelect = getNode('settings-mode-select');
  const coverageSelect = getNode('settings-coverage-select');
  const refreshButton = getNode('settings-refresh-btn');
  const saveButton = getNode('settings-save-btn');
  const resetButton = getNode('settings-reset-btn');
  const clearOverrideButton = getNode('settings-clear-override-btn');

  if (!monitorTab || !settingsTab) return;

  monitorTab.addEventListener('click', () => {
    setSidebarView('monitor');
  });
  settingsTab.addEventListener('click', async () => {
    setSidebarView('settings');
    if (!store.settingsPayload) {
      await refreshSettingsData();
    }
  });

  modeSelect?.addEventListener('change', async (event) => {
    setStore({
      settingsMode: event.target.value,
      settingsDraft: null,
    });
    await refreshSettingsData();
  });

  coverageSelect?.addEventListener('change', async (event) => {
    setStore({
      settingsCoverageClass: event.target.value,
      settingsDraft: null,
    });
    await refreshSettingsData();
  });

  refreshButton?.addEventListener('click', async () => {
    await refreshSettingsData();
  });

  saveButton?.addEventListener('click', async () => {
    try {
      await saveCurrentSettings(onRefreshSelection, onRefreshLayers);
    } catch (error) {
      setStatus(`No se pudieron guardar los cambios: ${error.message}`, 'error');
    }
  });

  resetButton?.addEventListener('click', async () => {
    try {
      await resetCurrentSettings(onRefreshSelection, onRefreshLayers);
    } catch (error) {
      setStatus(`No se pudo restaurar el global: ${error.message}`, 'error');
    }
  });

  clearOverrideButton?.addEventListener('click', async () => {
    try {
      await clearCurrentOverride(onRefreshSelection, onRefreshLayers);
    } catch (error) {
      setStatus(`No se pudo eliminar el override: ${error.message}`, 'error');
    }
  });

  syncSidebarView();
}
