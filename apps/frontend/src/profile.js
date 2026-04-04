import { fetchProfileMe, profilePageUrl, saveProfileMe } from './api.js?v=20260404-8';
import { setStore, store } from './state.js?v=20260404-8';
import { setSidebarView, syncSidebarView } from './settings.js?v=20260331-1';

function getNode(id) {
  return document.getElementById(id);
}

function escapeHtml(value) {
  return String(value ?? '')
    .replaceAll('&', '&amp;')
    .replaceAll('<', '&lt;')
    .replaceAll('>', '&gt;')
    .replaceAll('"', '&quot;')
    .replaceAll("'", '&#39;');
}

function clone(value) {
  return JSON.parse(JSON.stringify(value ?? {}));
}

function toNullableNumber(value) {
  if (value === '' || value === null || value === undefined) return null;
  const numeric = Number(value);
  return Number.isFinite(numeric) ? numeric : null;
}

function serializeDraftForSave(profile = {}) {
  const draft = clone(profile);
  draft.operation_size_hectares = toNullableNumber(draft.operation_size_hectares);
  draft.livestock_headcount = toNullableNumber(draft.livestock_headcount);
  return draft;
}

function normalizeDraft(profile = {}) {
  return {
    phone_e164: profile.phone_e164 || '',
    whatsapp_e164: profile.whatsapp_e164 || '',
    organization_name: profile.organization_name || '',
    organization_type: profile.organization_type || '',
    role_code: profile.role_code || '',
    job_title: profile.job_title || '',
    scope_type: profile.scope_type || '',
    scope_ids_json: Array.isArray(profile.scope_ids_json) ? [...profile.scope_ids_json] : [],
    production_type: profile.production_type || '',
    operation_size_hectares: profile.operation_size_hectares ?? '',
    livestock_headcount: profile.livestock_headcount ?? '',
    crop_types_json: Array.isArray(profile.crop_types_json) ? [...profile.crop_types_json] : [],
    use_cases_json: Array.isArray(profile.use_cases_json) ? [...profile.use_cases_json] : [],
    alert_channels_json: Array.isArray(profile.alert_channels_json) ? [...profile.alert_channels_json] : [],
    min_alert_state: profile.min_alert_state || 'Alerta',
    preferred_language: profile.preferred_language || 'es-UY',
    communications_opt_in: Boolean(profile.communications_opt_in),
    data_usage_consent: Boolean(profile.data_usage_consent_at),
  };
}

function profileOptions() {
  return store.profilePayload?.options || {};
}

function setStatus(message, tone = 'muted') {
  const node = getNode('profile-status');
  if (!node) return;
  node.textContent = message;
  node.dataset.tone = tone;
}

function formatDate(value) {
  if (!value) return '-';
  try {
    return new Date(value).toLocaleString('es-UY');
  } catch {
    return value;
  }
}

function profileBannerStorageKey() {
  const email = store.authUser?.email || store.profilePayload?.google_identity?.email || 'anon';
  return `agroclimax:profile-banner-dismissed:${email}`;
}

function profileBannerDismissToken(completion = {}) {
  const pct = Number(completion.completion_pct || 0).toFixed(0);
  const missing = Array.isArray(completion.missing_fields) ? completion.missing_fields.join('|') : '';
  return `${pct}:${missing}`;
}

function getDismissedProfileBannerToken() {
  try {
    return window.localStorage.getItem(profileBannerStorageKey()) || '';
  } catch {
    return '';
  }
}

function setDismissedProfileBannerToken(token) {
  try {
    window.localStorage.setItem(profileBannerStorageKey(), token);
  } catch {}
}

function clearDismissedProfileBannerToken() {
  try {
    window.localStorage.removeItem(profileBannerStorageKey());
  } catch {}
}

function dismissProfileBanner() {
  const completion = store.profilePayload?.completion || store.profileStatus;
  if (!completion) return;
  setDismissedProfileBannerToken(profileBannerDismissToken(completion));
  getNode('profile-completion-banner')?.classList.add('hidden');
}

function renderCompletionSummary() {
  const node = getNode('profile-completion-summary');
  if (!node) return;
  const completion = store.profilePayload?.completion || store.profileStatus || { completion_pct: 0, is_complete: false, missing_fields: [] };
  const missing = Array.isArray(completion.missing_fields) ? completion.missing_fields : [];
  node.innerHTML = `
    <div class="profile-summary-top">
      <strong>Perfil ${completion.is_complete ? 'completo' : 'incompleto'}</strong>
      <span>${Number(completion.completion_pct || 0).toFixed(0)}%</span>
    </div>
    <div class="profile-progress">
      <div class="profile-progress-bar" style="width:${Math.max(0, Math.min(100, Number(completion.completion_pct || 0)))}%"></div>
    </div>
    <div class="profile-summary-copy">
      ${completion.is_complete
        ? 'El usuario ya completo el cuestionario operativo.'
        : `Faltan datos para cerrar el onboarding: ${escapeHtml(missing.slice(0, 3).join(', ') || 'revisar perfil')}${missing.length > 3 ? '...' : ''}`}
    </div>
  `;
}

function renderCompletionBanner() {
  const banner = getNode('profile-completion-banner');
  const copy = getNode('profile-completion-copy');
  const pct = getNode('profile-completion-pct');
  if (!banner || !copy || !pct) return;
  const completion = store.profilePayload?.completion || store.profileStatus;
  if (!store.authUser || !completion || completion.is_complete) {
    if (completion?.is_complete) clearDismissedProfileBannerToken();
    banner.classList.add('hidden');
    return;
  }
  const dismissToken = profileBannerDismissToken(completion);
  if (getDismissedProfileBannerToken() === dismissToken) {
    banner.classList.add('hidden');
    return;
  }
  pct.textContent = `${Number(completion.completion_pct || 0).toFixed(0)}%`;
  copy.textContent = 'Completa el perfil operativo para personalizar cobertura, alertas y contexto de uso.';
  banner.classList.remove('hidden');
}

function renderIdentity() {
  const node = getNode('profile-google-identity');
  if (!node) return;
  const identity = store.profilePayload?.google_identity || {};
  const avatar = identity.picture_url
    ? `<img class="profile-identity-avatar" src="${escapeHtml(identity.picture_url)}" alt="Avatar Google" />`
    : '<div class="profile-identity-avatar profile-identity-avatar-fallback">G</div>';
  node.innerHTML = `
    <div class="profile-identity-head">
      ${avatar}
      <div>
        <div class="profile-identity-name">${escapeHtml(identity.full_name || identity.email || 'Usuario')}</div>
        <div class="profile-identity-email">${escapeHtml(identity.email || '-')}</div>
      </div>
    </div>
    <div class="profile-identity-grid">
      <div><strong>Google Sub</strong><span>${escapeHtml(identity.google_sub || '-')}</span></div>
      <div><strong>Email verificado</strong><span>${identity.email_verified ? 'Si' : 'No'}</span></div>
      <div><strong>Given name</strong><span>${escapeHtml(identity.given_name || '-')}</span></div>
      <div><strong>Family name</strong><span>${escapeHtml(identity.family_name || '-')}</span></div>
      <div><strong>Locale</strong><span>${escapeHtml(identity.locale || '-')}</span></div>
      <div><strong>Ultimo login</strong><span>${escapeHtml(formatDate(identity.last_login_at))}</span></div>
      <div><strong>Alta cuenta</strong><span>${escapeHtml(formatDate(identity.created_at))}</span></div>
      <div><strong>Estado</strong><span>${identity.is_active ? 'Activa' : 'Inactiva'}</span></div>
    </div>
  `;
}

function buildSelectOptions(options, selectedValue, placeholder = 'Seleccionar...') {
  const items = [`<option value="">${placeholder}</option>`];
  (options || []).forEach((option) => {
    const selected = option.value === selectedValue ? ' selected' : '';
    items.push(`<option value="${escapeHtml(option.value)}"${selected}>${escapeHtml(option.label)}</option>`);
  });
  return items.join('');
}

function buildCheckboxGroup(name, options, selectedValues) {
  const selected = new Set(selectedValues || []);
  return `
    <div class="profile-checkbox-group">
      ${(options || []).map((option) => `
        <label class="profile-checkbox">
          <input type="checkbox" data-array-field="${name}" value="${escapeHtml(option.value)}" ${selected.has(option.value) ? 'checked' : ''} />
          <span>${escapeHtml(option.label)}</span>
        </label>
      `).join('')}
    </div>
  `;
}

function buildScopeSelect(draft, options) {
  if (draft.scope_type === 'nacional') {
    return '<div class="profile-scope-note">Cobertura nacional seleccionada. No requiere detalle adicional.</div>';
  }
  const items = draft.scope_type === 'jurisdiccion'
    ? (options.jurisdictions || []).reduce((groups, item) => {
      const key = item.department || 'Sin departamento';
      if (!groups[key]) groups[key] = [];
      groups[key].push(item);
      return groups;
    }, {})
    : null;

  if (draft.scope_type === 'jurisdiccion') {
    return `
      <select class="settings-input profile-multiselect" id="profile-scope-ids" multiple size="8">
        ${Object.entries(items).map(([department, values]) => `
          <optgroup label="${escapeHtml(department)}">
            ${values.map((item) => `<option value="${escapeHtml(item.id)}" ${draft.scope_ids_json.includes(item.id) ? 'selected' : ''}>${escapeHtml(item.label)}</option>`).join('')}
          </optgroup>
        `).join('')}
      </select>
    `;
  }
  return `
    <select class="settings-input profile-multiselect" id="profile-scope-ids" multiple size="8">
      ${(options.departments || []).map((item) => `<option value="${escapeHtml(item.id)}" ${draft.scope_ids_json.includes(item.id) ? 'selected' : ''}>${escapeHtml(item.label)}</option>`).join('')}
    </select>
  `;
}

function renderForm() {
  const node = getNode('profile-form');
  if (!node) return;
  const draft = store.profileDraft || normalizeDraft(store.profilePayload?.profile);
  const options = profileOptions();
  const updatedAt = store.profilePayload?.profile?.updated_at;
  node.innerHTML = `
    <div class="settings-section-card">
      <div class="settings-section-title">Datos personales</div>
      <div class="settings-grid">
        <label class="settings-field">
          <span class="settings-field-label">Telefono</span>
          <input class="settings-input" data-field="phone_e164" value="${escapeHtml(draft.phone_e164)}" placeholder="+59899111222" />
          <span class="settings-field-help">Formato E.164 para SMS.</span>
        </label>
        <label class="settings-field">
          <span class="settings-field-label">WhatsApp</span>
          <input class="settings-input" data-field="whatsapp_e164" value="${escapeHtml(draft.whatsapp_e164)}" placeholder="+59899111222" />
          <span class="settings-field-help">Formato E.164 para notificaciones por WhatsApp.</span>
        </label>
        <label class="settings-field">
          <span class="settings-field-label">Cargo</span>
          <input class="settings-input" data-field="job_title" value="${escapeHtml(draft.job_title)}" placeholder="Ej. Director tecnico" />
        </label>
        <label class="settings-field">
          <span class="settings-field-label">Idioma preferido</span>
          <select class="settings-input" data-field="preferred_language">${buildSelectOptions(options.preferred_languages, draft.preferred_language)}</select>
        </label>
      </div>
    </div>

    <div class="settings-section-card">
      <div class="settings-section-title">Organizacion</div>
      <div class="settings-grid">
        <label class="settings-field">
          <span class="settings-field-label">Nombre</span>
          <input class="settings-input" data-field="organization_name" value="${escapeHtml(draft.organization_name)}" placeholder="Empresa, organismo o establecimiento" />
        </label>
        <label class="settings-field">
          <span class="settings-field-label">Tipo</span>
          <select class="settings-input" data-field="organization_type">${buildSelectOptions(options.organization_types, draft.organization_type)}</select>
        </label>
        <label class="settings-field">
          <span class="settings-field-label">Rol</span>
          <select class="settings-input" data-field="role_code">${buildSelectOptions(options.role_codes, draft.role_code)}</select>
        </label>
      </div>
    </div>

    <div class="settings-section-card">
      <div class="settings-section-title">Cobertura operativa</div>
      <div class="settings-grid">
        <label class="settings-field">
          <span class="settings-field-label">Ambito</span>
          <select class="settings-input" data-field="scope_type">${buildSelectOptions(options.scope_types, draft.scope_type)}</select>
        </label>
        <div class="settings-field settings-field-wide">
          <span class="settings-field-label">Seleccion</span>
          ${buildScopeSelect(draft, options)}
          <span class="settings-field-help">Seleccion multiple segun el ambito elegido.</span>
        </div>
      </div>
    </div>

    <div class="settings-section-card">
      <div class="settings-section-title">Perfil productivo</div>
      <div class="settings-grid">
        <label class="settings-field">
          <span class="settings-field-label">Tipo principal</span>
          <select class="settings-input" data-field="production_type">${buildSelectOptions(options.production_types, draft.production_type)}</select>
        </label>
        <label class="settings-field">
          <span class="settings-field-label">Superficie (ha)</span>
          <input class="settings-input" data-field="operation_size_hectares" type="number" min="0" step="0.1" value="${escapeHtml(draft.operation_size_hectares)}" />
        </label>
        <label class="settings-field">
          <span class="settings-field-label">Cabezas de ganado</span>
          <input class="settings-input" data-field="livestock_headcount" type="number" min="0" step="1" value="${escapeHtml(draft.livestock_headcount)}" />
        </label>
        <label class="settings-field">
          <span class="settings-field-label">Cultivos principales</span>
          <input class="settings-input" data-field="crop_types_json" value="${escapeHtml((draft.crop_types_json || []).join(', '))}" placeholder="maiz, soja, pasturas" />
          <span class="settings-field-help">Separados por coma.</span>
        </label>
      </div>
    </div>

    <div class="settings-section-card">
      <div class="settings-section-title">Uso de la plataforma</div>
      ${buildCheckboxGroup('use_cases_json', options.use_cases, draft.use_cases_json)}
    </div>

    <div class="settings-section-card">
      <div class="settings-section-title">Alertas y contacto</div>
      <div class="settings-grid">
        <div class="settings-field settings-field-wide">
          <span class="settings-field-label">Canales</span>
          ${buildCheckboxGroup('alert_channels_json', options.alert_channels, draft.alert_channels_json)}
        </div>
        <label class="settings-field">
          <span class="settings-field-label">Severidad minima</span>
          <select class="settings-input" data-field="min_alert_state">${buildSelectOptions(options.min_alert_states, draft.min_alert_state)}</select>
        </label>
      </div>
    </div>

    <div class="settings-section-card">
      <div class="settings-section-title">Consentimiento</div>
      <label class="profile-checkbox">
        <input type="checkbox" data-field="data_usage_consent" ${draft.data_usage_consent ? 'checked' : ''} />
        <span>Acepto el uso operativo de mis datos dentro de AgroClimaX.</span>
      </label>
      <label class="profile-checkbox">
        <input type="checkbox" data-field="communications_opt_in" ${draft.communications_opt_in ? 'checked' : ''} />
        <span>Acepto recibir comunicaciones operativas y actualizaciones de la plataforma.</span>
      </label>
    </div>

    <div class="profile-form-footer">
      <div class="profile-form-meta">Ultima actualizacion: ${escapeHtml(formatDate(updatedAt))}</div>
      <button class="settings-btn primary" id="profile-save-btn" type="button">Guardar perfil</button>
    </div>
  `;

  wireFormEvents();
}

function updateDraftField(field, value) {
  const draft = clone(store.profileDraft || normalizeDraft(store.profilePayload?.profile));
  draft[field] = value;
  setStore({ profileDraft: draft });
}

function wireFormEvents() {
  const form = getNode('profile-form');
  if (!form) return;

  form.querySelectorAll('[data-field]').forEach((node) => {
    node.addEventListener('change', (event) => {
      const { field } = event.target.dataset;
      let value;
      if (event.target.type === 'checkbox') {
        value = Boolean(event.target.checked);
      } else if (field === 'operation_size_hectares' || field === 'livestock_headcount') {
        value = event.target.value === '' ? '' : Number(event.target.value);
      } else if (field === 'crop_types_json') {
        value = event.target.value.split(',').map((item) => item.trim()).filter(Boolean);
      } else {
        value = event.target.value;
      }
      updateDraftField(field, value);
      if (field === 'scope_type') {
        updateDraftField('scope_ids_json', []);
        renderForm();
      }
    });
  });

  form.querySelectorAll('[data-array-field]').forEach((node) => {
    node.addEventListener('change', () => {
      const arrayField = node.dataset.arrayField;
      const selected = Array.from(form.querySelectorAll(`[data-array-field="${arrayField}"]:checked`)).map((item) => item.value);
      updateDraftField(arrayField, selected);
    });
  });

  const scopeSelect = getNode('profile-scope-ids');
  scopeSelect?.addEventListener('change', () => {
    const selected = Array.from(scopeSelect.selectedOptions).map((option) => option.value);
    updateDraftField('scope_ids_json', selected);
  });

  getNode('profile-save-btn')?.addEventListener('click', async () => {
    try {
      await persistProfile();
    } catch (error) {
      setStatus(`No se pudo guardar el perfil: ${error.message}`, 'error');
    }
  });
}

async function persistProfile() {
  const draft = serializeDraftForSave(store.profileDraft || normalizeDraft(store.profilePayload?.profile));
  setStatus('Guardando perfil de usuario...', 'info');
  const payload = await saveProfileMe(draft);
  setStore({
    profilePayload: payload,
    profileDraft: normalizeDraft(payload.profile),
    profileStatus: payload.completion,
    authSession: store.authSession ? { ...store.authSession, profile_status: payload.completion } : store.authSession,
  });
  renderCompletionBanner();
  renderProfilePanel();
  setStatus('Perfil guardado.', 'success');
}

export async function refreshProfilePanel() {
  const payload = await fetchProfileMe();
  setStore({
    profilePayload: payload,
    profileDraft: normalizeDraft(payload.profile),
    profileStatus: payload.completion,
  });
  renderCompletionBanner();
  renderProfilePanel();
}

export function renderProfilePanel() {
  renderCompletionSummary();
  renderIdentity();
  renderForm();
}

export function initProfilePanel() {
  const profileTab = getNode('sidebar-profile-tab');
  const profileBanner = getNode('profile-completion-banner');
  const profileBannerButton = getNode('profile-completion-open-btn');
  const profileBannerCloseButton = getNode('profile-completion-close-btn');
  const profileOpenFullButton = getNode('profile-open-full-btn');
  const openProfilePage = () => {
    window.location.assign(profilePageUrl());
  };
  profileTab?.addEventListener('click', async () => {
    setSidebarView('profile');
    if (!store.profilePayload) {
      await refreshProfilePanel();
    }
  });
  profileBanner?.addEventListener('click', (event) => {
    if (event.target instanceof HTMLElement && event.target.closest('button')) return;
    openProfilePage();
  });
  profileBanner?.addEventListener('keydown', (event) => {
    if (event.key === 'Enter' || event.key === ' ') {
      event.preventDefault();
      openProfilePage();
    }
  });
  profileBannerButton?.addEventListener('click', (event) => {
    event.preventDefault();
    openProfilePage();
  });
  profileBannerCloseButton?.addEventListener('click', (event) => {
    event.preventDefault();
    event.stopPropagation();
    dismissProfileBanner();
  });
  profileOpenFullButton?.addEventListener('click', () => {
    window.open(profilePageUrl(), '_blank', 'noopener,noreferrer');
  });
  window.addEventListener('agroclimax:open-profile', async () => {
    setSidebarView('profile');
    if (!store.profilePayload) {
      await refreshProfilePanel();
    }
  });
  syncSidebarView();
  renderCompletionBanner();
}
