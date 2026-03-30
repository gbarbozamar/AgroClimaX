import {
  deleteAlertSubscription,
  fetchAlertSubscriptionOptions,
  fetchAlertSubscriptions,
  fetchAuthMe,
  fetchProfileMe,
  googleLoginUrl,
  logoutCurrentUser,
  saveAlertSubscription,
  saveProfileMe,
  testAlertSubscription,
} from './api.js?v=20260330-1';
import { setStore } from './state.js?v=20260329-2';

const state = {
  authSession: null,
  authUser: null,
  profilePayload: null,
  profileDraft: null,
  isSaving: false,
  subscriptionOptions: null,
  subscriptions: [],
  subscriptionDraft: null,
  subscriptionSaving: false,
  subscriptionBusyId: null,
};

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

function defaultSubscriptionDraft() {
  return {
    id: null,
    scope_type: 'productive_unit',
    scope_id: '',
    channels_json: ['email'],
    min_alert_state: 'Alerta',
    active: true,
  };
}

function normalizeSubscriptionDraft(subscription = null) {
  if (!subscription) return defaultSubscriptionDraft();
  return {
    id: subscription.id || null,
    scope_type: subscription.scope_type || 'productive_unit',
    scope_id: subscription.scope_id || '',
    channels_json: Array.isArray(subscription.channels_json) ? [...subscription.channels_json] : [],
    min_alert_state: subscription.min_alert_state || 'Alerta',
    active: subscription.active !== false,
  };
}

function formatDate(value) {
  if (!value) return '-';
  try {
    return new Date(value).toLocaleString('es-UY');
  } catch {
    return value;
  }
}

function setStatus(message, tone = 'muted') {
  const node = getNode('profile-page-status');
  if (!node) return;
  node.textContent = message;
  node.dataset.tone = tone;
}

function setSubscriptionsStatus(message, tone = 'muted') {
  const node = getNode('profile-page-subscriptions-status');
  if (!node) return;
  node.textContent = message;
  node.dataset.tone = tone;
}

function setSavingState(isSaving) {
  state.isSaving = Boolean(isSaving);
  const button = getNode('profile-page-save-btn');
  if (!button) return;
  button.disabled = state.isSaving;
  button.textContent = state.isSaving ? 'Guardando...' : 'Guardar perfil';
}

function setSubscriptionSavingState(isSaving) {
  state.subscriptionSaving = Boolean(isSaving);
  const button = getNode('profile-page-subscription-save-btn');
  const reset = getNode('profile-page-subscription-reset-btn');
  if (button) {
    button.disabled = state.subscriptionSaving;
    button.textContent = state.subscriptionSaving ? 'Guardando...' : (state.subscriptionDraft?.id ? 'Actualizar alerta' : 'Crear alerta');
  }
  if (reset) reset.disabled = state.subscriptionSaving;
}

function formatRequestError(error) {
  if (!error) return 'Error desconocido';
  if (typeof error.message === 'string' && error.message.trim()) return error.message.trim();
  if (Array.isArray(error?.payload?.detail)) {
    return error.payload.detail
      .map((item) => {
        if (!item || typeof item !== 'object') return String(item);
        const field = Array.isArray(item.loc) ? item.loc.filter((value) => value !== 'body').join('.') : '';
        return field ? `${field}: ${item.msg || 'Valor invalido'}` : (item.msg || 'Valor invalido');
      })
      .join(' | ');
  }
  return 'Error desconocido';
}

function profileOptions() {
  return state.profilePayload?.options || {};
}

function subscriptionOptions() {
  return state.subscriptionOptions || {};
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
    <div class="check-grid">
      ${(options || []).map((option) => `
        <label class="check">
          <input type="checkbox" data-array-field="${name}" value="${escapeHtml(option.value)}" ${selected.has(option.value) ? 'checked' : ''} />
          <span>${escapeHtml(option.label)}</span>
        </label>
      `).join('')}
    </div>
  `;
}

function buildScopeSelect(draft, options) {
  if (draft.scope_type === 'nacional') {
    return '<div class="scope-note">Cobertura nacional seleccionada. No requiere detalle adicional.</div>';
  }

  if (draft.scope_type === 'jurisdiccion') {
    const groups = (options.jurisdictions || []).reduce((accumulator, item) => {
      const key = item.department || 'Sin departamento';
      if (!accumulator[key]) accumulator[key] = [];
      accumulator[key].push(item);
      return accumulator;
    }, {});

    return `
      <select class="select multiselect" id="profile-page-scope-ids" multiple size="10">
        ${Object.entries(groups).map(([department, items]) => `
          <optgroup label="${escapeHtml(department)}">
            ${items.map((item) => `<option value="${escapeHtml(item.id)}" ${draft.scope_ids_json.includes(item.id) ? 'selected' : ''}>${escapeHtml(item.label)}</option>`).join('')}
          </optgroup>
        `).join('')}
      </select>
    `;
  }

  return `
    <select class="select multiselect" id="profile-page-scope-ids" multiple size="10">
      ${(options.departments || []).map((item) => `<option value="${escapeHtml(item.id)}" ${draft.scope_ids_json.includes(item.id) ? 'selected' : ''}>${escapeHtml(item.label)}</option>`).join('')}
    </select>
  `;
}

function renderHeaderUser() {
  const wrap = getNode('profile-page-user');
  const avatar = getNode('profile-page-avatar');
  const fallback = getNode('profile-page-avatar-fallback');
  const name = getNode('profile-page-user-name');
  const email = getNode('profile-page-user-email');
  if (!wrap) return;
  if (!state.authUser) {
    wrap.classList.add('hidden');
    return;
  }
  wrap.classList.remove('hidden');
  name.textContent = state.authUser.full_name || state.authUser.email || 'Usuario';
  email.textContent = state.authUser.email || 'Cuenta Google';
  if (state.authUser.picture_url) {
    avatar.src = state.authUser.picture_url;
    avatar.classList.remove('hidden');
    fallback.classList.add('hidden');
  } else {
    avatar.classList.add('hidden');
    fallback.classList.remove('hidden');
  }
}

function renderAuthGate() {
  const gate = getNode('profile-page-auth-gate');
  const loginButton = getNode('profile-page-login-btn');
  const logoutButton = getNode('profile-page-logout-btn');
  if (!gate) return;
  const authenticated = Boolean(state.authUser);
  gate.classList.toggle('hidden', authenticated);
  loginButton?.classList.toggle('hidden', authenticated);
  logoutButton?.classList.toggle('hidden', !authenticated);
  document.body.classList.toggle('auth-blocked', !authenticated);
}

function renderSummary() {
  const completion = state.profilePayload?.completion || { completion_pct: 0, is_complete: false, missing_fields: [] };
  getNode('profile-page-completion-pct').textContent = `${Number(completion.completion_pct || 0).toFixed(0)}%`;
  getNode('profile-page-progress-bar').style.width = `${Math.max(0, Math.min(100, Number(completion.completion_pct || 0)))}%`;
  getNode('profile-page-summary-title').textContent = completion.is_complete ? 'Perfil completo' : 'Perfil incompleto';
  getNode('profile-page-summary-copy').textContent = completion.is_complete
    ? 'El cuestionario operativo ya esta completo para esta cuenta.'
    : 'Todavia faltan datos para definir contexto operativo, cobertura y canales.';
  const missingNode = getNode('profile-page-missing-list');
  missingNode.innerHTML = '';
  (completion.missing_fields || []).forEach((item) => {
    const pill = document.createElement('span');
    pill.className = 'pill';
    pill.textContent = item;
    missingNode.appendChild(pill);
  });
  if (!(completion.missing_fields || []).length) {
    const pill = document.createElement('span');
    pill.className = 'pill';
    pill.textContent = completion.is_complete ? 'Sin campos faltantes' : 'Sin detalle';
    missingNode.appendChild(pill);
  }
}

function renderIdentity() {
  const identity = state.profilePayload?.google_identity || {};
  const container = getNode('profile-page-identity');
  const avatar = identity.picture_url
    ? `<img class="identity-avatar" src="${escapeHtml(identity.picture_url)}" alt="Avatar Google" />`
    : '<div class="identity-avatar-fallback">G</div>';

  container.innerHTML = `
    <div class="identity-head">
      ${avatar}
      <div>
        <div class="identity-name">${escapeHtml(identity.full_name || identity.email || 'Usuario')}</div>
        <div class="identity-email">${escapeHtml(identity.email || '-')}</div>
      </div>
    </div>
    <div class="identity-grid">
      <div class="identity-item"><strong>Google Sub</strong><span>${escapeHtml(identity.google_sub || '-')}</span></div>
      <div class="identity-item"><strong>Email verificado</strong><span>${identity.email_verified ? 'Si' : 'No'}</span></div>
      <div class="identity-item"><strong>Given name</strong><span>${escapeHtml(identity.given_name || '-')}</span></div>
      <div class="identity-item"><strong>Family name</strong><span>${escapeHtml(identity.family_name || '-')}</span></div>
      <div class="identity-item"><strong>Locale</strong><span>${escapeHtml(identity.locale || '-')}</span></div>
      <div class="identity-item"><strong>Ultimo login</strong><span>${escapeHtml(formatDate(identity.last_login_at))}</span></div>
      <div class="identity-item"><strong>Alta cuenta</strong><span>${escapeHtml(formatDate(identity.created_at))}</span></div>
      <div class="identity-item"><strong>Estado</strong><span>${identity.is_active ? 'Activa' : 'Inactiva'}</span></div>
    </div>
  `;
}

function renderForm() {
  const draft = state.profileDraft || normalizeDraft(state.profilePayload?.profile);
  const options = profileOptions();
  const updatedAt = state.profilePayload?.profile?.updated_at;
  const node = getNode('profile-page-form');

  node.innerHTML = `
    <section class="section-card">
      <div class="section-title">Datos personales</div>
      <div class="section-copy">Canales de contacto y datos base del usuario autenticado.</div>
      <div class="form-grid">
        <div class="field">
          <label>Telefono</label>
          <input class="input" data-field="phone_e164" value="${escapeHtml(draft.phone_e164)}" placeholder="+59899111222" />
          <div class="hint">Formato E.164 para SMS.</div>
        </div>
        <div class="field">
          <label>WhatsApp</label>
          <input class="input" data-field="whatsapp_e164" value="${escapeHtml(draft.whatsapp_e164)}" placeholder="+59899111222" />
          <div class="hint">Formato E.164 para notificaciones por WhatsApp.</div>
        </div>
        <div class="field">
          <label>Cargo</label>
          <input class="input" data-field="job_title" value="${escapeHtml(draft.job_title)}" placeholder="Ej. Director tecnico" />
        </div>
        <div class="field">
          <label>Idioma preferido</label>
          <select class="select" data-field="preferred_language">${buildSelectOptions(options.preferred_languages, draft.preferred_language)}</select>
        </div>
      </div>
    </section>

    <section class="section-card">
      <div class="section-title">Organizacion</div>
      <div class="section-copy">Clasifica el contexto institucional o productivo desde el que se usa AgroClimaX.</div>
      <div class="form-grid">
        <div class="field">
          <label>Nombre</label>
          <input class="input" data-field="organization_name" value="${escapeHtml(draft.organization_name)}" placeholder="Empresa, organismo o establecimiento" />
        </div>
        <div class="field">
          <label>Tipo</label>
          <select class="select" data-field="organization_type">${buildSelectOptions(options.organization_types, draft.organization_type)}</select>
        </div>
        <div class="field">
          <label>Rol</label>
          <select class="select" data-field="role_code">${buildSelectOptions(options.role_codes, draft.role_code)}</select>
        </div>
      </div>
    </section>

    <section class="section-card">
      <div class="section-title">Cobertura operativa</div>
      <div class="section-copy">Define el alcance territorial principal con el que trabajara esta cuenta.</div>
      <div class="form-grid">
        <div class="field">
          <label>Ambito</label>
          <select class="select" data-field="scope_type">${buildSelectOptions(options.scope_types, draft.scope_type)}</select>
        </div>
        <div class="field wide">
          <label>Seleccion</label>
          ${buildScopeSelect(draft, options)}
          <div class="hint">Seleccion multiple segun el ambito elegido.</div>
        </div>
      </div>
    </section>

    <section class="section-card">
      <div class="section-title">Perfil productivo</div>
      <div class="section-copy">Informacion de escala y rubro para ajustar el contexto de monitoreo.</div>
      <div class="form-grid">
        <div class="field">
          <label>Tipo principal</label>
          <select class="select" data-field="production_type">${buildSelectOptions(options.production_types, draft.production_type)}</select>
        </div>
        <div class="field">
          <label>Superficie (ha)</label>
          <input class="input" data-field="operation_size_hectares" type="number" min="0" step="0.1" value="${escapeHtml(draft.operation_size_hectares)}" />
        </div>
        <div class="field">
          <label>Cabezas de ganado</label>
          <input class="input" data-field="livestock_headcount" type="number" min="0" step="1" value="${escapeHtml(draft.livestock_headcount)}" />
        </div>
        <div class="field">
          <label>Cultivos principales</label>
          <input class="input" data-field="crop_types_json" value="${escapeHtml((draft.crop_types_json || []).join(', '))}" placeholder="maiz, soja, pasturas" />
          <div class="hint">Separados por coma.</div>
        </div>
      </div>
    </section>

    <section class="section-card">
      <div class="section-title">Uso de la plataforma</div>
      <div class="section-copy">Selecciona los objetivos operativos principales de esta cuenta.</div>
      ${buildCheckboxGroup('use_cases_json', options.use_cases, draft.use_cases_json)}
    </section>

    <section class="section-card">
      <div class="section-title">Alertas y contacto</div>
      <div class="section-copy">Canales y umbral minimo para recibir alertas.</div>
      <div class="form-grid">
        <div class="field wide">
          <label>Canales</label>
          ${buildCheckboxGroup('alert_channels_json', options.alert_channels, draft.alert_channels_json)}
        </div>
        <div class="field">
          <label>Severidad minima</label>
          <select class="select" data-field="min_alert_state">${buildSelectOptions(options.min_alert_states, draft.min_alert_state)}</select>
        </div>
      </div>
    </section>

    <section class="section-card">
      <div class="section-title">Consentimiento</div>
      <div class="section-copy">Los consentimientos impactan directamente en la completitud del perfil.</div>
      <div class="form-layout">
        <label class="check">
          <input type="checkbox" data-field="data_usage_consent" ${draft.data_usage_consent ? 'checked' : ''} />
          <span>Acepto el uso operativo de mis datos dentro de AgroClimaX.</span>
        </label>
        <label class="check">
          <input type="checkbox" data-field="communications_opt_in" ${draft.communications_opt_in ? 'checked' : ''} />
          <span>Acepto recibir comunicaciones operativas y actualizaciones de la plataforma.</span>
        </label>
      </div>
    </section>

    <div class="page-footer">
      <div class="page-meta">Ultima actualizacion: ${escapeHtml(formatDate(updatedAt))}</div>
      <button class="btn primary" id="profile-page-save-btn" type="button">Guardar perfil</button>
    </div>
  `;

  wireFormEvents();
}

function updateDraftField(field, value) {
  const draft = clone(state.profileDraft || normalizeDraft(state.profilePayload?.profile));
  draft[field] = value;
  state.profileDraft = draft;
}

function subscriptionTargets() {
  const options = subscriptionOptions();
  const scopeType = state.subscriptionDraft?.scope_type || 'productive_unit';
  if (scopeType === 'department') return options.departments || [];
  if (scopeType === 'productive_unit') return options.productive_units || [];
  return [];
}

function renderSubscriptionManager() {
  const node = getNode('profile-page-subscriptions');
  if (!node) return;
  const options = subscriptionOptions();
  const draft = state.subscriptionDraft || defaultSubscriptionDraft();
  const availableChannels = options.channels || [];
  const channelsSelected = new Set(draft.channels_json || []);
  const targetOptions = subscriptionTargets();

  const subscriptionList = (state.subscriptions || []).length
    ? `
      <div class="sub-list">
        ${state.subscriptions.map((item) => `
          <div class="sub-item">
            <div class="sub-item-main">
              <div class="sub-item-title">
                <span>${escapeHtml(item.scope_label)}</span>
                <span class="badge">${escapeHtml(item.scope_type)}</span>
                <span class="badge">${escapeHtml(item.min_alert_state)}</span>
                <span class="badge">${item.active ? 'Activa' : 'Pausada'}</span>
              </div>
              <div class="sub-item-copy">
                Canales: ${escapeHtml((item.channels_json || []).join(', ') || 'sin canales')} ·
                Ultimo envio: ${escapeHtml(formatDate(item.last_sent_at))} ·
                Ultimo estado: ${escapeHtml(item.last_sent_state || '-')}
              </div>
            </div>
            <div class="sub-item-actions">
              <button class="btn small" type="button" data-sub-action="edit" data-subscription-id="${escapeHtml(item.id)}">Editar</button>
              <button class="btn small" type="button" data-sub-action="test" data-subscription-id="${escapeHtml(item.id)}">Probar envio</button>
              <button class="btn small" type="button" data-sub-action="delete" data-subscription-id="${escapeHtml(item.id)}">Eliminar</button>
            </div>
          </div>
        `).join('')}
      </div>
    `
    : '<div class="sub-empty">Todavia no hay alertas configuradas para esta cuenta.</div>';

  node.innerHTML = `
    <section class="section-card">
      <div class="section-title">Suscripciones actuales</div>
      <div class="section-copy">Cada suscripcion envia la alerta cuando el alcance entra o empeora segun tu severidad minima configurada.</div>
      ${subscriptionList}
    </section>

    <section class="section-card">
      <div class="section-title">${draft.id ? 'Editar alerta' : 'Nueva alerta'}</div>
      <div class="section-copy">Se enviaran dos imagenes: captura con alerta y Humedad Superficial del Suelo del mismo alcance.</div>
      <div class="form-grid">
        <div class="field">
          <label>Alcance</label>
          <select class="select" id="profile-page-sub-scope-type">
            ${buildSelectOptions(options.scope_types, draft.scope_type)}
          </select>
        </div>
        <div class="field">
          <label>Destino</label>
          ${draft.scope_type === 'national'
            ? '<div class="scope-note">Uruguay completo.</div>'
            : `
              <select class="select" id="profile-page-sub-scope-id">
                <option value="">Seleccionar...</option>
                ${targetOptions.map((item) => `<option value="${escapeHtml(item.id)}" ${item.id === draft.scope_id ? 'selected' : ''}>${escapeHtml(item.label)}${item.department ? ` · ${escapeHtml(item.department)}` : ''}</option>`).join('')}
              </select>
            `
          }
        </div>
        <div class="field wide">
          <label>Canales</label>
          <div class="check-grid">
            ${availableChannels.map((channel) => `
              <label class="check">
                <input type="checkbox" data-sub-channel="${escapeHtml(channel.value)}" ${channelsSelected.has(channel.value) ? 'checked' : ''} ${channel.enabled ? '' : 'disabled'} />
                <span>${escapeHtml(channel.label)}${channel.enabled ? '' : ` · ${escapeHtml(channel.reason || 'No disponible')}`}</span>
              </label>
            `).join('')}
          </div>
        </div>
        <div class="field">
          <label>Severidad minima</label>
          <select class="select" id="profile-page-sub-min-alert">
            ${buildSelectOptions(options.min_alert_states, draft.min_alert_state)}
          </select>
        </div>
        <div class="field">
          <label>Estado</label>
          <label class="check">
            <input type="checkbox" id="profile-page-sub-active" ${draft.active ? 'checked' : ''} />
            <span>Suscripcion activa</span>
          </label>
        </div>
      </div>
      <div class="page-footer">
        <div class="page-meta">Destino email: ${escapeHtml(options.contact?.email || '-')} · WhatsApp: ${escapeHtml(options.contact?.whatsapp_e164 || '-')}</div>
        <div class="sub-item-actions">
          <button class="btn small" id="profile-page-subscription-reset-btn" type="button">Limpiar</button>
          <button class="btn primary small" id="profile-page-subscription-save-btn" type="button">${draft.id ? 'Actualizar alerta' : 'Crear alerta'}</button>
        </div>
      </div>
    </section>
  `;
  wireSubscriptionEvents();
  setSubscriptionSavingState(false);
}

function wireFormEvents() {
  const form = getNode('profile-page-form');
  form.querySelectorAll('[data-field]').forEach((node) => {
    node.addEventListener('change', (event) => {
      const field = event.target.dataset.field;
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
      const field = node.dataset.arrayField;
      const selected = Array.from(form.querySelectorAll(`[data-array-field="${field}"]:checked`)).map((item) => item.value);
      updateDraftField(field, selected);
    });
  });

  const scopeSelect = getNode('profile-page-scope-ids');
  scopeSelect?.addEventListener('change', () => {
    const selected = Array.from(scopeSelect.selectedOptions).map((item) => item.value);
    updateDraftField('scope_ids_json', selected);
  });

  getNode('profile-page-save-btn')?.addEventListener('click', async () => {
    try {
      await persistProfile();
    } catch {}
  });
}

function updateSubscriptionDraft(patch) {
  state.subscriptionDraft = {
    ...(state.subscriptionDraft || defaultSubscriptionDraft()),
    ...patch,
  };
}

function wireSubscriptionEvents() {
  getNode('profile-page-sub-scope-type')?.addEventListener('change', (event) => {
    updateSubscriptionDraft({
      scope_type: event.target.value || 'productive_unit',
      scope_id: '',
    });
    renderSubscriptionManager();
  });

  getNode('profile-page-sub-scope-id')?.addEventListener('change', (event) => {
    updateSubscriptionDraft({ scope_id: event.target.value || '' });
  });

  getNode('profile-page-sub-min-alert')?.addEventListener('change', (event) => {
    updateSubscriptionDraft({ min_alert_state: event.target.value || 'Alerta' });
  });

  getNode('profile-page-sub-active')?.addEventListener('change', (event) => {
    updateSubscriptionDraft({ active: Boolean(event.target.checked) });
  });

  document.querySelectorAll('[data-sub-channel]').forEach((node) => {
    node.addEventListener('change', () => {
      const channels = Array.from(document.querySelectorAll('[data-sub-channel]:checked')).map((item) => item.dataset.subChannel);
      updateSubscriptionDraft({ channels_json: channels });
    });
  });

  document.querySelectorAll('[data-sub-action]').forEach((node) => {
    node.addEventListener('click', async () => {
      const subscriptionId = node.dataset.subscriptionId;
      const action = node.dataset.subAction;
      if (!subscriptionId || !action) return;
      if (action === 'edit') {
        const item = (state.subscriptions || []).find((entry) => entry.id === subscriptionId);
        state.subscriptionDraft = normalizeSubscriptionDraft(item);
        renderSubscriptionManager();
        return;
      }
      if (action === 'delete') {
        try {
          state.subscriptionBusyId = subscriptionId;
          setSubscriptionsStatus('Eliminando alerta...', 'info');
          await deleteAlertSubscription(subscriptionId);
          await refreshSubscriptionData();
          state.subscriptionDraft = defaultSubscriptionDraft();
          renderSubscriptionManager();
          setSubscriptionsStatus('Alerta eliminada.', 'success');
        } catch (error) {
          setSubscriptionsStatus(`No se pudo eliminar la alerta: ${formatRequestError(error)}`, 'error');
        } finally {
          state.subscriptionBusyId = null;
        }
        return;
      }
      if (action === 'test') {
        try {
          state.subscriptionBusyId = subscriptionId;
          setSubscriptionsStatus('Enviando prueba...', 'info');
          await testAlertSubscription(subscriptionId);
          setSubscriptionsStatus('Prueba enviada. Revisa email o WhatsApp segun la configuracion.', 'success');
        } catch (error) {
          setSubscriptionsStatus(`No se pudo enviar la prueba: ${formatRequestError(error)}`, 'error');
        } finally {
          state.subscriptionBusyId = null;
        }
      }
    });
  });

  getNode('profile-page-subscription-save-btn')?.addEventListener('click', async () => {
    try {
      await persistSubscription();
    } catch {}
  });
  getNode('profile-page-subscription-reset-btn')?.addEventListener('click', () => {
    state.subscriptionDraft = defaultSubscriptionDraft();
    renderSubscriptionManager();
    setSubscriptionsStatus('Formulario de alerta limpio.', 'info');
  });
}

async function refreshSubscriptionData() {
  const [optionsPayload, listPayload] = await Promise.all([
    fetchAlertSubscriptionOptions(),
    fetchAlertSubscriptions(),
  ]);
  state.subscriptionOptions = optionsPayload;
  state.subscriptions = listPayload.items || [];
  if (!state.subscriptionDraft) {
    state.subscriptionDraft = defaultSubscriptionDraft();
  }
}

async function persistProfile() {
  setSavingState(true);
  setStatus('Guardando perfil...', 'info');
  try {
    const payload = await saveProfileMe(serializeDraftForSave(state.profileDraft || normalizeDraft(state.profilePayload?.profile)));
    state.profilePayload = payload;
    state.profileDraft = normalizeDraft(payload.profile);
    await refreshSubscriptionData();
    setStore({
      profilePayload: payload,
      profileDraft: clone(state.profileDraft),
      profileStatus: payload.completion || null,
    });
    renderAuthenticatedPage();
    setStatus('Perfil guardado correctamente.', 'success');
    setSubscriptionsStatus('Canales y opciones de alerta actualizados.', 'success');
  } catch (error) {
    setStatus(`No se pudo guardar el perfil: ${formatRequestError(error)}`, 'error');
    setSavingState(false);
    throw error;
  }
}

async function persistSubscription() {
  setSubscriptionSavingState(true);
  setSubscriptionsStatus('Guardando alerta configurable...', 'info');
  try {
    const payload = await saveAlertSubscription(state.subscriptionDraft || defaultSubscriptionDraft());
    await refreshSubscriptionData();
    state.subscriptionDraft = normalizeSubscriptionDraft(payload);
    renderSubscriptionManager();
    setSubscriptionsStatus('Alerta configurada correctamente.', 'success');
  } catch (error) {
    setSubscriptionsStatus(`No se pudo guardar la alerta: ${formatRequestError(error)}`, 'error');
    setSubscriptionSavingState(false);
    throw error;
  }
}

function renderAuthenticatedPage() {
  renderHeaderUser();
  renderSummary();
  renderIdentity();
  renderForm();
  renderSubscriptionManager();
  setSavingState(false);
  renderAuthGate();
}

async function loadAuthenticatedData() {
  const [profilePayload] = await Promise.all([
    fetchProfileMe(),
    refreshSubscriptionData(),
  ]);
  state.profilePayload = profilePayload;
  state.profileDraft = normalizeDraft(state.profilePayload.profile);
  if (!state.subscriptionDraft) state.subscriptionDraft = defaultSubscriptionDraft();
  setStore({
    profilePayload: state.profilePayload,
    profileDraft: clone(state.profileDraft),
    profileStatus: state.profilePayload.completion || null,
  });
  renderAuthenticatedPage();
  setStatus('Perfil cargado.', 'success');
  setSubscriptionsStatus('Suscripciones cargadas.', 'success');
}

async function ensureAuthenticated() {
  try {
    const authPayload = await fetchAuthMe();
    state.authSession = authPayload;
    state.authUser = authPayload.user || null;
    setStore({
      authSession: authPayload,
      authUser: authPayload.user || null,
      authCsrfToken: authPayload.csrf_token || null,
      authReady: true,
      profileStatus: authPayload.profile_status || null,
    });
    renderAuthGate();
    await loadAuthenticatedData();
  } catch {
    state.authSession = null;
    state.authUser = null;
    state.subscriptionOptions = null;
    state.subscriptions = [];
    state.subscriptionDraft = defaultSubscriptionDraft();
    setStore({
      authSession: null,
      authUser: null,
      authCsrfToken: null,
      authReady: true,
      profilePayload: null,
      profileDraft: null,
      profileStatus: null,
    });
    renderAuthGate();
    setStatus('Inicia sesion con Google para acceder al perfil completo.', 'info');
    setSubscriptionsStatus('Inicia sesion con Google para configurar alertas.', 'info');
  }
}

function bindPageActions() {
  getNode('profile-page-back-btn')?.addEventListener('click', () => {
    window.location.assign('/');
  });
  getNode('profile-page-login-btn')?.addEventListener('click', () => {
    window.location.assign(googleLoginUrl('/perfil'));
  });
  getNode('profile-page-auth-login-btn')?.addEventListener('click', () => {
    window.location.assign(googleLoginUrl('/perfil'));
  });
  getNode('profile-page-logout-btn')?.addEventListener('click', async () => {
    try {
      await logoutCurrentUser();
    } finally {
      setStore({
        authSession: null,
        authUser: null,
        authCsrfToken: null,
        authReady: true,
        profilePayload: null,
        profileDraft: null,
        profileStatus: null,
      });
      window.location.assign('/perfil');
    }
  });
}

document.addEventListener('DOMContentLoaded', async () => {
  bindPageActions();
  await ensureAuthenticated();
});
