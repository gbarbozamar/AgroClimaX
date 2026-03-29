import { fetchAuthMe, googleLoginUrl, logoutCurrentUser, profilePageUrl } from './api.js?v=20260329-7';
import { setStore, store } from './state.js?v=20260329-2';

function getNode(id) {
  return document.getElementById(id);
}

function authErrorMessage(code) {
  const messages = {
    google_not_configured: 'El login con Google todavia no esta configurado en este entorno.',
    google_discovery_unavailable: 'Google no respondio a tiempo en este entorno. Proba de nuevo en unos minutos.',
    missing_google_state: 'La sesion de login expiro antes de completar Google. Proba de nuevo.',
    invalid_google_state: 'No se pudo validar la respuesta de Google. Proba otra vez.',
    google_login_failed: 'Google no pudo completar el inicio de sesion.',
    access_denied: 'Google cancelo o denego el acceso.',
  };
  return messages[code] || 'Inicia sesion con Google para entrar a AgroClimaX.';
}

function queryAuthError() {
  const params = new URLSearchParams(window.location.search);
  return params.get('auth_error');
}

function syncAuthChrome() {
  const gate = getNode('auth-gate');
  const userBadge = getNode('auth-user-badge');
  const loginButton = getNode('auth-login-btn');
  const logoutButton = getNode('auth-logout-btn');
  const welcome = getNode('auth-welcome');
  const message = getNode('auth-gate-message');
  const subtitle = getNode('auth-user-subtitle');
  const name = getNode('auth-user-name');
  const avatar = getNode('auth-user-avatar');

  const isAuthenticated = Boolean(store.authUser);
  document.body.classList.toggle('auth-blocked', !isAuthenticated);
  gate?.classList.toggle('hidden', isAuthenticated);
  userBadge?.classList.toggle('hidden', !isAuthenticated);
  logoutButton?.classList.toggle('hidden', !isAuthenticated);
  loginButton?.classList.toggle('hidden', isAuthenticated);

  if (isAuthenticated) {
    if (name) name.textContent = store.authUser.full_name || store.authUser.email || 'Usuario';
    if (subtitle) subtitle.textContent = store.authUser.email || 'Cuenta Google';
    if (welcome) welcome.textContent = `Sesion iniciada como ${store.authUser.full_name || store.authUser.email || 'usuario'}.`;
    if (avatar) {
      if (store.authUser.picture_url) {
        avatar.src = store.authUser.picture_url;
        avatar.classList.remove('hidden');
      } else {
        avatar.classList.add('hidden');
      }
    }
  } else {
    if (message) message.textContent = authErrorMessage(queryAuthError());
    if (welcome) welcome.textContent = 'Necesitas autenticarte con Google para usar el dashboard y las APIs operativas.';
  }
  if (userBadge) {
    userBadge.tabIndex = isAuthenticated ? 0 : -1;
    userBadge.setAttribute('role', 'button');
    userBadge.setAttribute('aria-label', 'Ir al perfil de usuario');
  }
}

function clearAuthQueryError() {
  const url = new URL(window.location.href);
  if (!url.searchParams.has('auth_error')) return;
  url.searchParams.delete('auth_error');
  window.history.replaceState({}, '', url.toString());
}

export async function ensureAuthenticatedSession() {
  try {
    const payload = await fetchAuthMe();
    setStore({
      authUser: payload.user || null,
      authCsrfToken: payload.csrf_token || null,
      authSession: payload,
      profileStatus: payload.profile_status || null,
      authReady: true,
    });
    clearAuthQueryError();
    syncAuthChrome();
    return true;
  } catch (error) {
    setStore({
      authUser: null,
      authCsrfToken: null,
      authSession: null,
      profileStatus: null,
      authReady: true,
    });
    syncAuthChrome();
    return false;
  }
}

export function initAuth() {
  const loginButton = getNode('auth-login-btn');
  const logoutButton = getNode('auth-logout-btn');
  const gateButton = getNode('auth-gate-login-btn');
  const userBadge = getNode('auth-user-badge');

  const startLogin = () => {
    window.location.assign(googleLoginUrl());
  };
  const openProfile = () => {
    window.location.assign(profilePageUrl());
  };

  loginButton?.addEventListener('click', startLogin);
  gateButton?.addEventListener('click', startLogin);
  userBadge?.addEventListener('click', () => {
    if (!store.authUser) return;
    openProfile();
  });
  userBadge?.addEventListener('keydown', (event) => {
    if (!store.authUser) return;
    if (event.key === 'Enter' || event.key === ' ') {
      event.preventDefault();
      openProfile();
    }
  });
  logoutButton?.addEventListener('click', async () => {
    try {
      await logoutCurrentUser();
    } finally {
      setStore({ authUser: null, authCsrfToken: null, authSession: null, profileStatus: null });
      window.location.assign('/');
    }
  });

  window.addEventListener('agroclimax:unauthorized', () => {
    setStore({ authUser: null, authCsrfToken: null, authSession: null, profileStatus: null });
    syncAuthChrome();
  });

  syncAuthChrome();
  return ensureAuthenticatedSession();
}
