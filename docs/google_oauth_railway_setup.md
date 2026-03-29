# Google OAuth Setup
## AgroClimaX - Railway + Google Sign-In

**Fecha:** 2026-03-29  
**Objetivo:** completar la activacion de login con Google para AgroClimaX en Railway y localhost.

## Estado actual

Ya quedo implementado en codigo:
- flujo `Google OAuth 2.0 / OIDC`
- sesion backend con cookie segura
- proteccion de APIs
- `CSRF` para metodos mutantes
- pantalla de acceso en frontend

Ya quedo configurado en Railway:
- `GOOGLE_DISCOVERY_URL=https://accounts.google.com/.well-known/openid-configuration`
- `GOOGLE_REDIRECT_URI=https://agroclimax-production-a43f.up.railway.app/api/v1/auth/google/callback`
- `AUTH_COOKIE_NAME=agroclimax_session`
- `AUTH_SESSION_TTL_HOURS=72`
- `AUTH_CSRF_HEADER_NAME=X-CSRF-Token`
- `AUTH_LOGIN_SUCCESS_REDIRECT=/`

Lo unico que falta para activar el login real es obtener y cargar:
- `GOOGLE_CLIENT_ID`
- `GOOGLE_CLIENT_SECRET`

## 1. Crear el cliente OAuth en Google Cloud

1. Entrar a [Google Cloud Console](https://console.cloud.google.com/).
2. Crear o seleccionar el proyecto que va a representar AgroClimaX.
3. Ir a `APIs & Services > OAuth consent screen`.
4. Configurar:
   - `User Type`: `External`
   - nombre de la app: `AgroClimaX`
   - email de soporte
   - developer contact email
5. Guardar la pantalla de consentimiento.

## 2. Crear credenciales OAuth

1. Ir a `APIs & Services > Credentials`.
2. Elegir `Create Credentials > OAuth client ID`.
3. Tipo de aplicacion: `Web application`.
4. Nombre sugerido:
   - `AgroClimaX Production`

## 3. Authorized redirect URIs

Agregar exactamente estas URIs:

### Produccion Railway
```text
https://agroclimax-production-a43f.up.railway.app/api/v1/auth/google/callback
```

### Localhost
```text
http://127.0.0.1:8050/api/v1/auth/google/callback
http://localhost:8050/api/v1/auth/google/callback
```

Si despues se usa otro puerto local, agregar tambien esa variante.

## 4. Authorized JavaScript origins

Agregar estos origenes:

### Produccion Railway
```text
https://agroclimax-production-a43f.up.railway.app
```

### Localhost
```text
http://127.0.0.1:8050
http://localhost:8050
```

## 5. Copiar credenciales

Cuando Google cree el cliente, copiar:
- `Client ID`
- `Client secret`

## 6. Cargar credenciales en Railway

Ejecutar:

```powershell
railway variable set -s AgroClimaX GOOGLE_CLIENT_ID="<client-id>" GOOGLE_CLIENT_SECRET="<client-secret>"
railway variable set -s "AgroClimaX Worker" GOOGLE_CLIENT_ID="<client-id>" GOOGLE_CLIENT_SECRET="<client-secret>"
```

## 7. Cargar credenciales en desarrollo local

En [apps/backend/.env.example](C:/Users/barbo/Documents/PhD/AI%20Deep%20Economics/AgroClimaX/apps/backend/.env.example) ya quedaron las variables necesarias.  
Para desarrollo local, definir en `apps/backend/.env`:

```env
GOOGLE_CLIENT_ID=<client-id>
GOOGLE_CLIENT_SECRET=<client-secret>
GOOGLE_REDIRECT_URI=http://127.0.0.1:8050/api/v1/auth/google/callback
AUTH_COOKIE_NAME=agroclimax_session
AUTH_SESSION_TTL_HOURS=72
AUTH_CSRF_HEADER_NAME=X-CSRF-Token
AUTH_LOGIN_SUCCESS_REDIRECT=/
```

## 8. Verificacion funcional

### En Railway
1. Abrir:
   - [https://agroclimax-production-a43f.up.railway.app/](https://agroclimax-production-a43f.up.railway.app/)
2. Click en `Entrar con Google`.
3. Confirmar que Google redirige a:
   - `/api/v1/auth/google/callback`
4. Confirmar que, tras login, responde:
   - `GET /api/v1/auth/me` con `200`
   - `GET /api/v1/settings` con `200`

### En localhost
1. Abrir:
   - [http://127.0.0.1:8050/](http://127.0.0.1:8050/)
2. Click en `Continuar con Google`.
3. Confirmar:
   - redirect a Google
   - vuelta correcta a `/api/v1/auth/google/callback`
   - dashboard desbloqueado

## 9. Errores esperables

- `google_not_configured`
  Significa que faltan `GOOGLE_CLIENT_ID` o `GOOGLE_CLIENT_SECRET`.

- `google_discovery_unavailable`
  Significa que el backend no pudo consultar el discovery endpoint de Google.

- `missing_google_state` o `invalid_google_state`
  Significa que expiro o no coincidió el estado temporal del login.

- `google_login_failed`
  Significa que fallo el intercambio de codigo o la obtencion del perfil.

## 10. Criterio de salida

Se considera cerrado cuando:
- Railway tiene `GOOGLE_CLIENT_ID` y `GOOGLE_CLIENT_SECRET`
- localhost tiene las mismas credenciales
- login con Google funciona en produccion
- login con Google funciona en desarrollo local
- `/api/v1/auth/me` devuelve `200` con sesion autentica
- el dashboard deja de quedar bloqueado tras iniciar sesion
