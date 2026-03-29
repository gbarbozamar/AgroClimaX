# Guia para implementar Google OAuth2 en otra aplicacion

Esta guia explica paso a paso como implementar Google OAuth2 (Authorization Code Flow)
reutilizando las mismas credenciales del OAuth client **AgroClimaX Web Client**.

---

## Credenciales compartidas

```
Client ID:     <ver en Google Cloud Console o en las variables de entorno de Railway>
Client Secret: <ver en Google Cloud Console o en las variables de entorno de Railway>
```

> Para obtenerlas: Google Cloud Console > APIs & Services > Credentials >
> OAuth 2.0 Client IDs > AgroClimaX Web Client

> Estas credenciales pertenecen al proyecto de Google Cloud `AI Deep Economics`.
> Cualquier aplicacion que las use comparte el mismo consent screen y limites de cuota.

---

## Paso 1: Registrar las URIs de tu nueva aplicacion en Google Cloud Console

Antes de tocar codigo, agrega las URIs de tu nueva app en:

**Google Cloud Console > APIs & Services > Credentials > OAuth 2.0 Client IDs >
`873768571599-...` > Edit**

### Authorized JavaScript origins

Agrega el origen (scheme + host + port) de tu nueva app. Ejemplos:

```
https://mi-nueva-app.railway.app
http://localhost:3000
http://127.0.0.1:3000
```

### Authorized redirect URIs

Agrega la URI exacta del callback de tu nueva app. La URI debe coincidir
**caracter por caracter** con lo que tu backend envia a Google. Ejemplos:

```
https://mi-nueva-app.railway.app/api/auth/google/callback
http://localhost:3000/api/auth/google/callback
http://127.0.0.1:3000/api/auth/google/callback
```

> Los cambios en Google Cloud Console pueden tardar 2-5 minutos en propagarse.

### URIs ya registradas (AgroClimaX)

```
# JavaScript origins
http://127.0.0.1:8050
https://agroclimax-production-a43f.up.railway.app
http://localhost:8050

# Redirect URIs
http://127.0.0.1:8050/api/v1/auth/google/callback
https://agroclimax-production-a43f.up.railway.app/api/v1/auth/google/callback
http://localhost:8050/api/v1/auth/google/callback
```

No borres estas URIs existentes — solo agrega las nuevas.

---

## Paso 2: Entender el flujo OAuth2 (Authorization Code)

```
Usuario hace click en "Login con Google"
  |
  v
Tu backend redirige al usuario a Google:
  GET https://accounts.google.com/o/oauth2/v2/auth
    ?client_id=873768571599-...
    &redirect_uri=https://tu-app.com/api/auth/google/callback
    &response_type=code
    &scope=openid email profile
    &state=<token-anti-csrf>
  |
  v
Google muestra pantalla de consentimiento
  |
  v
Usuario acepta -> Google redirige a tu redirect_uri:
  GET https://tu-app.com/api/auth/google/callback?code=XXXX&state=YYYY
  |
  v
Tu backend intercambia el `code` por tokens:
  POST https://oauth2.googleapis.com/token
    code=XXXX
    client_id=873768571599-...
    client_secret=GOCSPX-...
    redirect_uri=https://tu-app.com/api/auth/google/callback
    grant_type=authorization_code
  |
  v
Google responde con: { access_token, id_token, ... }
  |
  v
Tu backend usa el access_token para obtener info del usuario:
  GET https://www.googleapis.com/oauth2/v2/userinfo
    Authorization: Bearer <access_token>
  |
  v
Google responde con: { id, email, name, picture, ... }
  |
  v
Tu backend crea/actualiza el usuario en tu DB y emite un JWT propio
  |
  v
Redirige al frontend con el JWT (ej: /#token=<jwt>)
```

---

## Paso 3: Variables de entorno

Configura estas variables en tu nueva aplicacion:

```env
# Google OAuth (mismas credenciales)
GOOGLE_CLIENT_ID=<TU_GOOGLE_CLIENT_ID>
GOOGLE_CLIENT_SECRET=<TU_GOOGLE_CLIENT_SECRET>

# JWT (genera un secret unico para CADA aplicacion)
# Comando: python3 -c "import secrets; print(secrets.token_hex(32))"
JWT_SECRET_KEY=<tu-secret-unico-de-64-chars>
JWT_ALGORITHM=HS256
JWT_EXPIRE_MINUTES=1440
```

> **IMPORTANTE**: El `JWT_SECRET_KEY` debe ser diferente para cada aplicacion.
> Si dos apps comparten el mismo JWT secret, los tokens de una son validos en la otra.

---

## Paso 4: Implementacion backend (FastAPI / Python)

### 4.1 Dependencias necesarias

Estas librerias son suficientes (no necesitas `authlib` ni `google-auth`):

```toml
# pyproject.toml
[tool.poetry.dependencies]
fastapi = "^0.111.0"
httpx = "^0.27.0"           # HTTP client async para llamar a Google
python-jose = {version = "^3.3.0", extras = ["cryptography"]}  # JWT
sqlalchemy = {version = "^2.0.30", extras = ["asyncio"]}
```

### 4.2 Modelo de usuario

```python
# app/models/user.py
from datetime import datetime, timezone
from sqlalchemy import Column, Integer, String, Boolean, DateTime
from app.db.session import Base


class User(Base):
    __tablename__ = "users"

    id = Column(Integer, primary_key=True, autoincrement=True)
    google_id = Column(String(255), unique=True, nullable=False, index=True)
    email = Column(String(320), unique=True, nullable=False, index=True)
    name = Column(String(255), nullable=False)
    picture_url = Column(String(1024), nullable=True)
    is_active = Column(Boolean, default=True)
    created_at = Column(
        DateTime(timezone=True), default=lambda: datetime.now(timezone.utc)
    )
    last_login = Column(
        DateTime(timezone=True), default=lambda: datetime.now(timezone.utc)
    )
```

Campos clave:
- `google_id`: identificador estable de Google (no cambia aunque el usuario cambie su email)
- `email`: puede cambiar, pero util para busquedas y display
- `picture_url`: URL del avatar de Google (para mostrar en el frontend)

### 4.3 Utilidades JWT

```python
# app/core/security.py
from datetime import datetime, timedelta, timezone
from jose import jwt, JWTError
from fastapi import Depends, HTTPException, status
from fastapi.security import OAuth2PasswordBearer
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from app.core.config import settings
from app.db.session import get_db
from app.models.user import User

oauth2_scheme = OAuth2PasswordBearer(tokenUrl="token", auto_error=False)


def create_access_token(data: dict, expires_delta: timedelta | None = None) -> str:
    """Crea un JWT firmado con HS256."""
    to_encode = data.copy()
    expire = datetime.now(timezone.utc) + (
        expires_delta or timedelta(minutes=settings.jwt_expire_minutes)
    )
    to_encode["exp"] = expire
    return jwt.encode(to_encode, settings.jwt_secret_key, algorithm=settings.jwt_algorithm)


async def get_current_user(
    token: str | None = Depends(oauth2_scheme),
    db: AsyncSession = Depends(get_db),
) -> User:
    """Dependencia FastAPI: extrae y valida el usuario del JWT.
    Usa en endpoints protegidos: user: User = Depends(get_current_user)
    """
    if token is None:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Not authenticated")
    try:
        payload = jwt.decode(token, settings.jwt_secret_key, algorithms=[settings.jwt_algorithm])
        user_id: int = payload.get("sub")
        if user_id is None:
            raise JWTError()
    except JWTError:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid token")
    result = await db.execute(select(User).where(User.id == user_id))
    user = result.scalar_one_or_none()
    if user is None or not user.is_active:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="User not found")
    return user
```

### 4.4 Endpoints de autenticacion

```python
# app/api/auth.py
import secrets
from datetime import datetime, timedelta, timezone
from urllib.parse import urlencode

import httpx
from jose import jwt, JWTError
from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import RedirectResponse
from sqlalchemy import select

from app.core.config import settings
from app.core.security import create_access_token
from app.db.session import AsyncSessionLocal
from app.models.user import User

router = APIRouter(prefix="/auth", tags=["auth"])

# URLs de Google OAuth2 (no cambian)
GOOGLE_AUTH_URL = "https://accounts.google.com/o/oauth2/v2/auth"
GOOGLE_TOKEN_URL = "https://oauth2.googleapis.com/token"
GOOGLE_USERINFO_URL = "https://www.googleapis.com/oauth2/v2/userinfo"


def _build_redirect_uri(request: Request) -> str:
    """Construye la redirect_uri a partir del request actual.

    Esto hace que funcione automaticamente en local y produccion:
    - Local:      http://localhost:3000/api/auth/google/callback
    - Produccion: https://mi-app.railway.app/api/auth/google/callback

    IMPORTANTE: request.base_url respeta los headers X-Forwarded-Proto
    y X-Forwarded-Host que setean los reverse proxies (Railway, nginx, etc).
    """
    base = str(request.base_url).rstrip("/")
    # Adapta este path a tu estructura de rutas:
    return f"{base}/api/auth/google/callback"


def _encode_state(redirect_uri: str) -> str:
    """Codifica la redirect_uri y un nonce en un JWT de corta vida.

    Esto evita necesitar almacenamiento server-side (sessions/redis)
    para recordar la redirect_uri entre el login y el callback.
    El state tambien protege contra CSRF.
    """
    payload = {
        "nonce": secrets.token_hex(16),
        "redirect_uri": redirect_uri,
        "exp": datetime.now(timezone.utc) + timedelta(minutes=10),
    }
    return jwt.encode(payload, settings.jwt_secret_key, algorithm=settings.jwt_algorithm)


def _decode_state(state: str) -> dict:
    """Decodifica y verifica el state JWT."""
    try:
        return jwt.decode(state, settings.jwt_secret_key, algorithms=[settings.jwt_algorithm])
    except JWTError:
        raise HTTPException(status_code=400, detail="Invalid or expired state parameter")


# ── ENDPOINT 1: Iniciar login ──────────────────────────────────
@router.get("/google/login")
async def google_login(request: Request):
    """Redirige al usuario a la pantalla de consentimiento de Google."""
    redirect_uri = _build_redirect_uri(request)
    state = _encode_state(redirect_uri)

    params = {
        "client_id": settings.google_client_id,
        "redirect_uri": redirect_uri,
        "response_type": "code",
        "scope": "openid email profile",
        "state": state,
        "access_type": "online",
        "prompt": "consent",
    }
    return RedirectResponse(
        url=f"{GOOGLE_AUTH_URL}?{urlencode(params)}",
        status_code=307,
    )


# ── ENDPOINT 2: Callback de Google ─────────────────────────────
@router.get("/google/callback")
async def google_callback(request: Request, code: str, state: str):
    """Recibe el code de Google, lo intercambia por tokens,
    crea/actualiza el usuario, y redirige al frontend con un JWT."""

    # 1. Recuperar redirect_uri del state
    state_data = _decode_state(state)
    redirect_uri = state_data["redirect_uri"]

    async with httpx.AsyncClient() as client:
        # 2. Intercambiar code por tokens
        token_resp = await client.post(
            GOOGLE_TOKEN_URL,
            data={
                "code": code,
                "client_id": settings.google_client_id,
                "client_secret": settings.google_client_secret,
                "redirect_uri": redirect_uri,
                "grant_type": "authorization_code",
            },
        )
        if token_resp.status_code != 200:
            raise HTTPException(status_code=400, detail="Failed to exchange code")
        tokens = token_resp.json()

        # 3. Obtener info del usuario
        userinfo_resp = await client.get(
            GOOGLE_USERINFO_URL,
            headers={"Authorization": f"Bearer {tokens['access_token']}"},
        )
        if userinfo_resp.status_code != 200:
            raise HTTPException(status_code=400, detail="Failed to fetch user info")
        userinfo = userinfo_resp.json()

    # 4. Upsert en base de datos
    async with AsyncSessionLocal() as db:
        result = await db.execute(
            select(User).where(User.google_id == userinfo["id"])
        )
        user = result.scalar_one_or_none()
        now = datetime.now(timezone.utc)

        if user:
            user.name = userinfo.get("name", user.name)
            user.picture_url = userinfo.get("picture")
            user.email = userinfo.get("email", user.email)
            user.last_login = now
        else:
            user = User(
                google_id=userinfo["id"],
                email=userinfo["email"],
                name=userinfo.get("name", ""),
                picture_url=userinfo.get("picture"),
                created_at=now,
                last_login=now,
            )
            db.add(user)

        await db.commit()
        await db.refresh(user)

    # 5. Crear JWT de la aplicacion
    app_token = create_access_token(
        data={
            "sub": user.id,
            "email": user.email,
            "name": user.name,
            "picture": user.picture_url or "",
        }
    )

    # 6. Redirigir al frontend con el token en el fragment
    #    El fragment (#) no se envia al servidor — es mas seguro que ?token=
    frontend = settings.frontend_url.rstrip("/") if settings.frontend_url else str(request.base_url).rstrip("/")
    return RedirectResponse(url=f"{frontend}/#token={app_token}", status_code=302)
```

### 4.5 Registrar el router

```python
# En tu archivo principal de rutas
from app.api.auth import router as auth_router

api_router.include_router(auth_router)
```

---

## Paso 5: Frontend (JavaScript vanilla)

```html
<!-- En el header o nav de tu app -->
<button id="login-btn" onclick="loginWithGoogle()">Login con Google</button>
<div id="user-info" style="display:none;">
  <img id="user-avatar" style="width:28px;height:28px;border-radius:50%;" />
  <span id="user-name"></span>
  <button onclick="logout()">Salir</button>
</div>

<script>
const API_BASE = window.location.origin + '/api';  // adapta a tu prefijo

function loginWithGoogle() {
  window.location.href = API_BASE + '/auth/google/login';
}

function logout() {
  localStorage.removeItem('token');
  updateAuthUI();
}

function updateAuthUI() {
  const token = localStorage.getItem('token');
  const loginBtn = document.getElementById('login-btn');
  const userInfo = document.getElementById('user-info');
  if (token) {
    try {
      // Decodificar el payload del JWT (base64) para mostrar nombre y avatar
      const payload = JSON.parse(atob(token.split('.')[1]));
      loginBtn.style.display = 'none';
      userInfo.style.display = 'flex';
      document.getElementById('user-name').textContent = payload.name;
      if (payload.picture) {
        document.getElementById('user-avatar').src = payload.picture;
      }
    } catch (e) {
      localStorage.removeItem('token');
      loginBtn.style.display = 'block';
      userInfo.style.display = 'none';
    }
  } else {
    loginBtn.style.display = 'block';
    userInfo.style.display = 'none';
  }
}

// Al cargar la pagina: leer token del fragment si viene del OAuth redirect
(function handleAuthRedirect() {
  const hash = window.location.hash;
  if (hash.startsWith('#token=')) {
    const token = hash.substring(7);
    localStorage.setItem('token', token);
    // Limpiar el token de la URL (seguridad)
    window.history.replaceState(null, '', window.location.pathname);
  }
  updateAuthUI();
})();

// Para llamadas autenticadas a tu API:
async function fetchAutenticado(url) {
  const token = localStorage.getItem('token');
  const resp = await fetch(url, {
    headers: token ? { 'Authorization': `Bearer ${token}` } : {},
  });
  if (resp.status === 401) {
    localStorage.removeItem('token');
    updateAuthUI();
  }
  return resp;
}
</script>
```

---

## Paso 6: Proteger endpoints (opcional)

Para requerir login en un endpoint:

```python
from app.core.security import get_current_user
from app.models.user import User
from fastapi import Depends

@router.get("/mi-endpoint-protegido")
async def endpoint_protegido(user: User = Depends(get_current_user)):
    return {"mensaje": f"Hola {user.name}", "email": user.email}
```

El frontend debe enviar el header `Authorization: Bearer <token>` en cada request.

---

## Errores comunes

### `redirect_uri_mismatch` (Error 400)

La URI que tu backend envia a Google no coincide exactamente con las registradas
en Google Cloud Console. Verificar:

1. **Protocolo**: `http` vs `https` — en produccion debe ser `https`
2. **Puerto**: `localhost:3000` vs `localhost:8050` — debe coincidir
3. **Path exacto**: `/api/auth/google/callback` vs `/api/v1/auth/google/callback`
4. **Trailing slash**: `/callback` vs `/callback/` — Google los diferencia
5. **Propagacion**: cambios en Google Cloud Console tardan 2-5 minutos

### `access_denied`

El usuario rechazo el consentimiento. No es un error de codigo.

### Token expirado

El JWT expira despues de `JWT_EXPIRE_MINUTES` (default 1440 = 24h).
El frontend recibe un 401 y debe redirigir al login.

### Reverse proxy no forwarda headers

Si `request.base_url` devuelve `http://` en produccion (deberia ser `https://`),
tu reverse proxy no esta enviando `X-Forwarded-Proto`. Solucion:

```python
# En uvicorn (ya es default):
uvicorn app.main:app --proxy-headers --forwarded-allow-ips="*"
```

---

## Checklist rapido para una nueva app

- [ ] Agregar JavaScript origins en Google Cloud Console
- [ ] Agregar redirect URIs en Google Cloud Console
- [ ] Esperar 2-5 minutos a que propaguen
- [ ] Configurar env vars: `GOOGLE_CLIENT_ID`, `GOOGLE_CLIENT_SECRET`, `JWT_SECRET_KEY`
- [ ] Crear modelo `User` con campo `google_id`
- [ ] Crear endpoints `/auth/google/login` y `/auth/google/callback`
- [ ] Agregar boton de login en el frontend
- [ ] Manejar el `#token=` en el fragment de la URL
- [ ] Enviar `Authorization: Bearer <token>` en requests autenticados
- [ ] Testear en local y en produccion (las URIs son diferentes)
