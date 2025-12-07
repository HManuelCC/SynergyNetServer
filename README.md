# SynergyNet – Plataforma de orquestación y comunicación en tiempo real

SynergyNet es una plataforma modular para la orquestación de clientes y el intercambio de eventos en tiempo real, pensada para telemetría, coordinación de procesos y dashboards de observabilidad. El proyecto incluye:

- Un servidor principal en Go con WebSockets, ruteo y balanceo.
- Un panel (dashboard) HTTP con middleware de tokens.
- Módulos de conexión a base de datos.
- SDKs/Clientes oficiales en Go, Python y TypeScript (Node.js).
- Ejemplos/demos para iniciar rápidamente.

Este README describe el sistema completo (no solo los clientes), cómo arrancarlo y cómo integrarse.

## Objetivos y casos de uso

- Orquestar múltiples agentes/clients conectados al servidor.
- Transportar eventos y estados ($Event\_Slice$, $Event\_State$, información de cliente) de forma consistente.
- Proveer un dashboard/endpoint para administración y observabilidad.
- Facilitar integraciones en diferentes lenguajes (Go, Python, TS/Node.js).

## Arquitectura general

- **Servidor (Go):** núcleo del sistema (`Server/Socket.go`) que gestiona las conexiones WebSocket, enrutamiento de eventos y coordinación entre clientes.
- **Dashboard (Go/HTTP):** API y ruteo bajo `Server/dashboard/` con middleware de tokens (`midelware/Tokens.go`). Útil para administración/observabilidad.
- **Base de datos (Go):** módulo de conexión (`Database/Conection.go`) y contratos (`Database/interfaces.go`).
- **Proveedores y utilidades:** servicios para IP, procesos y utilidades del sistema en `Server/Data/interfaces/providers/` y `utils/`.
- **Balanceador/Queue Manager:** lógica de colas y balanceo en `Server/Data/interfaces/balancer/`.
- **Clientes/SDKs oficiales:**
  - Go: `Clients/Go/` (incluye `Socket_client/` y `cmd/demo.go`).
  - Python: `Clients/Python/` (paquete `synergynet_py`, ejemplos en `examples/demo.py`).
  - TypeScript: `Clients/TS/` (Node.js, `src/client.ts`, `src/demo.ts`).

Los clientes se conectan al servidor, envían/reciben eventos y estados, y exponen APIs sencillas para construir integraciones.

## Estructura del repositorio (resumen)

- `Server/` – Lógica del servidor
  - `Socket.go` – Socket server (WebSocket) y coordinación.
  - `dashboard/` – API del panel
    - `API.go` – endpoints del dashboard.
    - `router/Router.go` – ruteo HTTP.
    - `midelware/Tokens.go` – autenticación por tokens.
  - `Data/interfaces/` – contratos y módulos
    - `balancer/` – unified queue manager.
    - `client/` – información y métodos del cliente.
    - `comunication/` – estructuras de eventos/estados.
    - `providers/` – IP y procesos.
    - `utils/` – utilitarios (ej. procesos del sistema).
  - `handler_connections/Handler.go` – manejo de conexiones entrantes.
- `Database/` – conexión y contratos de base de datos.
- `Clients/`
  - `Go/` – SDK y demo.
  - `Python/` – paquete `synergynet_py` y demo.
  - `TS/` – SDK Node.js/TS y demo.
- `cmd/demo.go` – Demo de servidor.
- `docker-compose.yaml` / `Dockerfile` – despliegue y empaquetado.
- `Certs/` – certificados para conexiones seguras (opcional).

## Flujo de datos (alto nivel)

1. Clientes se conectan al servidor vía WebSocket (o protocolo definido).
2. Envían eventos y estados utilizando estructuras comunes (`EventSlice`, `Event_State`, `ClientInformation`).
3. El servidor enruta y balancea mensajes entre clientes, y expone endpoints de administración.
4. El dashboard puede consultar estados, métricas o gestionar tokens.
5. Opcionalmente, se persiste o se integra con una base de datos.

## Instalación y requisitos

### Prerrequisitos

- macOS/Linux/Windows.
- `Docker` y `docker-compose` (para despliegue rápido) o `Go >= 1.21` (para correr localmente).
- Para SDKs: `Python >= 3.10`, `Node.js >= 18`.

### Variables de entorno

Crear `.env` en la raíz (si no existe) y definir, por ejemplo:

```
# Ejemplos (ajusta a tu entorno)
PORT=8080
DASHBOARD_PORT=8081
JWT_SECRET=changeme
DB_URL=postgres://user:pass@host:5432/dbname
SSL_CERT_PATH=./Certs/server.crt
SSL_KEY_PATH=./Certs/server.key
```

### Arranque con Docker

```
docker compose up --build
```

Esto construye el servidor y levanta los servicios definidos en `docker-compose.yaml`.

### Arranque local (Go)

```
go mod download
go run ./cmd/demo.go
```

El servidor iniciará y aceptará conexiones de clientes. Revisa logs para puertos y rutas activas.

## Uso rápido: clientes oficiales

### Cliente Go

```
cd Clients/Go
go mod download
go run ./cmd/demo.go
```

- Implementa conexión y envío/recepción de eventos vía `Socket_client/`.

### Cliente Python

```
cd Clients/Python
python -m venv .venv
source .venv/bin/activate  # zsh/macOS
pip install -e .
python examples/demo.py
```

- El paquete `synergynet_py` expone `Client` y estructuras en `Data/interfaces/`.

### Cliente TypeScript (Node.js)

```
cd Clients/TS
npm install
npm run build
npm run demo
```

- El SDK incluye `client.ts`, `eventSlice.ts` y tipos para integraciones.

## Seguridad

- Middleware de tokens JWT para el dashboard (`midelware/Tokens.go`).
- Soporte de certificados TLS/SSL vía `Certs/` si configuras `SSL_CERT_PATH` y `SSL_KEY_PATH`.
- Recomendado ejecutar detrás de un proxy seguro y rotar secretos.

## Desarrollo y contribución

- Estilo de código y contratos en `Server/Data/interfaces/` para mantener consistencia.
- Mantener compatibilidad entre los SDKs (Go, Python, TS) respecto a estructuras de eventos y estados.
- Pull Requests y CI: ver `.github/workflows/` (tests de integración Python).

## Solución de problemas

- **Puertos ocupados:** ajusta `PORT` y `DASHBOARD_PORT` en `.env`.
- **Certificados/TLS:** verifica rutas en `Certs/` y permisos.
- **Dependencias:** ejecuta `go mod download`, `pip install -e .`, `npm install` según cliente.
- **Conexión WebSocket:** confirma la URL/host desde el cliente y que el servidor está corriendo.
- **Logs:** revisa salida del servidor y clientes; habilita niveles de log si corresponde.

## Licencia

Este repositorio no incluye una licencia explícita. Consulta al autor antes de redistribuir.

## Créditos

Desarrollado por HManuelCC y colaboradores. Agradecimientos a contribuyentes del ecosistema Go/Python/Node.
