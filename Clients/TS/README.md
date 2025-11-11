# SynergyNet TypeScript Client

Cliente TypeScript que replica el comportamiento de `Clients/Go` para el protocolo de SynergyNet.

- Conexión TCP/TLS con reconexión exponencial.
- Protocolo binario idéntico:
  - Cliente→Servidor: [tipo(1)][tamaño(4)][JSON].
  - Servidor→Cliente: [tipo(1)][PID(4)][tamaño(4)][JSON].
- Registro y despacho de eventos (`EventSlice`).
- Respuesta automática al evento `connect` con información del cliente (CPU, memoria, disco) usando `systeminformation`.
- Envío de `MessageState` (status 1/2) como en el cliente Go.

## Estructura

```
Clients/TS/
  src/
    client.ts      # Cliente TCP/TLS con framing y reconexión
    eventSlice.ts  # Registro de eventos y dispatch
    types.ts       # Tipos (Event, State, MessageState, ClientInformation)
    demo.ts        # Demo con endpoint HTTP
    index.ts       # Exports
  package.json
  tsconfig.json
  README.md
```

## Publicación como librería

### Estructura de build

Al compilar (`npm run build`) se generan archivos en `dist/` con módulos ESM y declaraciones `.d.ts` listos para publicar. El paquete expone su API principal mediante `exports` en `package.json`.

### Empaquetar localmente (sin publicar)

```sh
cd Clients/TS
npm install
npm run build
npm pack   # genera un archivo .tgz, p.ej. synergynet-ts-client-0.1.0.tgz
```

Luego en otro proyecto (para pruebas locales):

```sh
npm install ../ruta/a/synergynet-ts-client-0.1.0.tgz
```

### Publicar en npm

1. Ajusta la versión en `package.json` (semver).
2. Inicia sesión si no lo has hecho:
   ```sh
   npm login
   ```
3. Publica:
   ```sh
   npm publish --access public
   ```

> Asegúrate de que el nombre del paquete (`name`) no colisiona con uno existente. Si ya existe, cámbialo (por ejemplo `@tu-usuario/synergynet-client`).

### Consumir desde otro proyecto

```sh
npm install synergynet-ts-client
# o si usaste scope
npm install @tu-usuario/synergynet-client
```

Uso en código TypeScript / ES Modules:

```ts
import { NewClient, GlobalEventSlice } from "synergynet-ts-client";

GlobalEventSlice.addEvent("registro", (event, client, pid, destination) => {
  client.sendState(
    {
      status: true,
      state: "Hola amigo",
      error: "",
      data: null,
      pid: event.pid,
      origen: "consumer_app",
    },
    pid,
    destination
  );
});

const client = NewClient("localhost", 443, "consumer_app");

client
  .send(
    {
      event: "login",
      data: { username: "bob", password: "pw" },
      origen: "consumer_app",
    },
    5000
  )
  .then((state) => console.log("Respuesta", state))
  .catch((err) => console.error("Error", err));
```

### Notas de interoperabilidad

- El paquete es ESM. Si usas CommonJS, puedes importar dinámicamente:
  ```js
  (async () => {
    const mod = await import("synergynet-ts-client");
    const { NewClient } = mod;
  })();
  ```
- Requiere Node.js >= 18 para estabilidad en APIs y soporte ESM.

---

## Uso rápido (demo incluida)

1. Instalar dependencias

```sh
cd Clients/TS
npm install
```

2. Compilar y ejecutar demo

```sh
npx tsc -p .
node dist/demo.js
```

La demo arranca un HTTP en `http://localhost:8082` y crea un cliente `test_ts` hacia `localhost:443`.

3. Probar endpoint

```sh
curl "http://localhost:8082/login_prueba?username=alice"
```

## API básica

```ts
import { NewClient, GlobalEventSlice } from "./dist/index.js";

GlobalEventSlice.addEvent(
  "registro",
  (event, client, messagePid, destination) => {
    client.sendState(
      {
        status: true,
        state: "Hola amigo",
        error: "",
        data: null,
        pid: event.pid,
        origen: "ts_app",
      },
      messagePid,
      destination
    );
  }
);

const c = NewClient("localhost", 443, "test_ts", undefined, false);

// Enviar un evento y esperar respuesta
c.send(
  {
    event: "login",
    data: { username: "bob", password: "pw" },
    origen: "ts_app",
  },
  5000
).then((state) => {
  console.log("Respuesta", state);
});
```

## Notas

- Este cliente es idéntico en protocolo al de Go: las cabeceras y el orden de `MessageState` y `State` están respetados.
- Para TLS se usa `rejectUnauthorized: false` (equivalente a `InsecureSkipVerify: true` en Go). Ajusta esto si usas certificados válidos.
