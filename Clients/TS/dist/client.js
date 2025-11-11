import net from 'net';
import tls from 'tls';
import { randomInt } from 'crypto';
import si from 'systeminformation';
import { GlobalEventSlice } from './eventSlice.js';
// Header structure (client side send):
// For Event send: [1 byte type=1][4 bytes size][payload]
// Go server expects: [type][PID][size][payload]. Adjust accordingly to match server.
// From server we receive: [1 byte type][4 bytes PID][4 bytes size][payload]
// We'll replicate server send format when we emit.
function packNoPid(type, json) {
    const sizeBuf = Buffer.alloc(4);
    sizeBuf.writeUInt32BE(json.length, 0);
    return Buffer.concat([Buffer.from([type]), sizeBuf, json]);
}
export class Client {
    host;
    port;
    name;
    apiKey;
    useTLS;
    socket;
    closed = false;
    attempt = 1;
    minBackoff = 1000; // ms
    maxBackoff = 30000; // ms
    processes = [];
    buffer = Buffer.alloc(0);
    lastHandshakeLatency = 0;
    constructor(host, port, clientName, apiKey, useTLS = false) {
        this.host = host;
        this.port = port;
        this.name = clientName;
        this.apiKey = apiKey;
        this.useTLS = useTLS;
        this.connectLoop();
    }
    close() {
        this.closed = true;
        this.socket?.destroy();
    }
    async connectLoop() {
        while (!this.closed) {
            try {
                const start = Date.now();
                await this.connectOnce();
                const handshakeLatency = Date.now() - start;
                this.lastHandshakeLatency = handshakeLatency;
                console.log(`Conectado a ${this.host}:${this.port} (latencia de handshake: ${handshakeLatency} ms)`);
                // Start reader
                this.readLoop(handshakeLatency);
                // Wait until socket ends
                await new Promise(res => {
                    this.socket.once('close', () => res());
                    this.socket.once('end', () => res());
                    this.socket.once('error', () => res());
                });
                if (this.closed)
                    return;
                this.attempt = 1; // reset after a full session
            }
            catch (e) {
                const delay = this.backoff();
                await new Promise(r => setTimeout(r, delay));
                this.attempt++;
            }
        }
    }
    connectOnce() {
        return new Promise((resolve, reject) => {
            const onError = (err) => {
                cleanup();
                reject(err);
            };
            const onConnect = () => {
                cleanup();
                resolve();
            };
            const cleanup = () => {
                sock.removeListener('error', onError);
                sock.removeListener('connect', onConnect);
            };
            const sock = this.useTLS
                ? tls.connect({ host: this.host, port: this.port, rejectUnauthorized: false })
                : net.connect({ host: this.host, port: this.port });
            this.socket = sock;
            sock.once('error', onError);
            sock.once('connect', onConnect);
        });
    }
    backoff() {
        let base = this.minBackoff * Math.pow(2, this.attempt - 1);
        if (base > this.maxBackoff)
            base = this.maxBackoff;
        const jitter = Math.floor(Math.random() * (base / 5));
        return Math.random() < 0.5 ? base - jitter : base + jitter;
    }
    readLoop(latency) {
        if (!this.socket)
            return;
        this.socket.on('data', (chunk) => {
            this.buffer = Buffer.concat([this.buffer, chunk]);
            this.processBuffer(latency);
        });
    }
    processBuffer(latency) {
        // Expect frames: [type(1)][pid(4)][size(4)] then payload
        while (this.buffer.length >= 9) {
            const type = this.buffer.readUInt8(0);
            const pid = this.buffer.readUInt32BE(1);
            const size = this.buffer.readUInt32BE(5);
            if (this.buffer.length < 9 + size)
                return; // wait more
            const payload = this.buffer.slice(9, 9 + size);
            this.buffer = this.buffer.slice(9 + size);
            try {
                switch (type) {
                    case 1: { // Event
                        const event = JSON.parse(payload.toString());
                        // ack process status message 1
                        this.sendMessageState({ status: true, server_pid: pid, state: 'El servidor proceso la solicitud', error: '', process_status: 1 });
                        GlobalEventSlice.dispatch(event, this, pid, event.origen || '');
                        break;
                    }
                    case 2: { // State
                        const state = JSON.parse(payload.toString());
                        this.sendMessageState({ status: true, server_pid: pid, state: 'El servidor proceso la solicitud', error: '', process_status: 2 });
                        // Match process by state.pid
                        const trackerIndex = this.processes.findIndex(p => p.pid === state.pid);
                        if (trackerIndex >= 0) {
                            const tracker = this.processes[trackerIndex];
                            clearTimeout(tracker.timeout);
                            tracker.resolve(state);
                            this.processes.splice(trackerIndex, 1);
                        }
                        else {
                            // log stray state
                            console.log('Estado sin proceso activo:', state);
                        }
                        break;
                    }
                    case 3: { // MessageState
                        const msgState = JSON.parse(payload.toString());
                        // Could update process status here if needed
                        break;
                    }
                    default:
                        console.log('Tipo de mensaje desconocido:', type);
                }
            }
            catch (err) {
                console.error('Error procesando frame:', err);
            }
        }
    }
    sendMessageState(msg) {
        if (!this.socket)
            return;
        const buf = Buffer.from(JSON.stringify(msg));
        const packet = packNoPid(3, buf);
        this.socket.write(packet);
    }
    sendState(state, messagePid, destination) {
        if (!this.socket)
            return;
        state.destination = destination;
        // Primero avisamos MessageState (ProcessStatus=2) y luego enviamos el State
        this.sendMessageState({ status: true, server_pid: messagePid, state: 'El servidor proceso la solicitud', error: '', process_status: 2 });
        const buf = Buffer.from(JSON.stringify(state));
        const packet = packNoPid(2, buf);
        this.socket.write(packet);
    }
    async send(event, timeoutMs, cb) {
        if (!this.socket)
            throw new Error('no hay conexión activa');
        const pid = randomInt(1_000_000);
        event.pid = pid;
        const buf = Buffer.from(JSON.stringify(event));
        const packet = packNoPid(1, buf);
        this.socket.write(packet);
        if (cb || timeoutMs) {
            return new Promise((resolve, reject) => {
                const to = setTimeout(() => {
                    // remove tracker
                    this.processes = this.processes.filter(p => p.pid !== pid);
                    reject(new Error(`timeout esperando respuesta para PID ${pid}`));
                }, timeoutMs ?? 15000);
                this.processes.push({ pid, resolve: (s) => { cb?.(s); resolve(s); }, reject, timeout: to });
            });
        }
    }
    async sendClientInfo(messagePid) {
        try {
            const stats = await this.getSystemStats();
            const info = {
                client_name: this.name.toUpperCase(),
                latency: this.lastHandshakeLatency,
                resources: stats,
                events: { events: GlobalEventSlice.subscribed.events }
            };
            // send as State back
            this.sendState({ status: true, state: 'Cliente conectado con exito.', error: '', data: JSON.stringify(info), origen: this.name }, messagePid, '127.0.0.1');
        }
        catch (err) {
            this.sendState({ status: false, state: 'Error al convertir la información del cliente a JSON', error: String(err), data: null, origen: this.name }, messagePid, '127.0.0.1');
        }
    }
    async getSystemStats() {
        const cpuLoad = await si.currentLoad();
        const mem = await si.mem();
        const fsSize = await si.fsSize();
        const diskIOStart = await si.disksIO();
        await new Promise(r => setTimeout(r, 1000));
        const diskIOEnd = await si.disksIO();
        const readDelta = diskIOEnd.rIO - diskIOStart.rIO;
        const writeDelta = diskIOEnd.wIO - diskIOStart.wIO;
        const totalIO = readDelta + writeDelta;
        const maxThroughput = 100 * 1024 * 1024; // 100MB/s heuristic
        let diskBusy = totalIO > 0 ? (totalIO / maxThroughput) * 100 : 0;
        if (diskBusy > 100)
            diskBusy = 100;
        return {
            CPUUsage: cpuLoad.currentLoad,
            MemoryUsage: (mem.used / mem.total) * 100,
            DiskUsage: fsSize.length ? (fsSize.reduce((a, d) => a + d.used, 0) / fsSize.reduce((a, d) => a + d.size, 0)) * 100 : 0,
            DiskBusy: diskBusy
        };
    }
}
export function NewClient(host, port, clientName, apiKey, useTLS = false) {
    return new Client(host, port, clientName, apiKey, useTLS);
}
