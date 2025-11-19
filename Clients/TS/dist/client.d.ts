import { Event, State, ResponseCallback } from './types.js';
export declare class Client {
    private host;
    private port;
    private name;
    private apiKey?;
    private useTLS;
    private socket?;
    private closed;
    private attempt;
    private minBackoff;
    private maxBackoff;
    private processes;
    private buffer;
    private lastHandshakeLatency;
    constructor(host: string, port: number, clientName: string, apiKey?: string, useTLS?: boolean);
    close(): void;
    private connectLoop;
    private connectOnce;
    private backoff;
    private readLoop;
    private processBuffer;
    private sendMessageState;
    sendState(state: State, messagePid: number, destination: string): void;
    send(event: Event, timeoutMs?: number, cb?: ResponseCallback): Promise<State>;
    sendClientInfo(messagePid: number): Promise<void>;
    private getSystemStats;
}
export declare function NewClient(host: string, port: number, clientName: string, apiKey?: string, useTLS?: boolean): Client;
