import { Event } from './types.js';
import type { Client } from './client.js';
export interface EventString {
    name: string;
    eventProcess: (event: Event, client: Client, messagePid: number, destination: string) => void;
}
export declare class EventsSubscribed {
    events: string[];
}
export declare class EventSlice {
    private events;
    subscribed: EventsSubscribed;
    addEvent(event: string, handle: (event: Event, client: Client, messagePid: number, destination: string) => void): void;
    removeEvent(event: string): void;
    len(): number;
    dispatch(e: Event, client: Client, messagePid: number, destination: string): Promise<void>;
}
export declare const GlobalEventSlice: EventSlice;
