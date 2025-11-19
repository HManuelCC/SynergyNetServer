import { Event, ResponseCallback } from './types.js';
import type { Client } from './client.js';

export interface EventString {
  name: string;
  eventProcess: (event: Event, client: Client, messagePid: number, destination: string) => void;
}

export class EventsSubscribed {
  events: string[] = [];
}

export class EventSlice {
  private events: EventString[] = [];
  public subscribed: EventsSubscribed = new EventsSubscribed();

  addEvent(event: string, handle: (event: Event, client: Client, messagePid: number, destination: string) => void) {
    this.events.push({ name: event, eventProcess: handle });
    this.subscribed.events.push(event);
  }

  removeEvent(event: string) {
    this.events = this.events.filter(e => e.name !== event);
    this.subscribed.events = this.subscribed.events.filter(e => e !== event);
  }

  len(): number { return this.events.length; }

  async dispatch(e: Event, client: Client, messagePid: number, destination: string) {
    if (e.event === 'connect') {
      client.sendClientInfo(messagePid);
      return;
    }
    const found = this.events.find(ev => ev.name === e.event);
    if (found) {
      found.eventProcess(e, client, messagePid, destination);
    } else {
      client.sendState({
        status: false,
        message: 'No se puede reconocer el evento',
        error: 'Evento no reconocido',
        data: null,
        origen: e.destination,
        destination: e.origen,
        pid: e.pid
      }, messagePid, e.origen || '');
    }
  }
}

export const GlobalEventSlice = new EventSlice();
