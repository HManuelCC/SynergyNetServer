export class EventsSubscribed {
    events = [];
}
export class EventSlice {
    events = [];
    subscribed = new EventsSubscribed();
    addEvent(event, handle) {
        this.events.push({ name: event, eventProcess: handle });
        this.subscribed.events.push(event);
    }
    removeEvent(event) {
        this.events = this.events.filter(e => e.name !== event);
        this.subscribed.events = this.subscribed.events.filter(e => e !== event);
    }
    len() { return this.events.length; }
    dispatch(e, client, messagePid, destination) {
        if (e.event === 'connect') {
            client.sendClientInfo(messagePid);
            return;
        }
        const found = this.events.find(ev => ev.name === e.event);
        if (found) {
            found.eventProcess(e, client, messagePid, destination);
        }
        else {
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
