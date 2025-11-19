import { NewClient } from './client.js';
import { GlobalEventSlice } from './eventSlice.js';
import http from 'http';
// Register events similar to Go demo
GlobalEventSlice.addEvent('login', (event, client, messagePid, destination) => {
    const username = event.data.username;
    const password = event.data.password;
    var evt = {
        event: 'registro', data: null, destination: destination
    };
    client.send(evt, 5000).then(response => {
        client.sendState(response, messagePid, destination);
    }).catch(err => {
        console.error('Error enviando evento de login:', err);
    });
});
GlobalEventSlice.addEvent('registro', (event, client, messagePid, destination) => {
    client.sendState({
        status: true,
        message: 'Hola amigo',
        error: '',
        data: null,
        pid: event.pid,
    }, messagePid, destination);
});
const client = NewClient('localhost', 443, 'test_ts', undefined, false);
const server = http.createServer((req, res) => {
    if (req.url?.startsWith('/login_prueba')) {
        const url = new URL(req.url, 'http://localhost');
        const username = url.searchParams.get('username') || 'anon';
        const evt = {
            event: 'login',
            data: { username, password: 'test_password' },
            origen: 'test_ts'
        };
        client.send(evt, 5000).then(state => {
            res.writeHead(200, { 'Content-Type': 'application/json' });
            res.end(JSON.stringify(state));
        }).catch(err => {
            res.writeHead(500, { 'Content-Type': 'text/plain' });
            res.end('Error: ' + err.message);
        });
        return;
    }
    res.writeHead(404);
    res.end();
});
server.listen(8082, () => console.log('Demo HTTP listening on 8082'));
