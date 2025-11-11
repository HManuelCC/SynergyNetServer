import { NewClient } from './client.js';
import { GlobalEventSlice } from './eventSlice.js';
import { Event } from './types.js';
import http from 'http';

// Register events similar to Go demo
GlobalEventSlice.addEvent('login', (event, client, messagePid, destination) => {
  const username = event.data.username;
  const password = event.data.password;

  if (username === 'admin' && password === 'password123') {
    client.sendState({
      status: true,
      message: 'Login successful',
      error: '',
      data: { welcomeMessage: `Welcome back, ${username}!` },
      pid: event.pid,
    }, messagePid, destination);
  } else {
    client.sendState({
      status: true,
      message: 'Login failed',
      error: 'Invalid credentials',
      data: null,
      pid: event.pid,
    }, messagePid, destination);
  }
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
    const evt: Event = {
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
