# Avro services

Avro-powered RPC services.

## Features

+ Schema evolution
+ "Type safe" APIs
+ Efficient wire encoding

## Examples

### In-process client and server

```javascript
const {Client, Server, Service, Trace} = require('@avro/services');

const stringService = new Service({
  protocol: 'StringService',
  messages: {
    upperCase: {
      request: [{name: 'message', type: 'string'}],
      response: 'string',
    },
  },
});

const stringServer = new Server(stringService)
  .onMessage().upperCase((str, cb) => { cb(null, str.toUpperCase()); });

const stringClient = new Client(stringService);

// Point the client to our server.
stringClient.channel = stringServer.channel;

stringClient.emitMessage(new Trace()).upperCase('hello!', (err, str) => {
  if (err) {
    throw err;
  }
  console.log(str); // HELLO!
});
```

### TCP server hosting two services

```javascript
const {Router, Server, Service, netty} = require('@avro/services');
const net = require('net');

const echoService = new Service({
  protocol: 'Echo',
  messages: {
    echo: {
      request: [{name: 'message', type: 'string'}],
      response: 'string',
    },
  },
});

const upperService = new Service({
  protocol: 'Upper',
  messages: {
    upper: {
      request: [{name: 'message', type: 'string'}],
      response: 'string',
    },
  },
});

const echoServer = new Server(echoService)
  .onMessage().echo((str, cb) => { cb(null, str); });

const upperServer = new Server(upperService)
  .onMessage().upper((str, cb) => { cb(null, str.toUpperCase()); });

const gateway = new netty.Gateway(Router.forServers(echoServer, upperServer));

net.createServer()
  .on('connection', (conn) => {
    gateway.accept(conn.on('error', (err) => { console.error(err); }));
  })
  .listen(8080, () => { console.log('Listening...'); });
```
