# Avro services

Avro-powered RPC services.

## Features

+ Schema evolution
+ "Type safe" APIs
+ Efficient wire encoding

## Examples

### In-process client and server

```javascript
const {Client, Deadline, Server, Service, Trace} = require('@avro/services');

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

const stringClient = stringServer.client(); // In-process client.

stringClient.emitMessage(Deadline.forMillis(100))
  .upperCase('hello!', (err, str) => {
    if (err) {
      throw err;
    }
    console.log(str); // HELLO!
  });
```

### TCP server hosting two services

```javascript
const {NettyGateway, RoutingChannel, Server, Service} = require('@avro/services');
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

const channel = RoutingChannel.forServers([echoServer, upperServer]);
const gateway = new NettyGateway(channel);

net.createServer()
  .on('connection', (conn) => {
    gateway.accept(conn.on('error', (err) => { console.error(err); }));
  })
  .listen(8080, () => { console.log('Listening...'); });
```
