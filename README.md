# Avro [![Build status](https://travis-ci.org/mtth/avsc.svg?branch=master)](https://travis-ci.org/mtth/avsc)  [![Download count](https://img.shields.io/npm/dm/avsc.svg)](https://www.npmjs.com/package/avsc) [![Coverage status](https://coveralls.io/repos/mtth/avsc/badge.svg?branch=master&service=github)](https://coveralls.io/github/mtth/avsc?branch=master)

Pure JavaScript implementation of the [Avro
specification](https://avro.apache.org/docs/current/spec.html).

## Features

+ Blazingly fast and compact serialization! Typically faster than JSON with
  much smaller encodings.
+ All the Avro goodness and more: [type inference][type-inference], [schema
  evolution][schema-evolution], and [remote procedure calls][rpc].
+ Support for [serializing arbitrary JavaScript objects][logical-types].
+ Unopinionated [64-bit integer compatibility][custom-long].

## Installation

The functionality is available via individual modules, to help minimize client
bundle size:

+ Serialization: [`@avro/types`](https://www.npmjs.com/package/@avro/types)
+ Streams (e.g. to read and write container files):
  [`@avro/streams`](https://www.npmjs.com/package/@avro/streams)
+ IDL support: [`@avro/idl`](https://www.npmjs.com/package/@avro/idl)
+ RPC services: [`@avro/services`](https://www.npmjs.com/package/@avro/services)

For convenience and compatibility with previous versions, all non-RPC
functionality is also available via:

```bash
$ npm install avsc
```

## Documentation

+ [Home][home]
+ [API](https://github.com/mtth/avsc/wiki/API)
+ [Quickstart](https://github.com/mtth/avsc/wiki/Quickstart)
+ [Advanced usage](https://github.com/mtth/avsc/wiki/Advanced-usage)

## Examples

+ Encode and decode values from a known schema:

  ```javascript
  const {Type} = require('@avro/types');

  const type = Type.forSchema({
    type: 'record',
    fields: [
      {name: 'kind', type: {type: 'enum', symbols: ['CAT', 'DOG']}},
      {name: 'name', type: 'string'}
    ]
  });

  const buf = type.binaryEncode({kind: 'CAT', name: 'Albert'}); // Buffer.
  const val = type.binaryDecode(buf); // = {kind: 'CAT', name: 'Albert'}
  ```

+ Infer a value's schema and encode similar values:

  ```javascript
  const {Type} = require('@avro/types');

  const type = Type.forValue({
    city: 'Cambridge',
    zipCodes: ['02138', '02139'],
    visits: 2
  });

  // We can use `type` to encode any values with the same structure:
  const bufs = [
    type.binaryEncode({city: 'Seattle', zipCodes: ['98101'], visits: 3}),
    type.binaryEncode({city: 'NYC', zipCodes: [], visits: 0})
  ];
  ```

+ Get a [readable stream][readable-stream] of decoded values from an Avro
  container file compressed using [Snappy][snappy] (see the [`BlockDecoder`
  API][decoder-api] for an example including checksum validation):

  ```javascript
  const {createFileDecoder} = require('@avro/streams');
  const snappy = require('snappy'); // Or your favorite Snappy library.

  const codecs = {
    snappy: function (buf, cb) {
      // Avro appends checksums to compressed blocks, which we skip here.
      return snappy.uncompress(buf.slice(0, buf.length - 4), cb);
    }
  };

  createFileDecoder('./values.avro', {codecs})
    .on('metadata', function (type) { /* `type` is the writer's type. */ })
    .on('data', function (val) { /* Do something with the decoded value. */ });
  ```

+ Implement a TCP server for a protocol:

  ```javascript
  const {NettyGateway, Server, Service} = require('@avro/services');

  // We first define a service.
  const service = new avro.Service({
    protocol: 'LengthService',
    messages: {
      stringLength: {
        request: [{name: 'str', type: 'string'}],
        response: 'int'
      }
    }
  });

  // We then create a corresponding server, implementing our endpoint.
  const server = new Server(service)
    .onMessage().stringLength((str) => str.length);

  // Finally, we use our server to respond to incoming TCP connections!
  const gateway = new NettyGateway(server.channel());
  require('net').createServer()
    .on('connection', (con) => { gateway.accept(con); })
    .listen(24950);
  ```


[custom-long]: https://github.com/mtth/avsc/wiki/Advanced-usage#custom-long-types
[decoder-api]: https://github.com/mtth/avsc/wiki/API#class-blockdecoderopts
[home]: https://github.com/mtth/avsc/wiki
[idl]: https://avro.apache.org/docs/current/idl.html
[logical-types]: https://github.com/mtth/avsc/wiki/Advanced-usage#logical-types
[node.js]: https://nodejs.org/en/
[readable-stream]: https://nodejs.org/api/stream.html#stream_class_stream_readable
[releases]: https://github.com/mtth/avsc/releases
[rpc]: https://github.com/mtth/avsc/wiki/Quickstart#services
[schema-evolution]: https://github.com/mtth/avsc/wiki/Advanced-usage#schema-evolution
[snappy]: https://avro.apache.org/docs/current/spec.html#snappy
[type-inference]: https://github.com/mtth/avsc/wiki/Advanced-usage#type-inference
