# Avsc [![NPM version](https://img.shields.io/npm/v/avsc.svg)](https://www.npmjs.com/package/avsc) [![Download count](https://img.shields.io/npm/dm/avsc.svg)](https://www.npmjs.com/package/avsc) [![Build status](https://travis-ci.org/mtth/avsc.svg?branch=master)](https://travis-ci.org/mtth/avsc) [![Coverage status](https://coveralls.io/repos/mtth/avsc/badge.svg?branch=master&service=github)](https://coveralls.io/github/mtth/avsc?branch=master)

Pure JavaScript implementation of the [Avro
specification](https://avro.apache.org/docs/current/spec.html).


## Features

+ Blazingly [fast and compact][benchmarks] serialization! Typically faster than
  JSON with much smaller encodings.
+ All the Avro goodness and more: [type inference][type-inference], [schema
  evolution][schema-evolution], and [remote procedure calls][rpc].
+ Support for [serializing arbitrary JavaScript objects][logical-types].
+ Unopinionated [64-bit integer compatibility][custom-long].


## Installation

```bash
$ npm install avsc
```

`avsc` is compatible with all versions of [node.js][] since `0.11` and major
browsers via [browserify][] (see the full compatibility table
[here][browser-support]). For convenience, you can also find compiled
distributions with the [releases][] (but please host your own copy).


## Documentation

+ [Home][home]
+ [API](https://github.com/mtth/avsc/wiki/API)
+ [Quickstart](https://github.com/mtth/avsc/wiki/Quickstart)
+ [Advanced usage](https://github.com/mtth/avsc/wiki/Advanced-usage)
+ [Benchmarks][benchmarks]


## Examples

Inside a node.js module, or using browserify:

```javascript
const avro = require('avsc');
```

+ Encode and decode values from a known schema:

  ```javascript
  const type = avro.Type.forSchema({
    type: 'record',
    fields: [
      {name: 'kind', type: {type: 'enum', symbols: ['CAT', 'DOG']}},
      {name: 'name', type: 'string'}
    ]
  });

  const buf = type.toBuffer({kind: 'CAT', name: 'Albert'}); // Encoded buffer.
  const val = type.fromBuffer(buf); // = {kind: 'CAT', name: 'Albert'}
  ```

+ Infer a value's schema and encode similar values:

  ```javascript
  const type = avro.Type.forValue({
    city: 'Cambridge',
    zipCodes: ['02138', '02139'],
    visits: 2
  });

  // We can use `type` to encode any values with the same structure:
  const bufs = [
    type.toBuffer({city: 'Seattle', zipCodes: ['98101'], visits: 3}),
    type.toBuffer({city: 'NYC', zipCodes: [], visits: 0})
  ];
  ```

+ Use a [writable stream][writable-stream] to write or append encoded values to an Avro
  container file:

  ```javascript
  const type = avro.Type.forValue({
    city: 'Cambridge',
    zipCodes: ['02138', '02139'],
    visits: 2
  });

  const opts =  {};
  
  // Use this option if you like to flush values as soon as possible using 1 block per value
  // (better for continuous writing certaintiy at the cost of performance and larger file sizes)
  opts.writeFlushed = true;

  const encoder = avro.createFileAppender('./values.avro', type, opts);
  encoder.write({city: 'Seattle', zipCodes: ['98101'], visits: 3});
  encoder.end({city: 'New York', zipCodes: ['10001'], visits: 47});
  ```

+ Get a [readable stream][readable-stream] of decoded values from an Avro
  container file:

  ```javascript
  avro.createFileDecoder('./values.avro')
    .on('metadata', (type) => { /* `type` is the writer's type. */ })
    .on('data', (val) => { /* Do something with the decoded value. */ });
  ```

+ Implement a TCP server for an [IDL-defined][idl] protocol:

  ```javascript
  // We first generate a protocol from its IDL specification.
  const protocol = avro.readProtocol(`
    protocol LengthService {
      /** Endpoint which returns the length of the input string. */
      int stringLength(string str);
    }
  `);

  // We then create a corresponding server, implementing our endpoint.
  const server = avro.Service.forProtocol(protocol)
    .createServer()
    .onStringLength(function (str, cb) { cb(null, str.length); });

  // Finally, we use our server to respond to incoming TCP connections!
  require('net').createServer()
    .on('connection', (con) => { server.createChannel(con); })
    .listen(24950);
  ```


[node.js]: https://nodejs.org/en/
[benchmarks]: https://github.com/mtth/avsc/wiki/Benchmarks
[type-inference]: https://github.com/mtth/avsc/wiki/Advanced-usage#type-inference
[schema-evolution]: https://github.com/mtth/avsc/wiki/Advanced-usage#schema-evolution
[logical-types]: https://github.com/mtth/avsc/wiki/Advanced-usage#logical-types
[custom-long]: https://github.com/mtth/avsc/wiki/Advanced-usage#custom-long-types
[readable-stream]: https://nodejs.org/api/stream.html#stream_class_stream_readable
[writable-stream]: https://nodejs.org/api/stream.html#stream_class_stream_writable
[browserify]: http://browserify.org/
[browser-support]: https://github.com/mtth/avsc/wiki#browser-support
[home]: https://github.com/mtth/avsc/wiki
[rpc]: https://github.com/mtth/avsc/wiki/Quickstart#services
[releases]: https://github.com/mtth/avsc/releases
[idl]: https://avro.apache.org/docs/current/idl.html
