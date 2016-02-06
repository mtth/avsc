# Avsc [![NPM version](https://img.shields.io/npm/v/avsc.svg)](https://www.npmjs.com/package/avsc) [![Build status](https://travis-ci.org/mtth/avsc.svg?branch=master)](https://travis-ci.org/mtth/avsc) [![Coverage status](https://coveralls.io/repos/mtth/avsc/badge.svg?branch=master&service=github)](https://coveralls.io/github/mtth/avsc?branch=master)

Pure JavaScript implementation of the [Avro
specification](https://avro.apache.org/docs/current/spec.html).


## Features

+ Blazingly [fast and compact][benchmarks] serialization! Typically faster than
  JSON with much smaller encodings.
+ All the Avro goodness, including [schema evolution][schema-evolution] and
  [remote procedure calls][rpc].
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
var avro = require('avsc');
```

+ Encode and decode values:

  ```javascript
  var type = avro.parse({
    name: 'Pet',
    type: 'record',
    fields: [
      {name: 'kind', type: {name: 'Kind', type: 'enum', symbols: ['CAT', 'DOG']}},
      {name: 'name', type: 'string'}
    ]
  });
  var buf = type.toBuffer({kind: 'CAT', name: 'Albert'}); // Encoded buffer.
  var val = type.fromBuffer(buf); // {kind: 'CAT', name: 'Albert'}
  ```

+ Get a [readable stream][readable-stream] of decoded values from an Avro
  container file:

  ```javascript
  avro.createFileDecoder('./values.avro')
    .on('metadata', function (type) { /* `type` is the writer's type. */ })
    .on('data', function (val) { /* Do something with the decoded value. */ });
  ```

+ Implement a TCP server for an [IDL-defined][idl] protocol:

  ```javascript
  avro.assemble('./Ping.avdl', function (err, attrs) {
    var protocol = avro.parse(attrs);
    protocol.on('ping', function (req, ee, cb) { cb(null, 'pong'); });
    require('net').createServer()
      .on('connection', function (con) { protocol.createListener(con); })
      .listen(8000);
  });
  ```


[node.js]: https://nodejs.org/en/
[benchmarks]: https://github.com/mtth/avsc/wiki/Benchmarks
[schema-evolution]: https://github.com/mtth/avsc/wiki/Advanced-usage#schema-evolution
[logical-types]: https://github.com/mtth/avsc/wiki/Advanced-usage#logical-types
[custom-long]: https://github.com/mtth/avsc/wiki/Advanced-usage#custom-long-types
[readable-stream]: https://nodejs.org/api/stream.html#stream_class_stream_readable
[browserify]: http://browserify.org/
[browser-support]: https://github.com/mtth/avsc/wiki#browser-support
[home]: https://github.com/mtth/avsc/wiki
[rpc]: https://github.com/mtth/avsc/wiki/Advanced-usage#remote-procedure-calls
[releases]: https://github.com/mtth/avsc/releases
[idl]: https://avro.apache.org/docs/current/idl.html
