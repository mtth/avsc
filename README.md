# Avsc [![NPM version](https://img.shields.io/npm/v/avsc.svg)](https://www.npmjs.com/package/avsc) [![Download count](https://img.shields.io/npm/dm/avsc.svg)](https://www.npmjs.com/package/avsc) [![CI](https://github.com/mtth/avsc/actions/workflows/ci.yml/badge.svg)](https://github.com/mtth/avsc/actions/workflows/ci.yml) [![Coverage status](https://coveralls.io/repos/mtth/avsc/badge.svg?branch=master&service=github)](https://coveralls.io/github/mtth/avsc?branch=master)

Pure JavaScript implementation of the [Avro
specification](https://avro.apache.org/docs/current/spec.html).


## Features

+ Blazingly [fast and compact][benchmarks] serialization! Typically faster than
  JSON with much smaller encodings.
+ All the Avro goodness and more: [type inference][type-inference], [schema
  evolution][schema-evolution]...
+ Support for [serializing arbitrary JavaScript objects][logical-types].
+ Unopinionated [64-bit integer compatibility][custom-long].


## Installation

```sh
$ npm install avsc
```


## Documentation

+ [Home][home]
+ [API](https://github.com/mtth/avsc/wiki/API)
+ [Quickstart](https://github.com/mtth/avsc/wiki/Quickstart)
+ [Advanced usage](https://github.com/mtth/avsc/wiki/Advanced-usage)
+ [Benchmarks][benchmarks]


## Examples

```javascript
const avro = require('avsc');
```

+ Encode and decode values from a known schema:

  ```javascript
  const type = avro.Type.forSchema({
    type: 'record',
    name: 'Pet',
    fields: [
      {
        name: 'kind',
        type: {type: 'enum', name: 'PetKind', symbols: ['CAT', 'DOG']}
      },
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

+ Get a [readable stream][readable-stream] of decoded values from an Avro
  container file (see the [`BlockDecoder` API][decoder-api] for an example
  compressed using [Snappy][snappy]):

  ```javascript
  avro.createFileDecoder('./values.avro')
    .on('metadata', function (type) { /* `type` is the writer's type. */ })
    .on('data', function (val) { /* Do something with the decoded value. */ });
  ```


[benchmarks]: https://github.com/mtth/avsc/wiki/Benchmarks
[browser-support]: https://github.com/mtth/avsc/wiki#browser-support
[custom-long]: https://github.com/mtth/avsc/wiki/Advanced-usage#custom-long-types
[decoder-api]: https://github.com/mtth/avsc/wiki/API#class-blockdecoderopts
[home]: https://github.com/mtth/avsc/wiki
[idl]: https://avro.apache.org/docs/current/idl.html
[logical-types]: https://github.com/mtth/avsc/wiki/Advanced-usage#logical-types
[node.js]: https://nodejs.org/en/
[readable-stream]: https://nodejs.org/api/stream.html#stream_class_stream_readable
[schema-evolution]: https://github.com/mtth/avsc/wiki/Advanced-usage#schema-evolution
[snappy]: https://avro.apache.org/docs/current/spec.html#snappy
[type-inference]: https://github.com/mtth/avsc/wiki/Advanced-usage#type-inference
