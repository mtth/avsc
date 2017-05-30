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


## Documentation

+ [Home][home]
+ [API](https://github.com/mtth/avsc/wiki/API)
+ [Quickstart](https://github.com/mtth/avsc/wiki/Quickstart)
+ [Advanced usage](https://github.com/mtth/avsc/wiki/Advanced-usage)
+ [Benchmarks][benchmarks]


## Examples

Inside a node.js module, or using browserify:

```javascript
const avroTypes = require('@avro/types');
```

+ Encode and decode values from a known schema:

  ```javascript
  const type = avroTypes.Type.forSchema({
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
  const type = avroTypes.Type.forValue({
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


[benchmarks]: https://github.com/mtth/avsc/wiki/Benchmarks
[browser-support]: https://github.com/mtth/avsc/wiki#browser-support
[browserify]: http://browserify.org/
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
