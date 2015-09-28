# Avsc [![NPM version](https://img.shields.io/npm/v/avsc.svg)](https://www.npmjs.com/package/avsc) [![Build status](https://travis-ci.org/mtth/avsc.svg?branch=master)](https://travis-ci.org/mtth/avsc) [![Coverage status](https://coveralls.io/repos/mtth/avsc/badge.svg?branch=master&service=github)](https://coveralls.io/github/mtth/avsc?branch=master)

Pure JavaScript implementation of the [Avro specification](https://avro.apache.org/docs/current/spec.html).


## Features

+ Arbitrary Avro schema support, and [schema evolution][schema-evolution].
+ No dependencies.
+ [Fast!](#performance) Did you know that Avro could be faster than JSON?


## Installation

```bash
$ npm install avsc
```

`avsc` is compatible with [io.js][] and versions of [node.js][] from and
including `0.11`.


## Documentation

+ [Quickstart](wiki/Quickstart)
+ [API](wiki/API)
+ [Advanced usage](wiki/Advanced-usage)

A few examples to boot:

+ Encode and decode JavaScript objects using an Avro schema file:

  ```javascript
  var avsc = require('avsc'); // Implied in all other examples below.

  var type = avsc.parseFile('Person.avsc');
  var buf = type.encode({name: 'Ann', age: 25}); // Serialize a JS object.
  var obj = type.decode(buf); // And deserialize it back.
  ```

+ Get a readable record stream from an Avro container file:

  ```javascript
  avsc.decodeFile('records.avro')
    .on('data', function (record) { /* Do something with the record. */ });
  ```

+ Generate a random instance from a schema object:

  ```javascript
  var type = avsc.parse({
    name: 'Pet',
    type: 'record',
    fields: [
      {name: 'kind', type: {name: 'Kind', type: 'enum', symbols: ['CAT', 'DOG']}},
      {name: 'name', type: 'string'},
      {name: 'isFurry', type: 'boolean'}
    ]
  });
  var pet = type.random(); // E.g. {kind: 'CAT', name: 'qwXlrew', isFurry: true}
  ```

+ Create a writable stream to serialize objects on the fly:

  ```javascript
  var type = avsc.parse({type: 'array', items: 'int'});
  var encoder = new avsc.streams.RawEncoder(type)
    .on('data', function (chunk) { /* Use the encoded chunk somehow. */ });
  ```


## Performance

Despite being written in pure JavaScript, `avsc` is still fast: supporting
encoding and decoding throughput rates in the 100,000s per second for complex
schemas.

Schema | Decode (operations/sec) | Encode (operations/sec)
---|:-:|:-:
[`ArrayString.avsc`](blob/master/benchmarks/schemas/ArrayString.avsc)  | 905k | 280k
[`Coupon.avsc`](blob/master/benchmarks/schemas/Coupon.avsc) | 290k | 302k
[`Person.avsc`](blob/master/benchmarks/schemas/Person.avsc) | 1586k | 620k
[`User.avsc`](blob/master/benchmarks/schemas/User.avsc) | 116k | 284k

In fact, it is generally faster than the built-in JSON parser (also producing
encodings orders of magnitude smaller before compression). See the
[benchmarks][] page for the raw numbers.


## Limitations

+ Protocols aren't yet implemented.
+ JavaScript doesn't natively support the `long` type, so numbers larger than
  `Number.MAX_SAFE_INTEGER` (or smaller than the corresponding lower bound)
  will suffer a loss of precision.


[io.js]: https://iojs.org/en/
[node.js]: https://nodejs.org/en/
[benchmarks]: wiki/Benchmarks
[schema-evolution]: wiki/Advanced-usage#schema-evolution
