# Avsc [![NPM version](https://img.shields.io/npm/v/avsc.svg)](https://www.npmjs.com/package/avsc) [![Build status](https://travis-ci.org/mtth/avsc.svg?branch=master)](https://travis-ci.org/mtth/avsc) [![Coverage status](https://coveralls.io/repos/mtth/avsc/badge.svg?branch=master&service=github)](https://coveralls.io/github/mtth/avsc?branch=master)

Pure JavaScript implementation of the [Avro specification](https://avro.apache.org/docs/current/spec.html).


## Features

+ Arbitrary Avro schema support.
+ [Fast](#performance).
+ No dependencies.


## Installation

```bash
$ npm install avsc
```

`avsc` is compatible with [io.js][] and versions of [node.js][] from and
including `0.11`.


## Examples

+ Encode, validate, and decode JavaScript objects using an existing Avro
  schema:

  ```javascript
  var type = require('avsc').parseFile('Person.avsc');
  var buf = type.encode({name: 'Ann', age: 25}); // Serialize a JS object.
  var isValid = type.isValid({name: 'Bob', age: -1}) // Validate another.
  var obj = type.decode(buf); // And deserialize the first back.
  ```

+ Get a readable record stream from an Avro container file:

  ```javascript
  require('avsc')
    .decodeFile('records.avro')
    .on('data', function (record) { /* Do something with the record. */ });
  ```

+ Generate an Avro type from a schema object and generate a random instance:

  ```javascript
  var type = require('avsc').parse({
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
  var avsc = require('avsc');
  var type = avsc.parse('int');
  var encoder = new avsc.streams.RawEncoder(type)
    .on('data', function (chunk) { /* Use the encoded chunk somehow. */ });
  ```

## Documentation

+ [Quickstart](https://github.com/mtth/avsc/blob/master/doc/quickstart.md)
+ [API](https://github.com/mtth/avsc/blob/master/doc/api.md)
+ [Advanced usage](https://github.com/mtth/avsc/blob/master/doc/advanced.md)


## Performance

Despite being written in pure JavaScript, `avsc` is still fast: supporting
encoding and decoding throughput rates in the 100,000s per second for complex
schemas.

Schema | Decode (ops/sec) | Encode (ops/sec)
---|:-:|:-:
[`ArrayString.avsc`](https://github.com/mtth/avsc/blob/master/benchmarks/schemas/ArrayString.avsc)  | 905k | 280k
[`Coupon.avsc`](https://github.com/mtth/avsc/blob/master/benchmarks/schemas/Coupon.avsc) | 290k | 302k
[`Person.avsc`](https://github.com/mtth/avsc/blob/master/benchmarks/schemas/Person.avsc) | 1586k | 620k
[`User.avsc`](https://github.com/mtth/avsc/blob/master/benchmarks/schemas/User.avsc) | 116k | 284k

In fact, it is generally faster than the built-in JSON parser (also producing
encodings orders of magnitude smaller before compression). See the
[benchmarks][] page for the raw numbers.


## Status

What's there:

+ Parsing and resolving schemas (including schema evolution).
+ Encoding, decoding, validating, and generating data.
+ Reading and writing container files.

Coming up:

+ Protocols.
+ Sort order.
+ Logical types.

Known limitations:

+ JavaScript doesn't natively support the `long` type, so numbers larger than
  `Number.MAX_SAFE_INTEGER` (or smaller than the corresponding lower bound)
  will suffer a loss of precision.


[io.js]: https://iojs.org/en/
[node.js]: https://nodejs.org/en/
[benchmarks]: https://github.com/mtth/avsc/blob/master/doc/benchmarks.md
