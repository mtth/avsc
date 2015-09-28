# Avsc [![NPM version](https://img.shields.io/npm/v/avsc.svg)](https://www.npmjs.com/package/avsc) [![Build status](https://travis-ci.org/mtth/avsc.svg?branch=master)](https://travis-ci.org/mtth/avsc) [![Coverage status](https://coveralls.io/repos/mtth/avsc/badge.svg?branch=master&service=github)](https://coveralls.io/github/mtth/avsc?branch=master)

Pure JavaScript implementation of the [Avro specification](https://avro.apache.org/docs/current/spec.html).


## Installation

```bash
$ npm install avsc
```

`avsc` is compatible with `iojs` and versions of `node` from and including `0.11`.


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


## Status

What's already there:

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
