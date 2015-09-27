# Avsc [![NPM version](https://img.shields.io/npm/v/avsc.svg)](https://www.npmjs.com/package/avsc) [![Build status](https://travis-ci.org/mtth/avsc.svg?branch=master)](https://travis-ci.org/mtth/avsc) [![Coverage status](https://coveralls.io/repos/mtth/avsc/badge.svg?branch=master&service=github)](https://coveralls.io/github/mtth/avsc?branch=master)

Pure JavaScript implementation of the [Avro specification](https://avro.apache.org/docs/current/spec.html).


## Installation

```bash
$ npm install avsc
```


## Documentation

A few sample snippets first!

+ Encode and decode JavaScript objects:

  ```javascript
  var type = require('avsc').parse({
    type: 'record',
    name: 'Person',
    fields: [
      {name: 'name', type: 'string'},
      {name: 'age', type: 'int'}
    ]
  });
  var buf = type.encode({name: 'Ann', age: 25}); // Serialize a JS object.
  var obj = type.decode(buf); // And deserialize it back.
  ```

+ Get a readable record stream from an Avro container file:

  ```javascript
  require('avsc')
    .decodeFile('records.avro')
    .on('data', function (record) { console.log(record); });
  ```

+ Create a writable stream to serialize Avro records on the fly:

  ```javascript
  var avsc = require('avsc');
  var type = avsc.parse('int');
  var encoder = new avsc.streams.RawEncoder(type)
    .on('data', function (chunk) { /* Use the encoded chunk somehow */ });
  ```

Documentation links:

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
