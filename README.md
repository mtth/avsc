# Avsc [![Build status](https://travis-ci.org/mtth/avsc.svg?branch=master)](https://travis-ci.org/mtth/avsc)

Pure JavaScript implementation of the [Avro specification](https://avro.apache.org/docs/current/spec.html).


## Installation

```bash
$ npm install avsc
```


## Example

```javascript
var avsc = require('avsc');

var type = avsc.parse({
  type: 'record',
  name: 'Person',
  fields: [
    {name: 'name', type: 'string'},
    {name: 'age', type: 'int'}
  ]
});

var buf = type.encode({name: 'Ann', age: 25});
```


## Documentation

+ [API](https://github.com/mtth/avsc/wiki/API)


## Status

What's already there:

+ Parsing and resolving schemas (including schema evolution).
+ Encoding, decoding, validating, and generating data.
+ Reading and writing container files.

Coming up:

+ Protocols.
+ Sort order.
+ Logical types.
