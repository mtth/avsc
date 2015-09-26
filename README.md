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

+ [Quickstart](https://github.com/mtth/avsc/blob/master/doc/quickstart.md)
+ [API](https://github.com/mtth/avsc/blob/master/doc/api.md)


## Status

What's already there:

+ Parsing and resolving schemas (including schema evolution).
+ Encoding, decoding, validating, and generating data.
+ Reading and writing container files.

Coming up:

+ Protocols.
+ Sort order.
+ Logical types.
