# Avsc

Pure JavaScript implementation of the [Avro specification](https://avro.apache.org/docs/1.7.7/spec.html).

What's already there:

+ Parsing schemas.
+ Encoding, decoding, validating, and generating data.
+ Resolving schemas (a.k.a. "reader's schemas").
+ Reading container files.

Coming up:

+ Writing container files.
+ Sort order.
+ Canonical schemas and fingerprints.
+ Protocols.


## Example

```javascript
var avsc = require('avsc');

var Person = avsc.parse({
  type: 'record',
  name: 'Person',
  fields: [
    {name: 'name', type: 'string'},
    {name: 'age', type: 'int'}
  ]
}).getRecordConstructor();

var person = new Person('Ann', 25);
person.name; // == 'Ann'
person.age; // == 25
person.$encode(); // Buffer containing this record's Avro encoding.
```


## Installation

```bash
$ npm install avsc
```


## Documentation

+ [API](https://github.com/mtth/avsc/wiki/API)
