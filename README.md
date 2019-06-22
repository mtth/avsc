# Avro [![Build status](https://travis-ci.org/mtth/avsc.svg?branch=master)](https://travis-ci.org/mtth/avsc) [![Coverage status](https://coveralls.io/repos/mtth/avsc/badge.svg?branch=master&service=github)](https://coveralls.io/github/mtth/avsc?branch=master)

Pure JavaScript implementation of the [Avro
specification](https://avro.apache.org/docs/current/spec.html).

## Avro types [![NPM version](https://img.shields.io/npm/v/@avro/types.svg)](https://www.npmjs.com/package/@avro/types) [![Download count](https://img.shields.io/npm/dm/@avro/types.svg)](https://www.npmjs.com/package/@avro/types) 

+ Blazingly [fast and compact][benchmarks] serialization! Typically faster than
  JSON with much smaller encodings.
+ All the Avro goodness and more: [type inference][type-inference], [schema
  evolution][schema-evolution], [arbitrary JavaScript object
  serialization][logical-types]...

```javascript
const {Type} = require('@avro/types');

const type = Type.forSchema({
  type: 'record',
  fields: [
    {name: 'kind', type: {type: 'enum', symbols: ['CAT', 'DOG']}},
    {name: 'name', type: 'string'}
  ]
});

const buf = type.toBuffer({kind: 'CAT', name: 'Albert'}); // Encoded buffer.
const val = type.fromBuffer(buf); // = {kind: 'CAT', name: 'Albert'}
```

## Avro services [![NPM version](https://img.shields.io/npm/v/@avro/services.svg)](https://www.npmjs.com/package/@avro/services) [![Download count](https://img.shields.io/npm/dm/@avro/services.svg)](https://www.npmjs.com/package/@avro/services) 

TODO

## Avro streams [![NPM version](https://img.shields.io/npm/v/@avro/streams.svg)](https://www.npmjs.com/package/@avro/streams) [![Download count](https://img.shields.io/npm/dm/@avro/streams.svg)](https://www.npmjs.com/package/@avro/streams) 

TODO

## Avro IDL [![NPM version](https://img.shields.io/npm/v/@avro/idl.svg)](https://www.npmjs.com/package/@avro/idl) [![Download count](https://img.shields.io/npm/dm/@avro/idl.svg)](https://www.npmjs.com/package/@avro/idl) 

TODO
