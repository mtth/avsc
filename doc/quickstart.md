# Quickstart


## Parsing schemas

### `avsc.parse(schema, [opts])`

Parse a schema and return an instance of the corresponding `Type`.

+ `schema` {Object|String} Schema (type object or type name string).
+ `opts` {Object} Parsing options. The following keys are currently supported:
  + `namespace` {String} Optional parent namespace.
  + `registry` {Object} Optional registry of predefined type names.
  + `unwrapUnions` {Boolean} By default, Avro expects all unions to be wrapped
    inside an object with a single key. Setting this to `true` will prevent
    this, slightly improving performance (encoding is then done on the first
    type which validates).
  + `typeHook` {Function} Function called after each new Avro type is
    instantiated. The new type is available as `this` and the relevant schema
    as first and only argument.

### `avsc.parseFile(path, [opts])`

Convenience function to parse a schema file directly.

+ `path` {String} Path to schema file.
+ `opts` {Object} Parsing options (identical to those of `parse`).


## Decoding Avro files

### `avsc.decodeFile(path, [opts])`

+ `path` {String} Path to Avro file.
+ `opts` {Object} Decoding options, passed either to the `BlockDecoder`.

Returns a readable stream of decoded objects from an Avro container file.
