# API

+ [Parsing schemas](#parsing-schemas)
+ [Avro types](#avro-types)
+ [Records](#records)
+ [Streams](#streams)


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


## Avro types

All the classes below are available in the `avsc.types` namespace:

+ [`Type`](#class-type)
+ [`PrimitiveType`](#class-primitivetypename)
+ [`ArrayType`](#class-arraytype-opts)
+ [`EnumType`](#class-enumtype-opts)
+ [`FixedType`](#class-fixedtype-opts)
+ [`MapType`](#class-maptype-opts)
+ [`RecordType`](#class-recordtype-opts)
+ [`UnionType`](#class-uniontype-opts)

### Class `Type`

"Abstract" base Avro type class. All implementations inherit from it.


##### `type.type`

The type's name (e.g. `'int'`, `'record'`, ...).


##### `type.random()`

Generate a random instance of this type.


##### `type.isValid(obj)`

+ `obj` {Object} The object to validate.

Check whether `obj` is a valid representation of `type`.


##### `type.encode(obj, [size,] [unsafe])`

+ `obj` {Object} The instance to encode. It must be of type `type`.
+ `size` {Number}, Size in bytes used to initialize the buffer into which the
  object will be serialized. If the serialized object doesn't fit, a resize
  will be necessary. Defaults to 1024 bytes.
+ `unsafe` {Boolean} Do not check that the instance is valid before encoding
  it. Serializing invalid objects is undefined behavior, so use this only if
  you are sure the object satisfies the schema.

Returns a `Buffer` containing the Avro serialization of `obj`.


##### `type.decode(buf, [resolver,] [unsafe])`

+ `buf` {Buffer} Bytes containing a serialized object of the correct type.
+ `resolver` {Resolver} To decode records serialized from another schema. See
  `createResolver` for how to create an resolver.
+ `unsafe` {Boolean} Do not check that the entire buffer has been read. This
  can be useful when using an resolver which only decodes fields at the start of
  the buffer, allowing decoding to bail early.


##### `type.createResolver(writerType)`

+ `writerType` {Type} Writer type.

Create a resolver that can be be passed to the `type`'s `decode` method. This
will enable decoding objects which had been serialized using `writerType`,
according to the Avro [resolution rules][schema-resolution]. If the schemas are
incompatible, this method will throw an error.


##### `type.toString()`

Returns the [canonical version of the schema][canonical-schema]. This can be
used to compare schemas for equality.


##### `type.createFingerprint(algorithm)`

+ `algorithm` {String} Algorithm to use to generate the schema's
  [fingerprint][]. Defaults to `md5`.


##### `Type.fromSchema(schema, [opts])`

Alias for `avsc.parse`.


#### Class `PrimitiveType(name)`

The common type used for `null`, `boolean`, `int`, `long`, `float`, `double`,
`bytes`, and `string`. It has no other properties than the base `Type`'s.


#### Class `ArrayType(schema, [opts])`

##### `type.items`

The type of the array's items.


#### Class `EnumType(schema, [opts])`

##### `type.name`

The type's name.

##### `type.symbols`

Array of strings, representing the enum's valid values.

##### `type.aliases`

Optional type aliases. These are used when adapting a schema from another type.

##### `type.doc`

Optional documentation.


#### Class `FixedType(schema, [opts])`

##### `type.name`

The type's name.

##### `type.size`

The size in bytes of instances of this type.

##### `type.aliases`

Optional type aliases. These are used when adapting a schema from another type.


#### Class `MapType(schema, [opts])`

##### `type.values`

The type of the map's values (keys are always strings).


#### Class `RecordType(schema, [opts])`

##### `type.name`

The type's name.

##### `type.fields`

The array of fields contained in this record. Each field is an object with the
following keys:

+ `name`
+ `type`
+ `default` (can be undefined).
+ `aliases` (can be undefined).
+ `doc` (can be undefined).

##### `type.getRecordConstructor()`

The `Record` constructor for instances of this type.

##### `type.asReaderOf(writerType)`

+ `writerType` {Type} A compatible `type`.

Returns a type suitable for reading a file written using a different schema.

##### `type.aliases`

Optional type aliases. These are used when adapting a schema from another type.

##### `type.doc`

Optional documentation.


#### Class `UnionType(schema, [opts])`

Instances of this type will either be represented as wrapped objects (according
to the Avro spec), or as their value directly (if `unwrapUnions` was set when
parsing the schema).

##### `type.types`

The possible types that this union can take.


## Records

Each [`RecordType`](#class-recordtype-opts) generates a corresponding `Record`
constructor when its schema is parsed. It is available using the `RecordType`'s
`getRecordConstructor` methods. This makes decoding records more efficient and
lets us provide the following convenience methods:

### Class `Record(...)`

Calling the constructor directly can sometimes be a convenient shortcut to
instantiate new records of a given type.

#### `Record.random()`

#### `Record.decode(buf, [resolver,] [unsafe])`

#### `record.$encode([size,] [unsafe])`

#### `record.$isValid()`

#### `record.$type`


## Streams

### `avsc.decodeFile(path, [opts])`

+ `path` {String} Path to Avro file.
+ `opts` {Object} Decoding options, passed either to the `BlockDecoder`.

Returns a readable stream of decoded objects from an Avro container file.

For other use-cases, the following stream classes are available in the
`avsc.streams` namespace:

+ [`BlockDecoder`](#blockdecoderopts)
+ [`RawDecoder`](#rawdecoderopts)
+ [`BlockEncoder`](#blockencoderopts)
+ [`RawEncoder`](#rawencoderopts)


### Class `BlockDecoder([opts])`

+ `opts` {Object} Decoding options. Available keys:
  + `decode` {Boolean} Whether to decode records before returning them.
    Defaults to `true`.
  + `parseOpts` {Object} Options passed to instantiate the writer's `Type`.

#### Event `'metadata'`

+ `type` {Type} The type used to write the file.
+ `codec` {String} The codec's name.
+ `header` {Object} The file's header, containing in particular the raw schema
  and codec.

#### Event `'data'`

+ `data` {Object|Buffer} Decoded element or raw bytes.


### Class `RawDecoder(type, [opts])`

+ `type` {Type} Writer type.
+ `opts` {Object} Decoding options. Available keys:
  + `decode` {Boolean}

#### Event `'data'`

+ `data` {Object|Buffer} Decoded element or raw bytes.


### Class `BlockEncoder(type, [opts])`

+ `type` {Type} The type to use for encoding.
+ `opts` {Object} Encoding options. Available keys:
  + `codec` {String} Name of codec to use for encoding.
  + `blockSize` {Number} Maximum uncompressed size of each block data. A new
    block will be started when this number is exceeded. If it is too small to
    fit a single element, it will be increased appropriately. Defaults to 64kB.
  + `omitHeader` {Boolean} Don't emit the header. This can be useful when
    appending to an existing container file. Defaults to `false`.
  + `unsafe` {Boolean} Whether to check each record before encoding it.
    Defaults to `true`.

#### Event `'data'`

+ `data` {Buffer} Serialized bytes.


### Class `RawEncoder(type, [opts])`

+ `type` {Type} The type to use for encoding.
+ `opts` {Object} Encoding options. Available keys:
  + `batchSize` {Number} To increase performance, records are serialized in
    batches. Use this option to control how often batches are emitted. If it is
    too small to fit a single record, it will be increased automatically.
    Defaults to 64kB.
  + `unsafe` {Boolean} Whether to check each record before encoding it.
    Defaults to `true`.

#### Event `'data'`

+ `data` {Buffer} Serialized bytes.


[canonical-schema]: https://avro.apache.org/docs/current/spec.html#Parsing+Canonical+Form+for+Schemas
[schema-resolution]: https://avro.apache.org/docs/current/spec.html#Schema+Resolution
[fingerprint]: https://avro.apache.org/docs/current/spec.html#Schema+Fingerprints
