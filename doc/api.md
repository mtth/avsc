# API

## Avro types


### Class `Type`

"Abstract" base Avro type class. All implementations inherit from this type.

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

##### `type.toString()`

Return the canonical version of the schema. This can be used to compare schemas
for equality.

##### `type.createFingerprint(algorithm)`

+ `algorithm` {String} Algorithm to use to generate the fingerprint. Defaults
  to `md5`.


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


# Records

### Class `Record(...)`

Specific record class, programmatically generated for each record schema.

#### `Record.random()`

#### `Record.decode(buf, [resolver])`

#### `record.$encode([opts])`

#### `record.$isValid()`

#### `record.$type`


## Streams


### Class `BlockDecoder([opts])`

+ `opts` {Object} Decoding options. Available keys:
  + `readerType` {AvroType} Reader type.
  + `includeBuffer` {Boolean}
  + `unordered` {Boolean}

#### Event `'metadata'`

+ `writerType` {Type} The type used to write the file.
+ `header` {Object} The file's header, containing in particular the raw schema
  and codec.

#### Event `'data'`

+ `data` {...} Decoded element. If `includeBuffer` was set, `data` will be an
  object `{object, buffer}`.


### Class `RawDecoder([opts])`

+ `opts` {Object} Decoding options. Available keys:
  + `writerType` {Type}
  + `readerType` {Type}
  + `includeBuffer` {Boolean}

#### Event `'data'`

+ `data` {...} Decoded element. If `includeBuffer` was set, `data` will be an
  object `{object, buffer}`.


### Class `BlockEncoder([opts])`

+ `opts` {Object} Encoding options. Available keys:
  + `writerType` {AvroType} Writer type. As a convenience, this will be
    inferred if writing `Record` instances (from the first one passed).
  + `codec` {String}
  + `blockSize` {Number}
  + `omitHeader` {Boolean}
  + `unordered` {Boolean}
  + `unsafe` {Boolean}

#### Event `'data'`

+ `data` {Buffer} Serialized bytes.


### Class `RawEncoder([opts])`

+ `opts` {Object} Encoding options. Available keys:
  + `writerType` {AvroType} Writer type. As a convenience, this will be
    inferred if writing `Record` instances (from the first one passed).
  + `unsafe` {Boolean}

#### Event `'data'`

+ `data` {Buffer} Serialized bytes.
