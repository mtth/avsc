# API

## Avro types

Serializing a `type` back to JSON (e.g. using `JSON.stringify`) will return a
valid equivalent Avro schema!

It is also possible to generate types programmatically, using the classes
below. They are all available in the `avsc.types` namespace.


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
  it. Serializing invalid objects is undefined behavior, so use this if you are
  sure the object satisfies the schema.

Returns a `Buffer` containing the Avro serialization of `obj`.

##### `type.decode(buf, [adapter,] [unsafe])`

+ `buf` {Buffer} Bytes containing a serialized object of the correct type.
+ `adapter` {Adapter} To decode records serialized from another schema. See
  `createAdapter` for how to create an adapter.
+ `unsafe` {Boolean} Do not check that the entire buffer has been read. This
  can be useful when using an adapter which only decodes fields at the start of
  the buffer, allowing decoding to bail early.

##### `type.createAdapter(writerType)`

+ `writerType` {Type} Writer type.

##### `type.toString()`

Return the canonical version of the schema.


#### Class `PrimitiveType(name)`

The common type used for `null`, `boolean`, `int`, `long`, `float`, `double`,
`bytes`, and `string`.


#### Class `ArrayType(schema, [opts])`

##### `type.items`

The `type` of the array's items.


#### Class `EnumType(schema, [opts])`

##### `type.name`
##### `type.doc`
##### `type.symbols`

The enum's name, documentation, and symbols list.

Instances of this type will either be represented as wrapped objects (according
to the Avro spec), or as their value directly (if `unwrapUnions` was set when
parsing the schema).


#### Class `FixedType(schema, [opts])`

##### `type.name`
##### `type.size`

Instances of this type will be `Buffer`s.


#### Class `MapType(schema, [opts])`

##### `type.values`


#### Class `RecordType(schema, [opts])`

##### `type.name`
##### `type.doc`
##### `type.fields`

##### `type.getRecordConstructor()`

The `Record` constructor for instances of this type.

##### `type.asReaderOf(writerType)`

Returns a type suitable for reading a file written using a different schema.

+ `writerType` {Type} A compatible `type`.


#### Class `UnionType(schema, [opts])`

##### `type.types`


# Records

### Class `Record(...)`

Specific record class, programmatically generated for each record schema.

#### `Record.random()`
#### `Record.decode(buf, [adapter])`
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


### Class `avsc.BlockEncoder([opts])`

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
