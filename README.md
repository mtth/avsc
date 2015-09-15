# Avsc

A JavaScript Avro API which will make you smile.

*Under development.*


## Examples


### Fragments

```javascript
var avsc = require('avsc');

// Parsing a schema returns a corresponding Avro type.
var stringType = asvc.parse('string');

// This type exposes decoding and encoding methods.
var buf1 = stringType.encode('hello, Avro!'); // Bytes  encoding
stringType.decode(buf); // == 'hello, Avro!'

// Complex types work in the same way.
var intMapType = avsc.parse({type: 'map', values: 'int'});
var buf2 = intMapType.encode({one: 1, two: 2});
intMapType.decode(buf); // == {one: 1, two: 2}

// So do record types.
var recordType = avsc.parse({
  type: 'record',
  name: 'Person',
  fields: [
    {name: 'name', type: 'string'},
    {name: 'age', type: 'int'}
  ]
});

// For record types, constructors are programmatically generated!
var Person = recordType.getRecordConstructor();

// This constructor can be used to instantiate records directly.
var person = new Person('Ann', 25);

// The record's fields get set appropriately.
person.name; // == 'Ann'
person.age; // == 25

// Records also have a few useful properties and methods.
person.$typeName; // == 'Person'
person.$fieldNames; // == ['name', 'age']
person.$encode(); // Buffer with encoded record.

// Finally the record class exposes a static decoding method.
Person.decode(buf); // == person
```


### Object container files

(Soon.)

```javascript
var avsc = require('avsc'),
    fs = require('fs');

// Read.
var reader = avsc.createReadStream('events.avro')
reader.on('data', function (record) { console.log(record); });

// Write.
var writer = type.('events.avro');
writer.write(record);

// Or.
var byteStream = fs.createReadStream('events.avro');
var eventStream = new avsc.ReadStream(bytesStream);


```


## API

### `avsc.parse(schema, [opts])`

Parse a schema and return an instance of the corresponding `Type`.

### `avsc.parseFile(path, [opts])`

Convenience function to parse a schema file.

### `class Type`

"Abstract" base Avro type class. All implementations (see below) have the
following methods:

##### `type.decode(buf)`
##### `type.encode(obj, [opts])`
##### `type.validate(obj)`
##### `type.getTypeName()`

Implementations:

#### `class ArrayType(schema, [opts])`
##### `type.itemsType`

#### `class EnumType(schema, [opts])`
##### `type.name`
##### `type.doc`
##### `type.symbols`

#### `class FixedType(schema, [opts])`
##### `type.name`
##### `type.size`

#### `class MapType(schema, [opts])`
##### `type.valuesType`

#### `class PrimitiveType(name)`

#### `class RecordType(schema, [opts])`
##### `type.name`
##### `type.doc`
##### `type.fields`
##### `type.getRecordConstructor()`

#### `class UnionType(schema, [opts])`
##### `type.types`

### `class Record(...)`

Specific record class, programmatically generated for each record schema.

##### `Record.decode(buf)`
##### `record.$type`
##### `record.$encode([opts])`
##### `record.$validate()`
