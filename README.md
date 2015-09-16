# Avsc

A JavaScript Avro API which will make you smile.

*Under development.*


## Examples

### Fragments

```javascript
var avsc = require('avsc');

// Parsing a schema returns a corresponding Avro type.
var type = avsc.parse({
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
person.name; // == 'Ann' (The record's fields get set appropriately.)
person.age; // == 25

// Or to decode them from an existing buffer.
Person.decode(buf);

// Or even to create random instances.
var fakePerson = Person.random();

// Records instance also have a few useful properties and methods.
person.$type; // == type
person.$encode(); // Returns a buffer with the record's Avro encoding.
person.$isValid(); // Check that all fields satisfy the schema.
```


### Object container files

(API still undergoing changes.)

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
