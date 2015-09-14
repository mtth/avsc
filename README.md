# Avsc

A JavaScript Avro API which will make you smile.

*Under development.*


## API


### All Avro types

```javascript
var avsc = require('avsc');

// Each Avro type exposes decoding and encoding methods.
var intMapType = avsc.parse({type: 'map', values: 'int'});
var buf = intMapType.encode({one: 1, two: 2});
var obj = intMapType.decode(buf); // == {one: 1, two: 2}
```


### Records

```javascript
var avsc = require('avsc');

// Avro type corresponding to a record.
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
person.$name; // == 'Person'
person.$fields; // == ['name', 'age']
person.$toAvro(); // Buffer with encoded record.

// Finally the record class exposes a static decoding method.
Person.fromAvro(buf); // == person
```


### Object container files

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
