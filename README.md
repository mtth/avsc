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


### Container files

(API still undergoing changes.)

```javascript
var avsc = require('avsc'),
    fs = require('fs');

// Read an Avro container file.
fs.createReadStream('input.avro')
  .pipe(new avsc.Decoder()) // The decoder will infer the type.
  .on('data', function (record) { console.log(record); });

// Writable record stream.
var stream = new avsc.Encoder();
stream.pipe(fs.createWriteStream('output.avro', {defaultEncoding: 'binary'}));
```

## Documentation

API: https://github.com/mtth/avsc/wiki/API
