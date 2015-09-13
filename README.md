# Avsc

## API

### Any Avro types

```javascript
var intMapType = avsc.parse({type: 'map', values: 'int'});
var buf = intMapType.encode({one: 1, two: 2});
var obj = intMapType.decode(buf); // == {one: 1, two: 2}
```

### Records

```javascript
var recordType = avsc.parse({
  type: 'record',
  name: 'Person',
  fields: [
    {name: 'name', type: 'string'},
    {name: 'age', type: 'int'}
  ]
});

// Constructors are programmatically generated for each record type!
var Record = recordType.getRecordConstructor();

// This constructor can be used to instantiate records directly.
var record = new Record('Ann', 25);

// Records expose a few useful properties and methods.
record.$name; // == 'Person'
record.$fields; // == ['name', 'age']
record.$toAvro(); // Buffer with bytes representation of record.

// Or via a static method.
Record.fromAvro(buf); // == record
```

### Object container files

TODO
