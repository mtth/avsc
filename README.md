# Avsc

## API

### Type

```javascript
var Type = avsc.parse({type: 'map', values: 'int'});
var buf = Type.encode({one: 1, two: 2});
var obj = Type.decode(buf); // == buf
```

### Records

```javascript
var Type = avsc.parse({
  type: 'record',
  name: 'Person',
  fields: [
    {name: 'name', type: 'string'},
    {name: 'age', type: 'int'}
  ]
});

// Constructors are programmatically generated for each record type!
var Record = Type.getRecordConstructor();

// This constructor can be used to instantiate them directly.
var record = new Record('Ann', 25);
record.$name; // == 'Person'
record.$fields; // == ['name', 'age']
record.$toAvro(); // Buffer with bytes representation of record.

// Or via a static method.
Record.fromAvro(buf); // == record
```

### Object container files

TODO
