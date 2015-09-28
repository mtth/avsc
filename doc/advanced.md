Advanced usage
==============


Reader schema
-------------

Avro supports reading data written by another schema (as long as the reader's
and writer's schemas are compatible). We can do this by creating an appropriate
`Resolver`:

```javascript
var avsc = require('avsc');

// A schema's first version.
var v1 = avsc.parse({
  type: 'record',
  name: 'Person',
  fields: [
    {name: 'name', type: 'string'},
    {name: 'age', type: 'int'}
  ]
});

// The updated version.
has been added:
var v2 = avsc.parse({
  type: 'record',
  name: 'Person',
  fields: [
    {
      name: 'name', type: [
        'string',
        {
          name: 'Name',
          type: 'record',
          fields: [
            {name: 'first', type: 'string'},
            {name: 'last', type: 'string'}
          ]
        }
      ]
    },
    {name: 'phone', type: ['null', 'string'], default: null}
  ]
});

var resolver = v2.createResolver(v1);
var buf = v1.encode({name: 'Ann', age: 25});
var obj = v2.decode(buf, resolver); // === {name: {string: 'Ann'}, phone: null}
```


Type hooks
----------

Using the `typeHook` option, it is possible to introduce custom behavior on any
type. This can for example be used to override a type's `isValid` or `random`
method.

Below we show an example implementing a custom random float generator.

```javascript
var avsc = require('avsc');

/**
 * Hook which allows setting a range for float types.
 *
 * @param schema {Object} The type's corresponding schema.
 *
 */
var typeHook = function (schema) {
  var range = schema.range;
  if (this.type === 'float') {
    var span = range[1] - range[0];
    this.random = function () { return range[0] + span * Math.random(); };
  }
};

// We pass the above hook in the parsing options.
var type = avsc.parse({
  type: 'record',
  name: 'Recommendation',
  fields: [
    {name: 'id', type: 'long'},
    {name: 'score', type: {type: 'float', range: [-1, 1]}} // Note the range.
  ]
}, {typeHook: typeHook});
```
