Advanced usage
==============


Reader schema
-------------

TOOD


Type hooks
----------

```javascript
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
