/* jshint node: true */

'use strict';

/**
 * An example of how to build and edit a schema using `avsc` types.
 *
 * This is probably not something that you would do very often, but it can be
 * convenient to do edits to an already existing schema and output a new
 * correct schema.
 *
 */

var avsc = require('../lib');

// Integer type with custom random generator.
var ageType = new avsc.types.PrimitiveType('int');
ageType.random = function () { return 100 * Math.random(); };

// Create a new record type with this age field.
var personType = new avsc.types.RecordType({
  name: 'Person',
  fields: [
    {name: 'name', type: 'string'},
    {name: 'age', type: ageType}
  ]
});

// Add a new field to the previous record.
var groupType = new avsc.types.ArrayType({items: personType});
personType.fields.push({name: 'friends', type: groupType});

// Stringify schema, correctly taking care of circular references.
var stringified = JSON.stringify(personType, (function () {
  var cache = {};
  return function (key, value) {
    var name = value && typeof value != 'function' && value.name;
    if (name) {
      if (cache[name]) {
        return name;
      }
      cache[name] = value;
    }
    return value;
  };
})());

// Output new schema.
console.log(stringified);
