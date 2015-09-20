/* jshint node: true */

'use strict';

/**
 * An example of how to build and edit a schema using `avsc` types.
 *
 * This is probably not something that you would do very often, but it can be
 * convenient to do edits to an already existing schema and output a new
 * correct schema, especially when it contains type references.
 *
 */

var avsc = require('../lib');


// Create a record type.
var personType = new avsc.types.RecordType({
  name: 'Person',
  fields: [
    {name: 'name', type: 'string'},
    {name: 'age', type: 'int'}
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
