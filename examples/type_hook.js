/* jshint node: true */

'use strict';

/**
 * Script showing how to use the type hooks to generate useful random data.
 *
 */

var avsc = require('../lib');

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
    {name: 'score', type: {type: 'float', range: [-1, 1]}}
  ]
}, {typeHook: typeHook});

// Randomly generated recommendations will now have a score between 0 and 1!
var i = 5;
while (i--) {
  console.log(type.random());
}
