/* jshint node: true */

'use strict';

/**
 * Obfuscate a schema.
 *
 * Mangling names of records, fixed, enums, fields, and symbols.
 *
 */

var avsc = require('../../lib'),
    utils = require('../../lib/utils');


var RANDOM = new utils.Lcg();

var schemaPath = process.argv[2];
if (!schemaPath) {
  console.error('usage: node obfuscate.js PATH');
  process.exit(1);
}

var type = avsc.parse(schemaPath, {typeHook: function (schema) {
  if (!schema.name) {
    return; // Nothing to mangle.
  }
  schema.name = RANDOM.nextString(8);
  var fields, symbols;
  if ((fields = schema.fields)) {
    fields.forEach(function (o) { o.name = RANDOM.nextString(8); });
  } else if ((symbols = schema.symbols)) {
    symbols.forEach(function (s, i) { symbols[i] = RANDOM.nextString(8); });
  }
}});

console.log(type.toString());
