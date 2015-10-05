/* jshint node: true */

'use strict';

/**
 * Obfuscate a schema.
 *
 * Mangling names of records, fixed, enums, fields, and symbols.
 *
 * Note that the mangled schema here won't be functional for use directly in
 * this file (for example the symbols won't match with the type's internal
 * index). But it'll work just fine once loaded from its canonical
 * representation.
 *
 */

var avsc = require('../../lib'),
    utils = require('../../lib/utils');

var RANDOM = new utils.Lcg();

var schemaPath = process.argv[2];
if (!schemaPath) {
  console.error('usage: node obfuscate.js SCHEMA');
  process.exit(1);
}

var type = avsc.parse(schemaPath, {typeHook: function () {
  if (!this._name) {
    return; // Nothing to mangle.
  }
  this._name = RANDOM.nextString(8);
  var i, l;
  if (this instanceof avsc.types.RecordType) {
    var field;
    for (i = 0, l = this._fields.length; i < l; i++) {
      field = this._fields[i];
      field._name = RANDOM.nextString(8);
    }
  } else if (this instanceof avsc.types.EnumType) {
    for (i = 0, l = this._symbols.length; i < l; i++) {
      this._symbols[i] = RANDOM.nextString(8).toUpperCase();
    }
  }
}});

console.log(type.toString());
