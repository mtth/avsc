/* jshint esversion: 8, node: true */

'use strict';

const avro = require('../../lib');

/**
 * A logical type which remaps record keys.
 *
 * This is useful when the in-memory representation uses characters which are
 * invalid in an Avro schema (e.g. `$`).
 *
 * We make the mapping configurable to give an example of how to customize
 * logical types via the schema (here via `keyMapping`). This can be removed if
 * not needed (e.g. if the mapping is constant).
 */
class KeyMappingRecord extends avro.types.LogicalType {
  constructor(schema, opts) {
    super(schema, opts);
    this._mapping = schema.keyMapping;
  }

  _fromValue(val) {
    return this._mapKeys(val, this._mapping[0], this._mapping[1]);
  }

  _toValue(any) {
    return this._mapKeys(any, this._mapping[1], this._mapping[0]);
  }

  _mapKeys(unmapped, from, to) {
    const mapped = {};
    for (const [key, val] of Object.entries(unmapped)) {
      mapped[key.replace(from, to)] = val;
    }
    return mapped;
  }
}

// Let's use our logical type in a sample schema:
const type = avro.Type.forSchema({
  type: 'record',
  logicalType: 'key-mapping-record',
  keyMapping: ['_', '$'], // Swap underscores and dollars.
  fields: [
    {name: 'year', type: 'int'},
    {name: '_type', type: 'string'},
  ],
}, {logicalTypes: {'key-mapping-record': KeyMappingRecord}});

// We can check that it works:
const obj = {year: 2020, $type: 'TYPE'};
console.log(type.isValid(obj)); // True.
console.log(type.toBuffer(obj)); // Works!

/** Hook which adds the key mapping logical type to all records. */
function typeHook(schema, opts) {
  if (schema.type === 'record') {
    Object.assign(schema, {
      logicalType: 'key-mapping-record',
      keyMapping: ['_', '$'],
    });
  }
}

const type2 = avro.Type.forSchema({
  type: 'record',
  fields: [
    {name: 'year', type: 'int'},
    {name: '_type', type: 'string'},
  ],
}, {logicalTypes: {'key-mapping-record': KeyMappingRecord}, typeHook});

// We can check that it works:
console.log(type2.isValid({year: 2020, $type: 'TYPE'})); // True!

/*
 *
@logicalType("key-mapping-record")
@keyMapping(["_", "$"])
record Test {
  int year;
  string _type;
}
*/
