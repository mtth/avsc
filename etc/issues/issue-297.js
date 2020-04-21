/* jshint esversion: 8, node: true */

'use strict';

const avro = require('../../lib');

class LTType extends avro.types.LogicalType {

  _fromValue(val) {
    return val;
  }

  _toValue(any) {
    return any;
  }
}

const ptcl = avro.Service.forProtocol({
  protocol: 'Test',
  types: [
    {
      type: 'record',
      name: 'A',
      logicalType: 'LT',
      fields: [{type: 'string', name: 'a'}],
    },
    {
      type: 'record',
      name: 'B',
      logicalType: 'LT',
      fields: [{type: 'string', name: 'b'}],
    },
    {
      type: 'record',
      name: 'C',
      logicalType: 'LT',
      fields: [{type: ['A', 'B'], name: 'c'}],
    },
  ]
}, {logicalTypes: {LT: LTType}});

const type = ptcl.type('C');

console.log(type.isValid({c: {c: 'abc'}}));
