/* jshint esversion: 8, node: true */

'use strict';

const avro = require('../../lib');
const util = require('util');

class DateType extends avro.types.LogicalType {
  _fromValue(val) {
    return new Date(val);
  }
  _toValue(date) {
    return date instanceof Date ? +date : undefined;
  }
  _resolve(type) {
    if (avro.Type.isType(type, 'long', 'string', 'logical:timestamp-millis')) {
      return this._fromValue;
    }
  }
}

const schema = {
  "type": "record",
  "name": "avro_tester_han_field_validation",
  "fields": [
    { "name": "time", "type": { "type": "long", "logicalType": "timestamp-millis" } },
    { "name": "age", "type": { "type": "int", "logicalType": "validated-int", "minimum": 5 }, "default": 10 },
    { "name": "timeSpentOnlineMs", "type": { "type": "long", "logicalType": "validated-long", "maximum": 100000000 } },
    { "name": "amountSpendShopping", "type": { "type": "float", "logicalType": "validated-float", "minimum": 10.45, "maximum": 20.55 } },
    { "name": 'sessionId', "type": { "type": 'string', "logicalType": 'validated-string', "pattern": '^\\d{3}-\\d{4}-\\d{5}$' } },
    { "name": "amountSpendShoppingWithFloat", "type": ['double', 'null'] }
  ]
}

const eventObj = { time: Date.now(), age: 15, timeSpentOnlineMs: 100000000, amountSpendShopping: 20.56, sessionId: '123-1234-12345', amountSpendShoppingWithFloat: 20.56 }
const newType = avro.Type.forSchema( schema, { logicalTypes: {'timestamp-millis': DateType}, assertLogicalTypes: true} );
console.log(newType.fields);
const buf = newType.toBuffer(eventObj)
const date = newType.fromBuffer(buf);
console.log('****', date, '****')
