/* jshint node: true */

'use strict';

var schema = require('./schema');

function generateRecordType(obj) {

  return new schema.Schema(obj)._value._constructor;

}

module.exports = {
  generateRecordType: generateRecordType,
  Schema: schema.Schema
};
