/* jshint node: true */

'use strict';

var type = require('./type');

module.exports = {
  parse: type.AvroType.fromSchema
};
