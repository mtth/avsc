/* jshint node: true */

'use strict';

var parse = require('./parse');

module.exports = {
  parse: parse.parse,
  types: parse.types
};
