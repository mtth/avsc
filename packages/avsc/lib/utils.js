/* jshint node: true */

'use strict';

var utils = require('@avro/types/lib/utils');

module.exports = {
  Tap: utils.Tap,
  bufferFrom: Buffer.from,
  newBuffer: Buffer.alloc,
  toMap: utils.toMap
};
