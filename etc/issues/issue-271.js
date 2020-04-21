/* jshint esversion: 8, node: true */

'use strict';

const avro = require('../../lib');
const streams = require('streams')

class FilteringStream extends streams.Transform {
  constructor(type) {
    super({objectMode: true});
    this.type = type;
  }
  transform(any, encoding, cb) {
    if (this.type.isValid(any)) {
      cb(null, any);
    } else {
      this.emit('mismatch', any, this.type);
      cb();
    }
  }
}
