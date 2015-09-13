/* jshint node: true */

'use strict';

function Flow(buf, offset) {

  this.buf = buf;
  this.offset = offset | 0;

}

Flow.prototype.readNull = Flow.prototype.writeNull = function () {};

Flow.prototype.readBoolean = function () { return !!this.buf[this.offset++]; };

Flow.prototype.writeBoolean = function (b) { this.buf[this.offset++] = b; };

Flow.prototype.readInt = Flow.prototype.readLong = function () {

  // TODO: Check for overflow.

  var b = 0;
  var n = 0;
  var k = 0;
  var buf = this.buf;
  do {
    b = buf[this.offset++];
    n |= (b & 0x7f) << k;
    k += 7;
  } while (b & 0x80);
  return (n >> 1) ^ -(n & 1);

};

Flow.prototype.writeInt = Flow.prototype.writeLong = function (n) {

  n = n >= 0 ? n << 1 : (~n << 1) | 1;
  var buf = this.buf;
  do {
    buf[this.offset] = n & 0x7f;
    n >>= 7;
  } while (n && (buf[this.offset++] |= 0x80));
  this.offset++;

};

Flow.prototype.readString = function () {

  var len = this.readLong();
  var s = this.buf.toString(undefined, this.offset, this.offset + len);
  this.offset += len;
  return s;

};

Flow.prototype.readFloat = Flow.prototype.readDouble = function () {

  throw new Error('not implemented'); // TODO.

};

Flow.prototype.writeFloat = Flow.prototype.writeDouble = function () {

  throw new Error('not implemented'); // TODO.

};

Flow.prototype.writeString = function (s) {

  var len = Buffer.byteLength(s);
  var buf = this.buf;
  if (len + this.offset > buf.length) {
    // Since `buf.write` won't error out but silently truncate.
    throw new Error('full');
  }
  this.writeLong(len);
  buf.write(s, this.offset);
  this.offset += len;

};

Flow.prototype.readFixed = function (len) {

  var buf = this.buf;
  var offset = this.offset;
  var end = offset + len;
  if (end > buf.length) {
    throw new Error('short');
  }
  var fixed = new Buffer(len);
  this.buf.copy(fixed, 0, offset, end);
  this.offset += len;
  return fixed;

};

Flow.prototype.writeFixed = function (buf, len) {

  len = len || buf.length;
  var fixed = buf;
  buf = this.buf;
  var offset  = this.offset;
  if (offset + len > buf.length) {
    throw new Error('full');
  }
  fixed.copy(buf, offset, 0, len);
  this.offset += len;

};

Flow.prototype.readBytes = function () {

  return this.readFixed(this.readLong());

};

Flow.prototype.writeBytes = function (buf) {

  var len = buf.length;
  this.writeLong(len);
  this.writeFixed(buf, len);

};

Flow.prototype.readArray = function (fn) {

  var arr = [];
  var i, len;
  while ((len = this.readLong())) {
    if (len < 0) {
      len = -len;
      this.readLong(); // Skip size.
    }
    for (i = 0; i < len; i++) {
      arr.push(fn.call(this));
    }
  }
  return arr;

};

Flow.prototype.writeArray = function (arr, fn) {

  // TODO: Allow customizing block size.
  // TODO: Allow configuration of writing block size.

  var len = arr.length;
  var i;
  if (len) {
    this.writeLong(len);
    for (i = 0; i < len; i++) {
      fn.call(this, arr[i]);
    }
  }
  this.writeLong(0);

};

Flow.prototype.readMap = function (fn) {

  var obj = {};
  var i, len;
  while ((len = this.readLong())) {
    if (len < 0) {
      len = -len;
      this.readLong(); // Skip size.
    }
    for (i = 0; i < len; i++) {
      var key = this.readString();
      obj[key] = fn.call(this);
    }
  }
  return obj;

};

Flow.prototype.writeMap = function (obj, fn) {

  // TODO: Allow customizing block size.
  // TODO: Allow configuration of writing block size.

  var keys = Object.keys(obj);
  var len = keys.length;
  var i, key;
  if (len) {
    this.writeLong(len);
    for (i = 0; i < len; i++) {
      key = keys[i];
      this.writeString(key);
      fn.call(this, obj[key]);
    }
  }
  this.writeLong(0);

};

module.exports = {
  Flow: Flow
};
