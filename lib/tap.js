/* jshint node: true */

'use strict';

/**
 * A tap is simply a buffer which remembers what has been already read.
 *
 * It is optimized for performance, at the cost of failing silently when
 * overflowing the buffer. This is a purposeful trade-off given the expected
 * rarity of this case and the large performance hit necessary to enforce
 * validity. See `isValid` below for more information.
 *
 */
function Tap(buf, offset) {

  this.buf = buf;
  this.offset = offset | 0;

}

/**
 * Check that the tap is in a valid state.
 *
 * For efficiency reasons, none of the methods below will fail if an overflow
 * occurs (either read or write). For this reason, it is up to the caller to
 * always check that the read or write was valid by using this method.
 *
 */
Tap.prototype.isValid = function () {

  return this.offset <= this.buf.length;

};

// Decoding and encoding functions are below.
//
// Warning: See `isValid` method above.

Tap.prototype.readNull = function () { return null; };

Tap.prototype.writeNull = function () {};

Tap.prototype.readBoolean = function () { return !!this.buf[this.offset++]; };

Tap.prototype.writeBoolean = function (b) { this.buf[this.offset++] = !!b; };

Tap.prototype.readInt = Tap.prototype.readLong = function () {

  var b = 0;
  var n = 0;
  var k = 0;
  var buf = this.buf;
  var fk = 268435456; // 2 ** 28.
  var f;

  do {
    b = buf[this.offset++];
    if (k < 28) {
      n |= (b & 0x7f) << k;
      k += 7;
    } else {
      // Switch to float arithmetic, otherwise we might overflow.
      if (!f) {
        f = n;
      }
      f += (b & 0x7f) * fk;
      fk *= 128;
    }
  } while (b & 0x80);

  if (f) {
    return (f % 2 ? -(f + 1) : f) / 2;
  } else {
    return (n >> 1) ^ -(n & 1);
  }

};

Tap.prototype.writeInt = Tap.prototype.writeLong = function (n) {

  var m = n >= 0 ? n << 1 : (~n << 1) | 1;
  var buf = this.buf;
  var f;

  if (m > 0) {
    // No overflow, we can use integer arithmetic.
    do {
      buf[this.offset] = m & 0x7f;
      m >>= 7;
    } while (m && (buf[this.offset++] |= 0x80));
  } else {
    // We have to use slower floating arithmetic.
    f = n >= 0 ? n * 2 : (-n * 2) - 1;
    do {
      buf[this.offset] = f & 0x7f;
      f /= 128;
    } while (f >= 1 && (buf[this.offset++] |= 0x80));
  }
  this.offset++;

};

Tap.prototype.readString = function () {

  var len = this.readLong();
  var s = this.buf.toString(undefined, this.offset, this.offset + len);
  this.offset += len;
  return s;

};

Tap.prototype.readFloat = Tap.prototype.readDouble = function () {

  throw new Error('not implemented'); // TODO.

};

Tap.prototype.writeFloat = Tap.prototype.writeDouble = function () {

  throw new Error('not implemented'); // TODO.

};

Tap.prototype.writeString = function (s) {

  var len = Buffer.byteLength(s);
  this.writeLong(len);
  this.buf.write(s, this.offset);
  this.offset += len;

};

Tap.prototype.readFixed = function (len) {

  var offset = this.offset;
  var fixed = new Buffer(len);
  this.buf.copy(fixed, 0, offset, offset + len);
  this.offset += len;
  return fixed;

};

Tap.prototype.writeFixed = function (buf, len) {

  len = len || buf.length;
  buf.copy(this.buf, this.offset, 0, len);
  this.offset += len;

};

Tap.prototype.readBytes = function () {

  return this.readFixed(this.readLong());

};

Tap.prototype.writeBytes = function (buf) {

  var len = buf.length;
  this.writeLong(len);
  this.writeFixed(buf, len);

};

Tap.prototype.readArray = function (fn) {

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

Tap.prototype.writeArray = function (arr, fn) {

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

Tap.prototype.readMap = function (fn) {

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

Tap.prototype.writeMap = function (obj, fn) {

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

Tap.prototype.readUnion = function (fns) {

  // (This will also correctly fail silently on overflow.)
  return fns[this.readLong()]._reader.call(this); // TODO: Optimize this.

};

Tap.prototype.writeUnion = function () {

  throw new Error('not implemented'); // TODO.

};

module.exports = Tap;
