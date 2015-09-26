/* jshint node: true */

// TODO: Allow customizing block size when writing arrays and maps.
// TODO: Allow configuring when to write the size when writing arrays and maps.

'use strict';


/**
 * A tap is a buffer which remembers what has been already read.
 *
 * It is optimized for performance, at the cost of failing silently when
 * overflowing the buffer. This is a purposeful trade-off given the expected
 * rarity of this case and the large performance hit necessary to enforce
 * validity. See `isValid` below for more information.
 *
 */
function Tap(buf, pos) {
  this.buf = buf;
  this.pos = pos | 0;
}

/**
 * Check that the tap is in a valid state.
 *
 * For efficiency reasons, none of the methods below will fail if an overflow
 * occurs (either read or write). For this reason, it is up to the caller to
 * always check that the read or write was valid by calling this method.
 *
 */
Tap.prototype.isValid = function () { return this.pos <= this.buf.length; };

// Read, skip, write methods start here.

// These should fail silently when the buffer overflows. Note this is only
// required to be true when the functions are decoding valid objects. For
// example errors will still be thrown if a bad count is read, leading to a
// negative position offset (which will typically cause a failure in
// `readFixed`).

Tap.prototype.readNull = function () { return null; };

Tap.prototype.skipNull = Tap.prototype.writeNull = function () {};

Tap.prototype.readBoolean = function () { return !!this.buf[this.pos++]; };

Tap.prototype.skipBoolean = function () { this.pos++; };

Tap.prototype.writeBoolean = function (b) { this.buf[this.pos++] = !!b; };

Tap.prototype.readInt = Tap.prototype.readLong = function () {
  var n = 0;
  var k = 0;
  var buf = this.buf;
  var b, h, f, fk;

  do {
    b = buf[this.pos++];
    h = b & 0x80;
    n |= (b & 0x7f) << k;
    k += 7;
  } while (h && k < 28);

  if (h) {
    // Switch to float arithmetic, otherwise we might overflow.
    f = n;
    fk = 268435456; // 2 ** 28.
    do {
      b = buf[this.pos++];
      f += (b & 0x7f) * fk;
      fk *= 128;
    } while (b & 0x80);
    return (f % 2 ? -(f + 1) : f) / 2;
  }

  return (n >> 1) ^ -(n & 1);
};

Tap.prototype.skipInt = Tap.prototype.skipLong = function () {
  var buf = this.buf;
  while (buf[this.pos++] & 0x80) {}
};

Tap.prototype.writeInt = Tap.prototype.writeLong = function (n) {
  var m = n >= 0 ? n << 1 : (~n << 1) | 1;
  var buf = this.buf;
  var f;

  if (m > 0) {
    // No overflow, we can use integer arithmetic.
    do {
      buf[this.pos] = m & 0x7f;
      m >>= 7;
    } while (m && (buf[this.pos++] |= 0x80));
  } else {
    // We have to use slower floating arithmetic.
    f = n >= 0 ? n * 2 : (-n * 2) - 1;
    do {
      buf[this.pos] = f & 0x7f;
      f /= 128;
    } while (f >= 1 && (buf[this.pos++] |= 0x80));
  }
  this.pos++;
};

Tap.prototype.readFloat = function () {
  var buf = this.buf;
  var pos = this.pos;
  this.pos += 4;
  if (this.pos > buf.length) {
    return;
  }
  return this.buf.readFloatLE(pos);
};

Tap.prototype.skipFloat = function () { this.pos += 4; };

Tap.prototype.writeFloat = function (f) {
  var buf = this.buf;
  var pos = this.pos;
  this.pos += 4;
  if (this.pos > buf.length) {
    return;
  }
  return this.buf.writeFloatLE(f, pos);
};

Tap.prototype.readDouble = function () {
  var buf = this.buf;
  var pos = this.pos;
  this.pos += 8;
  if (this.pos > buf.length) {
    return;
  }
  return this.buf.readDoubleLE(pos);
};

Tap.prototype.skipDouble = function () { this.pos += 8; };

Tap.prototype.writeDouble = function (d) {
  var buf = this.buf;
  var pos = this.pos;
  this.pos += 8;
  if (this.pos > buf.length) {
    return;
  }
  return this.buf.writeDoubleLE(d, pos);
};

Tap.prototype.readString = function () {
  var len = this.readLong();
  var pos = this.pos;
  this.pos += len;
  return this.buf.toString(undefined, pos, pos + len);
};

Tap.prototype.skipString = function () {
  var len = this.readLong();
  this.pos += len;
};

Tap.prototype.writeString = function (s) {
  var len = Buffer.byteLength(s);
  this.writeLong(len);
  var pos = this.pos;
  this.pos += len;
  if (this.pos > this.buf.length) {
    return;
  }
  this.buf.write(s, pos);
};

Tap.prototype.readFixed = function (len) {
  var pos = this.pos;
  this.pos += len;
  if (this.pos > this.buf.length) {
    return;
  }
  var fixed = new Buffer(len);
  this.buf.copy(fixed, 0, pos, pos + len);
  return fixed;
};

Tap.prototype.skipFixed = function (len) { this.pos += len; };

Tap.prototype.writeFixed = function (buf, len) {
  len = len || buf.length;
  var pos = this.pos;
  this.pos += len;
  if (this.pos > this.buf.length) {
    return;
  }
  buf.copy(this.buf, pos, 0, len);
};

Tap.prototype.readBytes = function () {
  return this.readFixed(this.readLong());
};

Tap.prototype.skipBytes = function () {
  var len = this.readLong();
  this.pos += len;
};

Tap.prototype.writeBytes = function (buf) {
  var len = buf.length;
  this.writeLong(len);
  this.writeFixed(buf, len);
};

Tap.prototype.readArray = function (fn) {
  var arr = [];
  var n;
  while ((n = this.readLong())) {
    if (n < 0) {
      n = -n;
      this.skipLong(); // Skip size.
    }
    while (n--) {
      arr.push(fn.call(this));
    }
  }
  return arr;
};

Tap.prototype.skipArray = function (fn) {
  var len, n;
  while ((n = this.readLong())) {
    if (n < 0) {
      len = this.readLong();
      this.pos += len;
    } else {
      while (n--) {
        fn.call(this);
      }
    }
  }
};

Tap.prototype.writeArray = function (arr, fn) {
  var n = arr.length;
  var i;
  if (n) {
    this.writeLong(n);
    for (i = 0; i < n; i++) {
      fn.call(this, arr[i]);
    }
  }
  this.writeLong(0);
};

Tap.prototype.readMap = function (fn) {
  var obj = {};
  var n;
  while ((n = this.readLong())) {
    if (n < 0) {
      n = -n;
      this.skipLong(); // Skip size.
    }
    while (n--) {
      var key = this.readString();
      obj[key] = fn.call(this);
    }
  }
  return obj;
};

Tap.prototype.skipMap = function (fn) {
  var len, n;
  while ((n = this.readLong())) {
    if (n < 0) {
      len = this.readLong();
      this.pos += len;
    }
    while (n--) {
      this.skipString();
      fn.call(this);
    }
  }
};

Tap.prototype.writeMap = function (obj, fn) {
  var keys = Object.keys(obj);
  var n = keys.length;
  var i, key;
  if (n) {
    this.writeLong(n);
    for (i = 0; i < n; i++) {
      key = keys[i];
      this.writeString(key);
      fn.call(this, obj[key]);
    }
  }
  this.writeLong(0);
};


module.exports = Tap;
