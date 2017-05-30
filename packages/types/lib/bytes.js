/* jshint esversion: 6, node: true */

'use strict';

function BinaryDecoder(buf, idx) {
  this._reader = new Reader(buf, idx);
  this.index = idx; // Last valid reader index.
}

BinaryDecoder.prototype.decode = function (type, resolver) {
  var reader = this._reader;
  var val = type._read(reader);
  var newIndex = reader.index;
  if (newIndex <= reader.buffer.length) {
    this.index = newIndex;
    return val;
  } else {
    return undefined;
  }
};

BinaryDecoder.prototype.add = function (buf) {
  var reader = this._reader;
  var oldBuf = reader.buffer;
  var tailLength = oldBuf.length - this.index;
  reader.buffer = new Buffer(tailLength + buf.length);
  oldBuf.copy(reader.buffer, 0, this.index);
  buf.copy(reader.buffer, tailLength);
};

/** Decoding. */
function Reader(buf, idx) {
  this.buffer = buf;
  this.index = idx;
}

Reader.prototype.readBoolean = function () {
  return !!this.buffer[this.index++];
};

Reader.prototype.skipBoolean = function () { this.index++; };

Reader.prototype.readInt = function () {
  var n = 0;
  var k = 0;
  var buf = this.buffer;
  var b, h, f, fk;

  do {
    b = buf[this.index++];
    h = b & 0x80;
    n |= (b & 0x7f) << k;
    k += 7;
  } while (h && k < 28);

  if (h) {
    // Switch to float arithmetic, otherwise we might overflow.
    f = n;
    fk = 268435456; // 2 ** 28.
    do {
      b = buf[this.index++];
      f += (b & 0x7f) * fk;
      fk *= 128;
    } while (b & 0x80);
    return (f % 2 ? -(f + 1) : f) / 2;
  }

  return (n >> 1) ^ -(n & 1);
};

Reader.prototype.skipInt = function () {
  var buf = this.buffer;
  while (buf[this.index++] & 0x80) {}
};

Reader.prototype.readLong = Reader.prototype.readInt;

Reader.prototype.skipLong = Reader.prototype.skipInt;

Reader.prototype.readFloat = function () {
  var buf = this.buffer;
  var idx = this.index;
  this.index += 4;
  if (this.index > buf.length) {
    return;
  }
  return this.buffer.readFloatLE(idx);
};

Reader.prototype.skipFloat = function () { this.index += 4; };

Reader.prototype.readDouble = function () {
  var buf = this.buffer;
  var idx = this.index;
  this.index += 8;
  if (this.index > buf.length) {
    return;
  }
  return this.buffer.readDoubleLE(idx);
};

Reader.prototype.skipDouble = function () { this.index += 8; };

Reader.prototype.writeDouble = function (d) {
  var buf = this.buffer;
  var idx = this.index;
  this.index += 8;
  if (this.index > buf.length) {
    return;
  }
  return this.buffer.writeDoubleLE(d, idx);
};

Reader.prototype.readFixed = function (len) {
  var idx = this.index;
  this.index += len;
  if (this.index > this.buffer.length) {
    return;
  }
  var fixed = new Buffer(len);
  this.buffer.copy(fixed, 0, idx, idx + len);
  return fixed;
};

Reader.prototype.skipFixed = function (len) { this.index += len; };

Reader.prototype.readBytes = function () {
  return this.readFixed(this.readLong());
};

Reader.prototype.skipBytes = function () {
  var len = this.readLong();
  this.index += len;
};

/* istanbul ignore else */
if (typeof Buffer.prototype.utf8Slice == 'function') {
  // Use this optimized function when available.
  Reader.prototype.readString = function () {
    var len = this.readLong();
    var idx = this.index;
    var buf = this.buffer;
    this.index += len;
    if (this.index > buf.length) {
      return;
    }
    return this.buffer.utf8Slice(idx, idx + len);
  };
} else {
  Reader.prototype.readString = function () {
    var len = this.readLong();
    var idx = this.index;
    var buf = this.buffer;
    this.index += len;
    if (this.index > buf.length) {
      return;
    }
    return this.buffer.slice(idx, idx + len).toString();
  };
}

Reader.prototype.skipString = function () {
  var len = this.readLong();
  this.index += len;
};

/* istanbul ignore else */
if (typeof Buffer.prototype.latin1Write == 'function') {
  // `binaryWrite` has been renamed to `latin1Write` in Node v6.4.0, see
  // https://github.com/nodejs/node/pull/7111. Note that the `'binary'`
  // encoding argument still works however.
  Reader.prototype.writeBinary = function (str, len) {
    var idx = this.index;
    this.index += len;
    if (this.index > this.buffer.length) {
      return;
    }
    this.buffer.latin1Write(str, idx, len);
  };
} else if (typeof Buffer.prototype.binaryWrite == 'function') {
  Reader.prototype.writeBinary = function (str, len) {
    var idx = this.index;
    this.index += len;
    if (this.index > this.buffer.length) {
      return;
    }
    this.buffer.binaryWrite(str, idx, len);
  };
} else {
  // Slowest implementation.
  Reader.prototype.writeBinary = function (s, len) {
    var idx = this.index;
    this.index += len;
    if (this.index > this.buffer.length) {
      return;
    }
    this.buffer.write(s, idx, len, 'binary');
  };
}

/** Encoding. */
function Reader(buf, idx) {
  this.buffer = buf;
  this.index = idx;
}
