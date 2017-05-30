/* jshint node: true */

// TODO: Make long comparison impervious to precision loss.
// TODO: Optimize binary comparison methods.

'use strict';

/** Various utilities used across this library. */

var crypto = require('crypto');
var util = require('util');


/**
 * Uppercase the first letter of a string.
 *
 * @param s {String} The string.
 */
function capitalize(s) { return s.charAt(0).toUpperCase() + s.slice(1); }

/**
 * Compare two numbers.
 *
 * @param n1 {Number} The first one.
 * @param n2 {Number} The second one.
 */
function compare(n1, n2) { return n1 === n2 ? 0 : (n1 < n2 ? -1 : 1); }

/**
 * Get option or default if undefined.
 *
 * @param opts {Object} Options.
 * @param key {String} Name of the option.
 * @param def {...} Default value.
 *
 * This is useful mostly for true-ish defaults and false-ish values (where the
 * usual `||` idiom breaks down).
 */
function getOption(opts, key, def) {
  var value = opts[key];
  return value === undefined ? def : value;
}

/**
 * Compute a string's hash.
 *
 * @param str {String} The string to hash.
 * @param algorithm {String} The algorithm used. Defaults to MD5.
 */
function getHash(str, algorithm) {
  algorithm = algorithm || 'md5';
  var hash = crypto.createHash(algorithm);
  hash.end(str);
  return hash.read();
}

/**
 * Find index of value in array.
 *
 * @param arr {Array} Can also be a false-ish value.
 * @param v {Object} Value to find.
 *
 * Returns -1 if not found, -2 if found multiple times.
 */
function singleIndexOf(arr, v) {
  var idx = -1;
  var i, l;
  if (!arr) {
    return -1;
  }
  for (i = 0, l = arr.length; i < l; i++) {
    if (arr[i] === v) {
      if (idx >= 0) {
        return -2;
      }
      idx = i;
    }
  }
  return idx;
}

/**
 * Convert array to map.
 *
 * @param arr {Array} Elements.
 * @param fn {Function} Function returning an element's key.
 */
function toMap(arr, fn) {
  var obj = {};
  var i, elem;
  for (i = 0; i < arr.length; i++) {
    elem = arr[i];
    obj[fn(elem)] = elem;
  }
  return obj;
}

/**
 * Convert map to array of values (polyfill for `Object.values`).
 *
 * @param obj {Object} Map.
 */
function objectValues(obj) {
  return Object.keys(obj).map(function (key) { return obj[key]; });
}

/**
 * Check whether an array has duplicates.
 *
 * @param arr {Array} The array.
 * @param fn {Function} Optional function to apply to each element.
 */
function hasDuplicates(arr, fn) {
  var obj = {};
  var i, l, elem;
  for (i = 0, l = arr.length; i < l; i++) {
    elem = arr[i];
    if (fn) {
      elem = fn(elem);
    }
    if (obj[elem]) {
      return true;
    }
    obj[elem] = true;
  }
  return false;
}

/**
 * Copy properties from one object to another.
 *
 * @param src {Object} The source object.
 * @param dst {Object} The destination object.
 * @param overwrite {Boolean} Whether to overwrite existing destination
 * properties. Defaults to false.
 */
function copyOwnProperties(src, dst, overwrite) {
  var names = Object.getOwnPropertyNames(src);
  var i, l, name;
  for (i = 0, l = names.length; i < l; i++) {
    name = names[i];
    if (!dst.hasOwnProperty(name) || overwrite) {
      var descriptor = Object.getOwnPropertyDescriptor(src, name);
      Object.defineProperty(dst, name, descriptor);
    }
  }
  return dst;
}

/**
 * Returns offset in the string of the end of JSON object (-1 if past the end).
 *
 * To keep the implementation simple, this function isn't a JSON validator. It
 * will gladly return a result for invalid JSON (which is OK since that will be
 * promptly rejected by the JSON parser). What matters is that it is guaranteed
 * to return the correct end when presented with valid JSON.
 *
 * @param str {String} Input string containing serialized JSON..
 * @param idx {Number} Starting position.
 */
function jsonEnd(str, idx) {
  idx = idx | 0;

  // Handle the case of a simple literal separately.
  var c = str.charAt(idx++);
  if (/[\d-]/.test(c)) {
    while (/[eE\d.+-]/.test(str.charAt(idx))) {
      idx++;
    }
    return idx;
  } else if (/true|null/.test(str.slice(idx - 1, idx + 3))) {
    return idx + 3;
  } else if (/false/.test(str.slice(idx - 1, idx + 4))) {
    return idx + 4;
  }

  // String, object, or array.
  var depth = 0;
  var literal = false;
  do {
    switch (c) {
    case '{':
    case '[':
      if (!literal) { depth++; }
      break;
    case '}':
    case ']':
      if (!literal && !--depth) {
        return idx;
      }
      break;
    case '"':
      literal = !literal;
      if (!depth && !literal) {
        return idx;
      }
      break;
    case '\\':
      idx++; // Skip the next character.
    }
  } while ((c = str.charAt(idx++)));

  return -1;
}

/** "Abstract" function to help with "subclassing". */
function abstractFunction() { throw new Error('abstract'); }

/** Batch-deprecate "getters" from an object's prototype. */
function addDeprecatedGetters(obj, props) {
  var proto = obj.prototype;
  var i, l, prop, getter;
  for (i = 0, l = props.length; i < l; i++) {
    prop = props[i];
    getter = 'get' + capitalize(prop);
    proto[getter] = util.deprecate(
      createGetter(prop),
      'use `.' + prop + '` instead of `.' + getter + '()`'
    );
  }

  function createGetter(prop) {
    return function () {
      var delegate = this[prop];
      return typeof delegate == 'function' ?
        delegate.apply(this, arguments) :
        delegate;
    };
  }
}

/**
 * Generator of random things.
 *
 * Inspired by: http://stackoverflow.com/a/424445/1062617
 */
function Lcg(seed) {
  var a = 1103515245;
  var c = 12345;
  var m = Math.pow(2, 31);
  var state = Math.floor(seed || Math.random() * (m - 1));

  this._max = m;
  this._nextInt = function () { return state = (a * state + c) % m; };
}

Lcg.prototype.nextBoolean = function () {
  // jshint -W018
  return !!(this._nextInt() % 2);
};

Lcg.prototype.nextInt = function (start, end) {
  if (end === undefined) {
    end = start;
    start = 0;
  }
  end = end === undefined ? this._max : end;
  return start + Math.floor(this.nextFloat() * (end - start));
};

Lcg.prototype.nextFloat = function (start, end) {
  if (end === undefined) {
    end = start;
    start = 0;
  }
  end = end === undefined ? 1 : end;
  return start + (end - start) * this._nextInt() / this._max;
};

Lcg.prototype.nextString = function(len, flags) {
  len |= 0;
  flags = flags || 'aA';
  var mask = '';
  if (flags.indexOf('a') > -1) {
    mask += 'abcdefghijklmnopqrstuvwxyz';
  }
  if (flags.indexOf('A') > -1) {
    mask += 'ABCDEFGHIJKLMNOPQRSTUVWXYZ';
  }
  if (flags.indexOf('#') > -1) {
    mask += '0123456789';
  }
  if (flags.indexOf('!') > -1) {
    mask += '~`!@#$%^&*()_+-={}[]:";\'<>?,./|\\';
  }
  var result = [];
  for (var i = 0; i < len; i++) {
    result.push(this.choice(mask));
  }
  return result.join('');
};

Lcg.prototype.nextBuffer = function (len) {
  var arr = [];
  var i;
  for (i = 0; i < len; i++) {
    arr.push(this.nextInt(256));
  }
  return new Buffer(arr);
};

Lcg.prototype.choice = function (arr) {
  var len = arr.length;
  if (!len) {
    throw new Error('choosing from empty array');
  }
  return arr[this.nextInt(len)];
};

/**
 * Ordered queue which returns items consecutively.
 *
 * This is actually a heap by index, with the added requirements that elements
 * can only be retrieved consecutively.
 */
function OrderedQueue() {
  this._index = 0;
  this._items = [];
}

OrderedQueue.prototype.push = function (item) {
  var items = this._items;
  var i = items.length | 0;
  var j;
  items.push(item);
  while (i > 0 && items[i].index < items[j = ((i - 1) >> 1)].index) {
    item = items[i];
    items[i] = items[j];
    items[j] = item;
    i = j;
  }
};

OrderedQueue.prototype.pop = function () {
  var items = this._items;
  var len = (items.length - 1) | 0;
  var first = items[0];
  if (!first || first.index > this._index) {
    return null;
  }
  this._index++;
  if (!len) {
    items.pop();
    return first;
  }
  items[0] = items.pop();
  var mid = len >> 1;
  var i = 0;
  var i1, i2, j, item, c, c1, c2;
  while (i < mid) {
    item = items[i];
    i1 = (i << 1) + 1;
    i2 = (i + 1) << 1;
    c1 = items[i1];
    c2 = items[i2];
    if (!c2 || c1.index <= c2.index) {
      c = c1;
      j = i1;
    } else {
      c = c2;
      j = i2;
    }
    if (c.index >= item.index) {
      break;
    }
    items[j] = item;
    items[i] = c;
    i = j;
  }
  return first;
};

/**
 * A buffer associated with an index.
 *
 * It is optimized for performance, at the cost of failing silently when
 * overflowing the buffer. This is a purposeful trade-off given the expected
 * rarity of this case and the large performance hit necessary to enforce
 * validity. See `isValid` below for more information.
 */
function IndexedBuffer(buf, idx) {
  this.buffer = buf;
  this.index = idx | 0;
  if (this.index < 0) {
    throw new Error('negative offset');
  }
}

/**
 * Check that the indexed buffer is in a valid state.
 *
 * For efficiency reasons, none of the methods below will fail if an overflow
 * occurs (either read, skip, or write). For this reason, it is up to the
 * caller to always check that the read, skip, or write was valid by calling
 * this method.
 */
IndexedBuffer.prototype.isValid = function () { return this.index <= this.buffer.length; };

// Read, skip, write methods.
//
// These should fail silently when the buffer overflows. Note this is only
// required to be true when the functions are decoding valid objects. For
// example errors will still be thrown if a bad count is read, leading to a
// negative position offset (which will typically cause a failure in
// `readFixed`).

IndexedBuffer.prototype.readBoolean = function () { return !!this.buffer[this.index++]; };

IndexedBuffer.prototype.skipBoolean = function () { this.index++; };

IndexedBuffer.prototype.writeBoolean = function (b) { this.buffer[this.index++] = !!b; };

IndexedBuffer.prototype.readInt = IndexedBuffer.prototype.readLong = function () {
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

IndexedBuffer.prototype.skipInt = IndexedBuffer.prototype.skipLong = function () {
  var buf = this.buffer;
  while (buf[this.index++] & 0x80) {}
};

IndexedBuffer.prototype.writeInt = IndexedBuffer.prototype.writeLong = function (n) {
  var buf = this.buffer;
  var f, m;

  if (n >= -1073741824 && n < 1073741824) {
    // Won't overflow, we can use integer arithmetic.
    m = n >= 0 ? n << 1 : (~n << 1) | 1;
    do {
      buf[this.index] = m & 0x7f;
      m >>= 7;
    } while (m && (buf[this.index++] |= 0x80));
  } else {
    // We have to use slower floating arithmetic.
    f = n >= 0 ? n * 2 : (-n * 2) - 1;
    do {
      buf[this.index] = f & 0x7f;
      f /= 128;
    } while (f >= 1 && (buf[this.index++] |= 0x80));
  }
  this.index++;
};

IndexedBuffer.prototype.readFloat = function () {
  var buf = this.buffer;
  var idx = this.index;
  this.index += 4;
  if (this.index > buf.length) {
    return;
  }
  return this.buffer.readFloatLE(idx);
};

IndexedBuffer.prototype.skipFloat = function () { this.index += 4; };

IndexedBuffer.prototype.writeFloat = function (f) {
  var buf = this.buffer;
  var idx = this.index;
  this.index += 4;
  if (this.index > buf.length) {
    return;
  }
  return this.buffer.writeFloatLE(f, idx);
};

IndexedBuffer.prototype.readDouble = function () {
  var buf = this.buffer;
  var idx = this.index;
  this.index += 8;
  if (this.index > buf.length) {
    return;
  }
  return this.buffer.readDoubleLE(idx);
};

IndexedBuffer.prototype.skipDouble = function () { this.index += 8; };

IndexedBuffer.prototype.writeDouble = function (d) {
  var buf = this.buffer;
  var idx = this.index;
  this.index += 8;
  if (this.index > buf.length) {
    return;
  }
  return this.buffer.writeDoubleLE(d, idx);
};

IndexedBuffer.prototype.readFixed = function (len) {
  var idx = this.index;
  this.index += len;
  if (this.index > this.buffer.length) {
    return;
  }
  var fixed = new Buffer(len);
  this.buffer.copy(fixed, 0, idx, idx + len);
  return fixed;
};

IndexedBuffer.prototype.skipFixed = function (len) { this.index += len; };

IndexedBuffer.prototype.writeFixed = function (buf, len) {
  len = len || buf.length;
  var idx = this.index;
  this.index += len;
  if (this.index > this.buffer.length) {
    return;
  }
  buf.copy(this.buffer, idx, 0, len);
};

IndexedBuffer.prototype.readBytes = function () {
  return this.readFixed(this.readLong());
};

IndexedBuffer.prototype.skipBytes = function () {
  var len = this.readLong();
  this.index += len;
};

IndexedBuffer.prototype.writeBytes = function (buf) {
  var len = buf.length;
  this.writeLong(len);
  this.writeFixed(buf, len);
};

/* istanbul ignore else */
if (typeof Buffer.prototype.utf8Slice == 'function') {
  // Use this optimized function when available.
  IndexedBuffer.prototype.readString = function () {
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
  IndexedBuffer.prototype.readString = function () {
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

IndexedBuffer.prototype.skipString = function () {
  var len = this.readLong();
  this.index += len;
};

IndexedBuffer.prototype.writeString = function (s) {
  var len = Buffer.byteLength(s);
  var buf = this.buffer;
  this.writeLong(len);
  var idx = this.index;
  this.index += len;
  if (this.index > buf.length) {
    return;
  }
  var i, l, c1, c2;
  for (i = 0, l = s.length; i < l; i++) {
    c1 = s.charCodeAt(i);
    if (c1 < 0x80) {
      buf[idx++] = c1;
    } else if (c1 < 0x800) {
      buf[idx++] = c1 >> 6 | 0xc0;
      buf[idx++] = c1 & 0x3f | 0x80;
    } else if (
      (c1 & 0xfc00) === 0xd800 &&
      ((c2 = s.charCodeAt(i + 1)) & 0xfc00) === 0xdc00
    ) {
      c1 = 0x10000 + ((c1 & 0x03ff) << 10) + (c2 & 0x03ff);
      i++;
      buf[idx++] = c1 >> 18 | 0xf0;
      buf[idx++] = c1 >> 12 & 0x3f | 0x80;
      buf[idx++] = c1 >> 6 & 0x3f | 0x80;
      buf[idx++] = c1 & 0x3f | 0x80;
    } else {
      buf[idx++] = c1 >> 12 | 0xe0;
      buf[idx++] = c1 >> 6 & 0x3f | 0x80;
      buf[idx++] = c1 & 0x3f | 0x80;
    }
  }
};

/* istanbul ignore else */
if (typeof Buffer.prototype.latin1Write == 'function') {
  // `binaryWrite` has been renamed to `latin1Write` in Node v6.4.0, see
  // https://github.com/nodejs/node/pull/7111. Note that the `'binary'`
  // encoding argument still works however.
  IndexedBuffer.prototype.writeBinary = function (str, len) {
    var idx = this.index;
    this.index += len;
    if (this.index > this.buffer.length) {
      return;
    }
    this.buffer.latin1Write(str, idx, len);
  };
} else if (typeof Buffer.prototype.binaryWrite == 'function') {
  IndexedBuffer.prototype.writeBinary = function (str, len) {
    var idx = this.index;
    this.index += len;
    if (this.index > this.buffer.length) {
      return;
    }
    this.buffer.binaryWrite(str, idx, len);
  };
} else {
  // Slowest implementation.
  IndexedBuffer.prototype.writeBinary = function (s, len) {
    var idx = this.index;
    this.index += len;
    if (this.index > this.buffer.length) {
      return;
    }
    this.buffer.write(s, idx, len, 'binary');
  };
}

// Binary comparison methods.
//
// These are not guaranteed to consume the objects they are comparing when
// returning a non-zero result (allowing for performance benefits), so no other
// operations should be done on either tap after a compare returns a non-zero
// value. Also, these methods do not have the same silent failure requirement
// as read, skip, and write since they are assumed to be called on valid
// buffers.

IndexedBuffer.prototype.matchBoolean = function (ibuf) {
  return this.buffer[this.index++] - ibuf.buffer[ibuf.index++];
};

IndexedBuffer.prototype.matchInt = IndexedBuffer.prototype.matchLong = function (ibuf) {
  var n1 = this.readLong();
  var n2 = ibuf.readLong();
  return n1 === n2 ? 0 : (n1 < n2 ? -1 : 1);
};

IndexedBuffer.prototype.matchFloat = function (ibuf) {
  var n1 = this.readFloat();
  var n2 = ibuf.readFloat();
  return n1 === n2 ? 0 : (n1 < n2 ? -1 : 1);
};

IndexedBuffer.prototype.matchDouble = function (ibuf) {
  var n1 = this.readDouble();
  var n2 = ibuf.readDouble();
  return n1 === n2 ? 0 : (n1 < n2 ? -1 : 1);
};

IndexedBuffer.prototype.matchFixed = function (ibuf, len) {
  return this.readFixed(len).compare(ibuf.readFixed(len));
};

IndexedBuffer.prototype.matchBytes = IndexedBuffer.prototype.matchString = function (ibuf) {
  var l1 = this.readLong();
  var p1 = this.index;
  this.index += l1;
  var l2 = ibuf.readLong();
  var p2 = ibuf.index;
  ibuf.index += l2;
  var b1 = this.buffer.slice(p1, this.index);
  var b2 = ibuf.buffer.slice(p2, ibuf.index);
  return b1.compare(b2);
};

// Functions for supporting custom long classes.
//
// The two following methods allow the long implementations to not have to
// worry about Avro's zigzag encoding, we directly expose longs as unpacked.

IndexedBuffer.prototype.unpackLongBytes = function () {
  var res = new Buffer(8);
  var n = 0;
  var i = 0; // Byte index in target buffer.
  var j = 6; // Bit offset in current target buffer byte.
  var buf = this.buffer;
  var b, neg;

  b = buf[this.index++];
  neg = b & 1;
  res.fill(0);

  n |= (b & 0x7f) >> 1;
  while (b & 0x80) {
    b = buf[this.index++];
    n |= (b & 0x7f) << j;
    j += 7;
    if (j >= 8) {
      // Flush byte.
      j -= 8;
      res[i++] = n;
      n >>= 8;
    }
  }
  res[i] = n;

  if (neg) {
    invert(res, 8);
  }

  return res;
};

IndexedBuffer.prototype.packLongBytes = function (buf) {
  var neg = (buf[7] & 0x80) >> 7;
  var res = this.buffer;
  var j = 1;
  var k = 0;
  var m = 3;
  var n;

  if (neg) {
    invert(buf, 8);
    n = 1;
  } else {
    n = 0;
  }

  var parts = [
    buf.readUIntLE(0, 3),
    buf.readUIntLE(3, 3),
    buf.readUIntLE(6, 2)
  ];
  // Not reading more than 24 bits because we need to be able to combine the
  // "carry" bits from the previous part and JavaScript only supports bitwise
  // operations on 32 bit integers.
  while (m && !parts[--m]) {} // Skip trailing 0s.

  // Leading parts (if any), we never bail early here since we need the
  // continuation bit to be set.
  while (k < m) {
    n |= parts[k++] << j;
    j += 24;
    while (j > 7) {
      res[this.index++] = (n & 0x7f) | 0x80;
      n >>= 7;
      j -= 7;
    }
  }

  // Final part, similar to normal packing aside from the initial offset.
  n |= parts[m] << j;
  do {
    res[this.index] = n & 0x7f;
    n >>= 7;
  } while (n && (res[this.index++] |= 0x80));
  this.index++;

  // Restore original buffer (could make this optional?).
  if (neg) {
    invert(buf, 8);
  }
};

// Helpers.

/**
 * Invert all bits in a buffer.
 *
 * @param buf {Buffer} Non-empty buffer to invert.
 * @param len {Number} Buffer length (must be positive).
 */
function invert(buf, len) {
  while (len--) {
    buf[len] = ~buf[len];
  }
}


module.exports = {
  abstractFunction: abstractFunction,
  addDeprecatedGetters: addDeprecatedGetters,
  capitalize: capitalize,
  copyOwnProperties: copyOwnProperties,
  getHash: getHash,
  compare: compare,
  getOption: getOption,
  jsonEnd: jsonEnd,
  objectValues: objectValues,
  toMap: toMap,
  singleIndexOf: singleIndexOf,
  hasDuplicates: hasDuplicates,
  IndexedBuffer: IndexedBuffer,
  Lcg: Lcg,
  OrderedQueue: OrderedQueue
};
