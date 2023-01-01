// TODO: Make long comparison impervious to precision loss.
// TODO: Optimize binary comparison methods.

'use strict';

/** Various utilities used across this library. */

let buffer = require('buffer');
let platform = require('./platform');

let Buffer = buffer.Buffer;

// Valid (field, type, and symbol) name regex.
const NAME_PATTERN = /^[A-Za-z_][A-Za-z0-9_]*$/;

/**
 * Create a new empty buffer.
 *
 * @param size {Number} The buffer's size.
 */
function newBuffer(size) {
  return Buffer.alloc(size);
}

/**
 * Create a new buffer with the input contents.
 *
 * @param data {Array|String} The buffer's data.
 * @param enc {String} Encoding, used if data is a string.
 */
function bufferFrom(data, enc) {
  return Buffer.from(data, enc);
}

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
  let value = opts[key];
  return value === undefined ? def : value;
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
  let pos = -1;
  if (!arr) {
    return -1;
  }
  for (let i = 0, l = arr.length; i < l; i++) {
    if (arr[i] === v) {
      if (pos >= 0) {
        return -2;
      }
      pos = i;
    }
  }
  return pos;
}

/**
 * Convert array to map.
 *
 * @param arr {Array} Elements.
 * @param fn {Function} Function returning an element's key.
 */
function toMap(arr, fn) {
  let obj = {};
  for (let i = 0; i < arr.length; i++) {
    let elem = arr[i];
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
  return Object.keys(obj).map((key) => { return obj[key]; });
}

/**
 * Check whether an array has duplicates.
 *
 * @param arr {Array} The array.
 * @param fn {Function} Optional function to apply to each element.
 */
function hasDuplicates(arr, fn) {
  let obj = Object.create(null);
  for (let i = 0, l = arr.length; i < l; i++) {
    let elem = arr[i];
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
  let names = Object.getOwnPropertyNames(src);
  for (let i = 0, l = names.length; i < l; i++) {
    let name = names[i];
    if (!Object.prototype.hasOwnProperty.call(dst, name) || overwrite) {
      let descriptor = Object.getOwnPropertyDescriptor(src, name);
      Object.defineProperty(dst, name, descriptor);
    }
  }
  return dst;
}

/**
 * Check whether a string is a valid Avro identifier.
 */
function isValidName(str) { return NAME_PATTERN.test(str); }

/**
 * Verify and return fully qualified name.
 *
 * @param name {String} Full or short name. It can be prefixed with a dot to
 * force global namespace.
 * @param namespace {String} Optional namespace.
 */
function qualify(name, namespace) {
  if (~name.indexOf('.')) {
    name = name.replace(/^\./, ''); // Allow absolute referencing.
  } else if (namespace) {
    name = namespace + '.' + name;
  }
  name.split('.').forEach((part) => {
    if (!isValidName(part)) {
      throw new Error(`invalid name: ${printJSON(name)}`);
    }
  });
  return name;
}

/**
 * Remove namespace from a name.
 *
 * @param name {String} Full or short name.
 */
function unqualify(name) {
  let parts = name.split('.');
  return parts[parts.length - 1];
}

/**
 * Return the namespace implied by a name.
 *
 * @param name {String} Full or short name. If short, the returned namespace
 *  will be empty.
 */
function impliedNamespace(name) {
  let match = /^(.*)\.[^.]+$/.exec(name);
  return match ? match[1] : undefined;
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
 * @param pos {Number} Starting position.
 */
function jsonEnd(str, pos) {
  pos = pos | 0;

  // Handle the case of a simple literal separately.
  let c = str.charAt(pos++);
  if (/[\d-]/.test(c)) {
    while (/[eE\d.+-]/.test(str.charAt(pos))) {
      pos++;
    }
    return pos;
  } else if (/true|null/.test(str.slice(pos - 1, pos + 3))) {
    return pos + 3;
  } else if (/false/.test(str.slice(pos - 1, pos + 4))) {
    return pos + 4;
  }

  // String, object, or array.
  let depth = 0;
  let literal = false;
  do {
    switch (c) {
      case '{':
      case '[':
        if (!literal) { depth++; }
        break;
      case '}':
      case ']':
        if (!literal && !--depth) {
          return pos;
        }
        break;
      case '"':
        literal = !literal;
        if (!depth && !literal) {
          return pos;
        }
        break;
      case '\\':
        pos++; // Skip the next character.
    }
  } while ((c = str.charAt(pos++)));

  return -1;
}

/** "Abstract" function to help with "subclassing". */
function abstractFunction() { throw new Error('abstract'); }

/** Batch-deprecate "getters" from an object's prototype. */
function addDeprecatedGetters(obj, props) {
  let proto = obj.prototype;
  for (let i = 0, l = props.length; i < l; i++) {
    let prop = props[i];
    let getter = 'get' + capitalize(prop);
    proto[getter] = platform.deprecate(
      createGetter(prop),
      'use `.' + prop + '` instead of `.' + getter + '()`'
    );
  }

  function createGetter(prop) {
    return function () {
      let delegate = this[prop];
      return typeof delegate == 'function' ?
        delegate.apply(this, arguments) :
        delegate;
    };
  }
}

/**
 * Simple buffer pool to avoid allocating many small buffers.
 *
 * This provides significant speedups in recent versions of node (6+).
 */
class BufferPool {
  constructor (len) {
    this._len = len | 0;
    this._pos = 0;
    this._slab = newBuffer(this._len);
  }

  alloc (len) {
    if (len < 0) {
      throw new Error('negative length');
    }
    let maxLen = this._len;
    if (len > maxLen) {
      return newBuffer(len);
    }
    if (this._pos + len > maxLen) {
      this._slab = newBuffer(maxLen);
      this._pos = 0;
    }
    return this._slab.slice(this._pos, this._pos += len);
  }
}

/**
 * Generator of random things.
 *
 * Inspired by: http://stackoverflow.com/a/424445/1062617
 */
class Lcg {
  constructor (seed) {
    let a = 1103515245;
    let c = 12345;
    let m = Math.pow(2, 31);
    let state = Math.floor(seed || Math.random() * (m - 1));

    this._max = m;
    this._nextInt = function () {
      state = (a * state + c) % m;
      return state;
    };
  }

  nextBoolean () {
    return !!(this._nextInt() % 2);
  }

  nextInt (start, end) {
    if (end === undefined) {
      end = start;
      start = 0;
    }
    end = end === undefined ? this._max : end;
    return start + Math.floor(this.nextFloat() * (end - start));
  }

  nextFloat (start, end) {
    if (end === undefined) {
      end = start;
      start = 0;
    }
    end = end === undefined ? 1 : end;
    return start + (end - start) * this._nextInt() / this._max;
  }

  nextString(len, flags) {
    len |= 0;
    flags = flags || 'aA';
    let mask = '';
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
    let result = [];
    for (let i = 0; i < len; i++) {
      result.push(this.choice(mask));
    }
    return result.join('');
  }

  nextBuffer (len) {
    let arr = [];
    for (let i = 0; i < len; i++) {
      arr.push(this.nextInt(256));
    }
    return bufferFrom(arr);
  }

  choice (arr) {
    let len = arr.length;
    if (!len) {
      throw new Error('choosing from empty array');
    }
    return arr[this.nextInt(len)];
  }
}

/**
 * Ordered queue which returns items consecutively.
 *
 * This is actually a heap by index, with the added requirements that elements
 * can only be retrieved consecutively.
 */
class OrderedQueue {
  constructor () {
    this._index = 0;
    this._items = [];
  }

  push (item) {
    let items = this._items;
    let i = items.length | 0;
    let j;
    items.push(item);
    while (i > 0 && items[i].index < items[j = ((i - 1) >> 1)].index) {
      item = items[i];
      items[i] = items[j];
      items[j] = item;
      i = j;
    }
  }

  pop () {
    let items = this._items;
    let len = (items.length - 1) | 0;
    let first = items[0];
    if (!first || first.index > this._index) {
      return null;
    }
    this._index++;
    if (!len) {
      items.pop();
      return first;
    }
    items[0] = items.pop();
    let mid = len >> 1;
    let i = 0;
    let i1, i2, j, item, c, c1, c2;
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
  }
}

// Shared buffer pool for all taps.
const POOL = new BufferPool(4096);

/**
 * A tap is a buffer which remembers what has been already read.
 *
 * It is optimized for performance, at the cost of failing silently when
 * overflowing the buffer. This is a purposeful trade-off given the expected
 * rarity of this case and the large performance hit necessary to enforce
 * validity. See `isValid` below for more information.
 */
class Tap {
  constructor (buf, pos) {
    this.buf = buf;
    this.pos = pos | 0;
    if (this.pos < 0) {
      throw new Error('negative offset');
    }
  }

  /**
   * Check that the tap is in a valid state.
   *
   * For efficiency reasons, none of the methods below will fail if an overflow
   * occurs (either read, skip, or write). For this reason, it is up to the
   * caller to always check that the read, skip, or write was valid by calling
   * this method.
   */
  isValid () { return this.pos <= this.buf.length; }

  _invalidate () { this.pos = this.buf.length + 1; }

  // Read, skip, write methods.
  //
  // These should fail silently when the buffer overflows. Note this is only
  // required to be true when the functions are decoding valid objects. For
  // example errors will still be thrown if a bad count is read, leading to a
  // negative position offset (which will typically cause a failure in
  // `readFixed`).

  readBoolean () { return !!this.buf[this.pos++]; }

  skipBoolean () { this.pos++; }

  writeBoolean (b) { this.buf[this.pos++] = !!b; }

  readLong () {
    let n = 0;
    let k = 0;
    let buf = this.buf;
    let b, h, f, fk;

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
  }

  skipLong () {
    let buf = this.buf;
    while (buf[this.pos++] & 0x80) {}
  }

  writeLong (n) {
    let buf = this.buf;
    let f, m;

    if (n >= -1073741824 && n < 1073741824) {
      // Won't overflow, we can use integer arithmetic.
      m = n >= 0 ? n << 1 : (~n << 1) | 1;
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
  }

  readFloat () {
    let buf = this.buf;
    let pos = this.pos;
    this.pos += 4;
    if (this.pos > buf.length) {
      return 0;
    }
    return this.buf.readFloatLE(pos);
  }

  skipFloat () { this.pos += 4; }

  writeFloat (f) {
    let buf = this.buf;
    let pos = this.pos;
    this.pos += 4;
    if (this.pos > buf.length) {
      return;
    }
    return this.buf.writeFloatLE(f, pos);
  }

  readDouble () {
    let buf = this.buf;
    let pos = this.pos;
    this.pos += 8;
    if (this.pos > buf.length) {
      return 0;
    }
    return this.buf.readDoubleLE(pos);
  }

  skipDouble () { this.pos += 8; }

  writeDouble (d) {
    let buf = this.buf;
    let pos = this.pos;
    this.pos += 8;
    if (this.pos > buf.length) {
      return;
    }
    return this.buf.writeDoubleLE(d, pos);
  }

  readFixed (len) {
    let pos = this.pos;
    this.pos += len;
    if (this.pos > this.buf.length) {
      return;
    }
    let fixed = POOL.alloc(len);
    this.buf.copy(fixed, 0, pos, pos + len);
    return fixed;
  }

  skipFixed (len) { this.pos += len; }

  writeFixed (buf, len) {
    len = len || buf.length;
    let pos = this.pos;
    this.pos += len;
    if (this.pos > this.buf.length) {
      return;
    }
    buf.copy(this.buf, pos, 0, len);
  }

  readBytes () {
    let len = this.readLong();
    if (len < 0) {
      this._invalidate();
      return;
    }
    return this.readFixed(len);
  }

  skipBytes () {
    let len = this.readLong();
    if (len < 0) {
      this._invalidate();
      return;
    }
    this.pos += len;
  }

  writeBytes (buf) {
    let len = buf.length;
    this.writeLong(len);
    this.writeFixed(buf, len);
  }

  skipString () {
    let len = this.readLong();
    if (len < 0) {
      this._invalidate();
      return;
    }
    this.pos += len;
  }

  writeString (s) {
    let len = Buffer.byteLength(s);
    let buf = this.buf;
    this.writeLong(len);
    let pos = this.pos;
    this.pos += len;
    if (this.pos > buf.length) {
      return;
    }
    if (len > 64 && typeof Buffer.prototype.utf8Write == 'function') {
      // This method is roughly 50% faster than the manual implementation below
      // for long strings (which is itself faster than the generic
      // `Buffer#write` at least in most browsers, where `utf8Write` is not
      // available).
      buf.utf8Write(s, pos, len);
    } else {
      for (let i = 0, l = len; i < l; i++) {
        let c1 = s.charCodeAt(i);
        let c2;
        if (c1 < 0x80) {
          buf[pos++] = c1;
        } else if (c1 < 0x800) {
          buf[pos++] = c1 >> 6 | 0xc0;
          buf[pos++] = c1 & 0x3f | 0x80;
        } else if (
          (c1 & 0xfc00) === 0xd800 &&
          ((c2 = s.charCodeAt(i + 1)) & 0xfc00) === 0xdc00
        ) {
          c1 = 0x10000 + ((c1 & 0x03ff) << 10) + (c2 & 0x03ff);
          i++;
          buf[pos++] = c1 >> 18 | 0xf0;
          buf[pos++] = c1 >> 12 & 0x3f | 0x80;
          buf[pos++] = c1 >> 6 & 0x3f | 0x80;
          buf[pos++] = c1 & 0x3f | 0x80;
        } else {
          buf[pos++] = c1 >> 12 | 0xe0;
          buf[pos++] = c1 >> 6 & 0x3f | 0x80;
          buf[pos++] = c1 & 0x3f | 0x80;
        }
      }
    }
  }

  // Binary comparison methods.
  //
  // These are not guaranteed to consume the objects they are comparing when
  // returning a non-zero result (allowing for performance benefits), so no
  // other operations should be done on either tap after a compare returns a
  // non-zero value. Also, these methods do not have the same silent failure
  // requirement as read, skip, and write since they are assumed to be called on
  // valid buffers.

  matchBoolean (tap) {
    return this.buf[this.pos++] - tap.buf[tap.pos++];
  }

  matchLong (tap) {
    let n1 = this.readLong();
    let n2 = tap.readLong();
    return n1 === n2 ? 0 : (n1 < n2 ? -1 : 1);
  }

  matchFloat (tap) {
    let n1 = this.readFloat();
    let n2 = tap.readFloat();
    return n1 === n2 ? 0 : (n1 < n2 ? -1 : 1);
  }

  matchDouble (tap) {
    let n1 = this.readDouble();
    let n2 = tap.readDouble();
    return n1 === n2 ? 0 : (n1 < n2 ? -1 : 1);
  }

  matchFixed (tap, len) {
    return this.readFixed(len).compare(tap.readFixed(len));
  }

  matchBytes (tap) {
    let l1 = this.readLong();
    let p1 = this.pos;
    this.pos += l1;
    let l2 = tap.readLong();
    let p2 = tap.pos;
    tap.pos += l2;
    let b1 = this.buf.slice(p1, this.pos);
    let b2 = tap.buf.slice(p2, tap.pos);
    return b1.compare(b2);
  }

  // Functions for supporting custom long classes.
  //
  // The two following methods allow the long implementations to not have to
  // worry about Avro's zigzag encoding, we directly expose longs as unpacked.

  unpackLongBytes () {
    let res = newBuffer(8);
    let n = 0;
    let i = 0; // Byte index in target buffer.
    let j = 6; // Bit offset in current target buffer byte.
    let buf = this.buf;

    let b = buf[this.pos++];
    let neg = b & 1;
    res.fill(0);

    n |= (b & 0x7f) >> 1;
    while (b & 0x80) {
      b = buf[this.pos++];
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
  }

  packLongBytes (buf) {
    let neg = (buf[7] & 0x80) >> 7;
    let res = this.buf;
    let j = 1;
    let k = 0;
    let m = 3;
    let n;

    if (neg) {
      invert(buf, 8);
      n = 1;
    } else {
      n = 0;
    }

    let parts = [
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
        res[this.pos++] = (n & 0x7f) | 0x80;
        n >>= 7;
        j -= 7;
      }
    }

    // Final part, similar to normal packing aside from the initial offset.
    n |= parts[m] << j;
    do {
      res[this.pos] = n & 0x7f;
      n >>= 7;
    } while (n && (res[this.pos++] |= 0x80));
    this.pos++;

    // Restore original buffer (could make this optional?).
    if (neg) {
      invert(buf, 8);
    }
  }
}

// TODO: check if the following optimizations are really worth it or if they're
// premature micro-optimizations.

/* istanbul ignore else */
if (typeof Buffer.prototype.utf8Slice == 'function') {
  // Use this optimized function when available.
  Tap.prototype.readString = function () {
    let len = this.readLong();
    if (len < 0) {
      this._invalidate();
      return '';
    }
    let pos = this.pos;
    let buf = this.buf;
    this.pos += len;
    if (this.pos > buf.length) {
      return;
    }
    return this.buf.utf8Slice(pos, pos + len);
  };
} else {
  Tap.prototype.readString = function () {
    let len = this.readLong();
    if (len < 0) {
      this._invalidate();
      return '';
    }
    let pos = this.pos;
    let buf = this.buf;
    this.pos += len;
    if (this.pos > buf.length) {
      return;
    }
    return this.buf.slice(pos, pos + len).toString();
  };
}

/* istanbul ignore else */
if (typeof Buffer.prototype.latin1Write == 'function') {
  // `binaryWrite` has been renamed to `latin1Write` in Node v6.4.0, see
  // https://github.com/nodejs/node/pull/7111. Note that the `'binary'`
  // encoding argument still works however.
  Tap.prototype.writeBinary = function (str, len) {
    let pos = this.pos;
    this.pos += len;
    if (this.pos > this.buf.length) {
      return;
    }
    this.buf.latin1Write(str, pos, len);
  };
} else if (typeof Buffer.prototype.binaryWrite == 'function') {
  Tap.prototype.writeBinary = function (str, len) {
    let pos = this.pos;
    this.pos += len;
    if (this.pos > this.buf.length) {
      return;
    }
    this.buf.binaryWrite(str, pos, len);
  };
} else {
  // Slowest implementation.
  Tap.prototype.writeBinary = function (s, len) {
    let pos = this.pos;
    this.pos += len;
    if (this.pos > this.buf.length) {
      return;
    }
    this.buf.write(s, pos, len, 'binary');
  };
}

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

/**
 * Prints an object as a string; mostly used for printing objects in errors.
 * @param {object} obj The object to display.
 * @returns The object as JSON.
 */
function printJSON (obj) {
  let seen = new Set();
  try {
    return JSON.stringify(obj, (key, value) => {
      if (seen.has(value)) return '[Circular]';
      if (typeof value === 'object' && value !== null) seen.add(value);
      // eslint-disable-next-line no-undef
      if (typeof BigInt !== 'undefined' && (value instanceof BigInt)) {
        return `[BigInt ${value.toString()}n]`;
      }
      return value;
    });
  } catch (err) {
    return '[object]';
  }
}

module.exports = {
  abstractFunction,
  addDeprecatedGetters,
  bufferFrom,
  capitalize,
  copyOwnProperties,
  getHash: platform.getHash,
  compare,
  getOption,
  impliedNamespace,
  isValidName,
  jsonEnd,
  newBuffer,
  objectValues,
  qualify,
  toMap,
  singleIndexOf,
  hasDuplicates,
  unqualify,
  BufferPool,
  Lcg,
  OrderedQueue,
  Tap,
  printJSON
};
