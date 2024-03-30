// TODO: Make long comparison impervious to precision loss.
// TODO: Optimize binary comparison methods.

'use strict';

/** Various utilities used across this library. */

let platform = require('./platform');

// Valid (field, type, and symbol) name regex.
const NAME_PATTERN = /^[A-Za-z_][A-Za-z0-9_]*$/;

function isBufferLike(data) {
  return (data instanceof Uint8Array);
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

let bufCompare;
if (typeof Buffer == 'function') {
  bufCompare = Buffer.compare;
} else {
  bufCompare = function(buf1, buf2) {
    let len = Math.min(buf1.length, buf2.length);
    for (let i = 0; i < len; i++) {
      if (buf1[i] !== buf2[i]) {
        return Math.sign(buf1[i] - buf2[i]);
      }
    }
    return Math.sign(buf1.length - buf2.length);
  };
}

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
    return Buffer.from(arr);
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

let decodeSlice;
if (typeof Buffer === 'function' && Buffer.prototype.utf8Slice) {
  decodeSlice = function(arr, start, end) {
    return Buffer.prototype.utf8Slice.call(arr, start, end);
  };
} else {
  const DECODER = new TextDecoder();

  // Calling `subarray` is expensive enough that for small strings, it's faster
  // to decode manually.
  decodeSlice = function(arr, start, end) {
    if (end - start > 32) {
      return DECODER.decode(arr.subarray(start, end));
    }

    let output = '';
    let i = start;
    // Consume the string in 4-byte chunks. The performance benefit comes not
    // from *reading* in chunks, but calling fromCharCode with 4 characters per
    // call.
    while (i + 3 < end) {
      const n = (arr[i] << 24) |
          (arr[i + 1] << 16) |
          (arr[i + 2] << 8) |
          arr[i + 3];
      // If the high bit of any character is set, it's a non-ASCII character.
      // Fall back to TextDecoder for the remaining characters.
      if (n & 0x80808080) {
        output += DECODER.decode(arr.subarray(start + i, end));
        return output;
      }
      output += String.fromCharCode(
        n >>> 24,
        (n >> 16) & 0xff,
        (n >> 8) & 0xff,
        n & 0xff
      );
      i += 4;
    }

    // Handle the remainder of the string.
    while (i < end) {
      if (arr[i] & 0x80) {
        output += DECODER.decode(arr.subarray(start + i, end));
        return output;
      }
      output += String.fromCharCode(arr[i]);
      i++;
    }

    return output;
  };
}

const ENCODER = new TextEncoder();
const encodeBuf = new Uint8Array(4096);
const encodeBufs = [];
// Believe it or not, `subarray` is actually quite expensive. To avoid the cost,
// we call `subarray` once for each possible slice length and reuse those cached
// views.
for (let i = 0; i <= encodeBuf.length; i++) {
  encodeBufs.push(encodeBuf.subarray(0, i));
}

function encodeSlice(str) {
  const {read, written} = ENCODER.encodeInto(str, encodeBuf);
  if (read === str.length) {
    return encodeBufs[written];
  }

  return ENCODER.encode(str);
}

let utf8Length;
if (typeof Buffer === 'function') {
  utf8Length = Buffer.byteLength;
} else {
  utf8Length = function(str) {
    let len = 0;
    for (;;) {
      // encodeInto is faster than any manual implementation (or even
      // Buffer.byteLength), provided the string fits entirely within the
      // buffer. Past that, it slows down but is still faster than other
      // options.
      const {read, written} = ENCODER.encodeInto(str, encodeBuf);
      len += written;
      if (read === str.length) break;
      str = str.slice(read);
    }
    return len;
  };
}

// Having multiple views into the same buffer seems to massively decrease read
// performance. To read and write float and double types, copy them to and from
// this data view instead.
const FLOAT_VIEW = new DataView(new ArrayBuffer(8));

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
    this.setData(buf, pos);
  }

  setData (buf, pos) {
    if (typeof Buffer === 'function' && buf instanceof Buffer) {
      buf = new Uint8Array(buf.buffer, buf.byteOffset, buf.length);
    }
    this.arr = buf;
    this.pos = pos | 0;
    this.length = buf.length;
    if (this.pos < 0) {
      throw new Error('negative offset');
    }
  }

  reinitialize (capacity) {
    this.setData(new Uint8Array(capacity));
  }

  static fromBuffer (buf, pos) {
    return new Tap(buf, pos);
  }

  static withCapacity (capacity) {
    let buf = new Uint8Array(capacity);
    return new Tap(buf);
  }

  toBuffer () {
    return this.arr.slice(0, this.pos);
  }

  subarray (start, end) {
    return this.arr.subarray(start, end);
  }

  append (newBuf) {
    const newArr = new Uint8Array(this.arr.length + newBuf.length);
    newArr.set(this.arr, 0);
    newArr.set(newBuf, this.arr.length);
    this.setData(newArr, 0);
  }

  forward (newBuf) {
    const subArr = this.arr.subarray(this.pos);
    const newArr = new Uint8Array(subArr.length + newBuf.length);
    newArr.set(subArr, 0);
    newArr.set(newBuf, subArr.length);
    this.setData(newArr, 0);
  }

  /**
   * Check that the tap is in a valid state.
   *
   * For efficiency reasons, none of the methods below will fail if an overflow
   * occurs (either read, skip, or write). For this reason, it is up to the
   * caller to always check that the read, skip, or write was valid by calling
   * this method.
   */
  isValid () { return this.pos <= this.length; }

  _invalidate () { this.pos = this.length + 1; }

  // Read, skip, write methods.
  //
  // These should fail silently when the buffer overflows. Note this is only
  // required to be true when the functions are decoding valid objects. For
  // example errors will still be thrown if a bad count is read, leading to a
  // negative position offset (which will typically cause a failure in
  // `readFixed`).

  readBoolean () { return !!this.arr[this.pos++]; }

  skipBoolean () { this.pos++; }

  writeBoolean (b) { this.arr[this.pos++] = !!b; }

  readLong () {
    let n = 0;
    let k = 0;
    let buf = this.arr;
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
    let buf = this.arr;
    while (buf[this.pos++] & 0x80) {}
  }

  writeLong (n) {
    let buf = this.arr;
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
    let pos = this.pos;
    this.pos += 4;
    if (this.pos > this.length) {
      return 0;
    }
    FLOAT_VIEW.setUint32(
      0,
      this.arr[pos] |
      (this.arr[pos + 1] << 8) |
      (this.arr[pos + 2] << 16) |
      (this.arr[pos + 3] << 24),
      true);
    return FLOAT_VIEW.getFloat32(0, true);
  }

  skipFloat () { this.pos += 4; }

  writeFloat (f) {
    let pos = this.pos;
    this.pos += 4;
    if (this.pos > this.length) {
      return;
    }

    FLOAT_VIEW.setFloat32(0, f, true);
    const n = FLOAT_VIEW.getUint32(0, true);
    this.arr[pos] = n & 0xff;
    this.arr[pos + 1] = (n >> 8) & 0xff;
    this.arr[pos + 2] = (n >> 16) & 0xff;
    this.arr[pos + 3] = n >> 24;
  }

  readDouble () {
    let pos = this.pos;
    this.pos += 8;
    if (this.pos > this.length) {
      return 0;
    }
    FLOAT_VIEW.setUint32(
      0,
      this.arr[pos] |
      (this.arr[pos + 1] << 8) |
      (this.arr[pos + 2] << 16) |
      (this.arr[pos + 3] << 24),
      true
    );
    FLOAT_VIEW.setUint32(
      4,
      this.arr[pos + 4] |
      (this.arr[pos + 5] << 8) |
      (this.arr[pos + 6] << 16) |
      (this.arr[pos + 7] << 24),
      true
    );
    return FLOAT_VIEW.getFloat64(0, true);
  }

  skipDouble () { this.pos += 8; }

  writeDouble (d) {
    let pos = this.pos;
    this.pos += 8;
    if (this.pos > this.length) {
      return;
    }
    FLOAT_VIEW.setFloat64(0, d, true);
    const a = FLOAT_VIEW.getUint32(0, true);
    const b = FLOAT_VIEW.getUint32(4, true);
    this.arr[pos] = a & 0xff;
    this.arr[pos + 1] = (a >> 8) & 0xff;
    this.arr[pos + 2] = (a >> 16) & 0xff;
    this.arr[pos + 3] = a >> 24;
    this.arr[pos + 4] = b & 0xff;
    this.arr[pos + 5] = (b >> 8) & 0xff;
    this.arr[pos + 6] = (b >> 16) & 0xff;
    this.arr[pos + 7] = b >> 24;
  }

  readFixed (len) {
    let pos = this.pos;
    this.pos += len;
    if (this.pos > this.length) {
      return;
    }
    return this.arr.slice(pos, pos + len);
  }

  skipFixed (len) { this.pos += len; }

  writeFixed (buf, len) {
    len = len || buf.length;
    let pos = this.pos;
    this.pos += len;
    if (this.pos > this.length) {
      return;
    }
    this.arr.set(buf.subarray(0, len), pos);
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

  readString () {
    let len = this.readLong();
    if (len < 0) {
      this._invalidate();
      return '';
    }
    let pos = this.pos;
    this.pos += len;
    if (this.pos > this.length) {
      return;
    }
    return decodeSlice(this.arr, pos, pos + len);
  }

  writeString (s) {
    let buf = this.arr;
    const stringLen = s.length;
    // The maximum number that a signed varint can store in a single byte is 63.
    // The maximum size of a UTF-8 representation of a UTF-16 string is 3 times
    // its length, as one UTF-16 character can be represented by up to 3 bytes
    // in UTF-8. Therefore, if the string is 21 characters or less, we know that
    // its length can be stored in a single byte, which is why we choose 21 as
    // the small-string threshold specifically.
    if (stringLen > 21) {
      let encodedLength, encoded;

      // If we're already over the buffer size, we don't need to encode the
      // string. While encodeInto is actually faster than Buffer.byteLength, we
      // could still overflow the preallocated encoding buffer and have to fall
      // back to allocating, which is really really slow.
      if (this.isValid()) {
        encoded = encodeSlice(s);
        encodedLength = encoded.length;
      } else {
        encodedLength = utf8Length(s);
      }
      this.writeLong(encodedLength);
      let pos = this.pos;

      if (this.isValid() && typeof encoded != 'undefined') {
        buf.set(encoded, pos);
      }

      this.pos += encodedLength;
    } else {
      // For small strings, this manual implementation is faster.

      // Set aside 1 byte to write the string length.
      let pos = this.pos + 1;
      let startPos = pos;
      let bufLen = buf.length;

      // This is not a micro-optimization: caching the string length for the
      // loop predicate really does make a difference!
      for (let i = 0; i < stringLen; i++) {
        let c1 = s.charCodeAt(i);
        let c2;
        if (c1 < 0x80) {
          if (pos < bufLen) buf[pos] = c1;
          pos++;
        } else if (c1 < 0x800) {
          if (pos + 1 < bufLen) {
            buf[pos] = c1 >> 6 | 0xc0;
            buf[pos + 1] = c1 & 0x3f | 0x80;
          }
          pos += 2;
        } else if (
          (c1 & 0xfc00) === 0xd800 &&
          ((c2 = s.charCodeAt(i + 1)) & 0xfc00) === 0xdc00
        ) {
          c1 = 0x10000 + ((c1 & 0x03ff) << 10) + (c2 & 0x03ff);
          i++;
          if (pos + 3 < bufLen) {
            buf[pos] = c1 >> 18 | 0xf0;
            buf[pos + 1] = c1 >> 12 & 0x3f | 0x80;
            buf[pos + 2] = c1 >> 6 & 0x3f | 0x80;
            buf[pos + 3] = c1 & 0x3f | 0x80;
          }
          pos += 4;
        } else {
          if (pos + 2 < bufLen) {
            buf[pos] = c1 >> 12 | 0xe0;
            buf[pos + 1] = c1 >> 6 & 0x3f | 0x80;
            buf[pos + 2] = c1 & 0x3f | 0x80;
          }
          pos += 3;
        }
      }

      // Note that we've not yet updated this.pos, so it's currently pointing to
      // the place where we want to write the string length.
      if (this.pos <= bufLen) {
        this.writeLong(pos - startPos);
      }

      this.pos = pos;
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
    return this.arr[this.pos++] - tap.arr[tap.pos++];
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
    return bufCompare(this.readFixed(len), tap.readFixed(len));
  }

  matchBytes (tap) {
    let l1 = this.readLong();
    let p1 = this.pos;
    this.pos += l1;
    let l2 = tap.readLong();
    let p2 = tap.pos;
    tap.pos += l2;
    let b1 = this.arr.subarray(p1, this.pos);
    let b2 = tap.arr.subarray(p2, tap.pos);
    return bufCompare(b1, b2);
  }

  // Functions for supporting custom long classes.
  //
  // The two following methods allow the long implementations to not have to
  // worry about Avro's zigzag encoding, we directly expose longs as unpacked.

  unpackLongBytes () {
    let res = new Uint8Array(8);
    let n = 0;
    let i = 0; // Byte index in target buffer.
    let j = 6; // Bit offset in current target buffer byte.
    let buf = this.arr;

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
    let res = this.arr;
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
      (buf[0] | (buf[1] << 8) | (buf[2] << 16)),
      (buf[3] | (buf[4] << 8) | (buf[5] << 16)),
      (buf[6] | (buf[7] << 8))
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

// Helpers.

/**
 * Invert all bits in a buffer.
 *
 * @param {Uint8Array} buf Non-empty buffer to invert.
 * @param {number} len Buffer length (must be positive).
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
  capitalize,
  copyOwnProperties,
  getHash: platform.getHash,
  compare,
  getOption,
  impliedNamespace,
  isBufferLike,
  isValidName,
  jsonEnd,
  objectValues,
  qualify,
  toMap,
  singleIndexOf,
  hasDuplicates,
  unqualify,
  Lcg,
  OrderedQueue,
  Tap,
  printJSON
};
