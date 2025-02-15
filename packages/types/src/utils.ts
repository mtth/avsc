/** Various utilities used across this library. */

// Valid (field, type, and symbol) name regex.
const NAME_PATTERN = /^[A-Za-z_][A-Za-z0-9_]*$/;

function isBufferLike(data: unknown): data is Uint8Array {
  return data instanceof Uint8Array;
}

/**
 * Uppercase the first letter of a string.
 *
 * @param s {String} The string.
 */
function capitalize(s: string): string {
  return s.charAt(0).toUpperCase() + s.slice(1);
}

/**
 * Compare two numbers.
 *
 * @param n1 {Number} The first one.
 * @param n2 {Number} The second one.
 */
function compare(n1: number, n2: number): number {
  return n1 === n2 ? 0 : n1 < n2 ? -1 : 1;
}

let bufCompare: (b1: Uint8Array, b2: Uint8Array) => number,
  bufEqual: (b1: Uint8Array, b2: Uint8Array) => boolean;
if (typeof Buffer == 'function') {
  bufCompare = Buffer.compare;
  bufEqual = function (buf1, buf2) {
    return Buffer.prototype.equals.call(buf1, buf2);
  };
} else {
  bufCompare = function (buf1, buf2) {
    if (buf1 === buf2) {
      return 0;
    }
    const len = Math.min(buf1.length, buf2.length);
    for (let i = 0; i < len; i++) {
      if (buf1[i] !== buf2[i]) {
        return Math.sign(buf1[i]! - buf2[i]!);
      }
    }
    return Math.sign(buf1.length - buf2.length);
  };
  bufEqual = function (buf1, buf2) {
    if (buf1.length !== buf2.length) {
      return false;
    }
    return bufCompare(buf1, buf2) === 0;
  };
}

/** Check whether an array has duplicates. */
function hasDuplicates<V, K>(
  arr: ReadonlyArray<V>,
  fn: (val: V) => K
): boolean {
  const keys = new Set(arr.map(fn));
  return keys.size === arr.length;
}

/**
 * Copy properties from one object to another.
 *
 * @param src {Object} The source object.
 * @param dst {Object} The destination object.
 * @param overwrite {Boolean} Whether to overwrite existing destination
 * properties. Defaults to false.
 */
function copyOwnProperties<O>(src: object, dst: O, overwrite?: boolean): O {
  const names = Object.getOwnPropertyNames(src);
  for (let i = 0, l = names.length; i < l; i++) {
    const name = names[i]!;
    if (!Object.prototype.hasOwnProperty.call(dst, name) || overwrite) {
      const descriptor = Object.getOwnPropertyDescriptor(src, name)!;
      Object.defineProperty(dst, name, descriptor);
    }
  }
  return dst;
}

/**
 * Check whether a string is a valid Avro identifier.
 */
function isValidName(str: string): boolean {
  return NAME_PATTERN.test(str);
}

/**
 * Verify and return fully qualified name. The input name is a full or short
 * name. It can be prefixed with a dot to force global namespace. The namespace
 * is optional.
 */
function qualify(name: string, namespace?: string): string {
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

/** Remove namespace from a (full or short) name. */
function unqualify(name: string): string {
  const parts = name.split('.');
  return parts[parts.length - 1]!;
}

/**
 * Return the namespace implied by a (full or short) name. If short, the
 * returned namespace will be undefined.
 */
function impliedNamespace(name: string): string | undefined {
  const match = /^(.*)\.[^.]+$/.exec(name);
  return match ? match[1] : undefined;
}

let decodeSlice: (arr: Uint8Array, start?: number, end?: number) => string;
if (
  typeof Buffer === 'function' &&
  typeof Buffer.prototype.utf8Slice === 'function'
) {
  // Note that calling `Buffer.prototype.toString.call(buf, 'utf-8')` on a
  // `Uint8Array` throws because Node's internal implementation expects the
  // argument to be a `Buffer` specifically.
  decodeSlice = Function.prototype.call.bind(Buffer.prototype.utf8Slice);
} else {
  const DECODER = new TextDecoder();

  decodeSlice = function (arr, start, end) {
    return DECODER.decode(arr.subarray(start, end));
  };
}

const ENCODER = new TextEncoder();
const encodeBuf = new Uint8Array(4096);
const encodeBufs: Uint8Array[] = [];

function encodeSlice(str: string): Uint8Array {
  const {read, written} = ENCODER.encodeInto(str, encodeBuf);
  if (read === str.length) {
    // Believe it or not, `subarray` is actually quite expensive. To avoid the
    // cost, we cache and reuse `subarray`s.
    if (!encodeBufs[written]) {
      encodeBufs[written] = encodeBuf.subarray(0, written);
    }
    return encodeBufs[written];
  }

  return ENCODER.encode(str);
}

let utf8Length: (str: string) => number;
if (typeof Buffer === 'function') {
  utf8Length = Buffer.byteLength;
} else {
  utf8Length = function (str) {
    let len = 0;
    for (;;) {
      // encodeInto is faster than any manual implementation (or even
      // Buffer.byteLength), provided the string fits entirely within the
      // buffer. Past that, it slows down but is still faster than other
      // options.
      const {read, written} = ENCODER.encodeInto(str, encodeBuf);
      len += written;
      if (read === str.length) {
        break;
      }
      str = str.slice(read);
    }
    return len;
  };
}

let bufferToBinaryString: (arr: Uint8Array) => string;
if (
  typeof Buffer === 'function' &&
  typeof Buffer.prototype.latin1Slice === 'function'
) {
  // Note that calling `Buffer.prototype.toString.call(buf, 'binary')` on a
  // `Uint8Array` throws because Node's internal implementation expects the
  // argument to be a `Buffer` specifically.
  bufferToBinaryString = Function.prototype.call.bind(
    Buffer.prototype.latin1Slice
  );
} else {
  bufferToBinaryString = function (buf) {
    let str = '';
    let i = 0,
      len = buf.length;
    for (; i + 7 < len; i += 8) {
      str += String.fromCharCode(
        buf[i]!,
        buf[i + 1]!,
        buf[i + 2]!,
        buf[i + 3]!,
        buf[i + 4]!,
        buf[i + 5]!,
        buf[i + 6]!,
        buf[i + 7]!
      );
    }
    for (; i < len; i++) {
      str += String.fromCharCode(buf[i]!);
    }
    return str;
  };
}

let binaryStringToBuffer: (str: string) => Uint8Array;
if (typeof Buffer === 'function') {
  binaryStringToBuffer = function (str) {
    const buf = Buffer.from(str, 'binary');
    return new Uint8Array(buf.buffer, buf.byteOffset, buf.length);
  };
} else {
  binaryStringToBuffer = function (str) {
    const buf = new Uint8Array(str.length);
    for (let i = 0; i < str.length; i++) {
      buf[i] = str.charCodeAt(i);
    }
    return Buffer.from(buf);
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
  // @ts-expect-error initialized below
  private arr: Uint8Array;
  // @ts-expect-error initialized below
  private pos: number;
  constructor(buf: Uint8Array, pos?: number) {
    this.setData(buf, pos);
  }

  setData(buf: Uint8Array, pos?: number): void {
    if (typeof Buffer === 'function' && buf instanceof Buffer) {
      buf = new Uint8Array(buf.buffer, buf.byteOffset, buf.length);
    }
    this.arr = buf;
    this.pos = pos ?? 0;
    if (this.pos < 0) {
      throw new Error('negative offset');
    }
  }

  get length(): number {
    return this.arr.length;
  }

  reinitialize(capacity: number): void {
    this.setData(new Uint8Array(capacity));
  }

  static fromBuffer(buf: Uint8Array, pos?: number): Tap {
    return new Tap(buf, pos);
  }

  static withCapacity(capacity: number): Tap {
    const buf = new Uint8Array(capacity);
    return new Tap(buf);
  }

  toBuffer(): Uint8Array {
    return this.arr.slice(0, this.pos);
  }

  subarray(start: number, end?: number): Uint8Array {
    return this.arr.subarray(start, end);
  }

  append(newBuf: Uint8Array): void {
    const newArr = new Uint8Array(this.arr.length + newBuf.length);
    newArr.set(this.arr, 0);
    newArr.set(newBuf, this.arr.length);
    this.setData(newArr, 0);
  }

  forward(newBuf: Uint8Array): void {
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
  isValid(): boolean {
    return this.pos <= this.arr.length;
  }

  _invalidate(): void {
    this.pos = this.arr.length + 1;
  }

  // Read, skip, write methods.
  //
  // These should fail silently when the buffer overflows. Note this is only
  // required to be true when the functions are decoding valid objects. For
  // example errors will still be thrown if a bad count is read, leading to a
  // negative position offset (which will typically cause a failure in
  // `readFixed`).

  readBoolean(): boolean {
    return !!this.arr[this.pos++];
  }

  skipBoolean(): void {
    this.pos++;
  }

  writeBoolean(b: boolean): void {
    this.arr[this.pos++] = +b;
  }

  readLong(): number {
    let n = 0;
    let k = 0;
    const buf = this.arr;
    let b, h, f, fk;

    do {
      b = buf[this.pos++]!;
      h = b & 0x80;
      n |= (b & 0x7f) << k;
      k += 7;
    } while (h && k < 28);

    if (h) {
      // Switch to float arithmetic, otherwise we might overflow.
      f = n;
      fk = 268435456; // 2 ** 28.
      do {
        b = buf[this.pos++]!;
        f += (b & 0x7f) * fk;
        fk *= 128;
      } while (b & 0x80);
      return (f % 2 ? -(f + 1) : f) / 2;
    }

    return (n >> 1) ^ -(n & 1);
  }

  skipLong(): void {
    const buf = this.arr;
    while (buf[this.pos++]! & 0x80) {}
  }

  writeLong(n: number): void {
    const buf = this.arr;
    let f, m;

    if (n >= -1073741824 && n < 1073741824) {
      // Won't overflow, we can use integer arithmetic.
      m = n >= 0 ? n << 1 : (~n << 1) | 1;
      do {
        buf[this.pos] = m & 0x7f;
        m >>= 7;
      } while (m && (buf[this.pos++]! |= 0x80));
    } else {
      // We have to use slower floating arithmetic.
      f = n >= 0 ? n * 2 : -n * 2 - 1;
      do {
        buf[this.pos] = f & 0x7f;
        f /= 128;
      } while (f >= 1 && (buf[this.pos++]! |= 0x80));
    }
    this.pos++;
  }

  readFloat(): number {
    const pos = this.pos;
    this.pos += 4;
    if (this.pos > this.arr.length) {
      return 0;
    }
    FLOAT_VIEW.setUint32(
      0,
      this.arr[pos]! |
        (this.arr[pos + 1]! << 8) |
        (this.arr[pos + 2]! << 16) |
        (this.arr[pos + 3]! << 24),
      true
    );
    return FLOAT_VIEW.getFloat32(0, true);
  }

  skipFloat(): void {
    this.pos += 4;
  }

  writeFloat(f: number): void {
    const pos = this.pos;
    this.pos += 4;
    if (this.pos > this.arr.length) {
      return;
    }

    FLOAT_VIEW.setFloat32(0, f, true);
    const n = FLOAT_VIEW.getUint32(0, true);
    this.arr[pos]! = n & 0xff;
    this.arr[pos + 1]! = (n >> 8) & 0xff;
    this.arr[pos + 2]! = (n >> 16) & 0xff;
    this.arr[pos + 3]! = n >> 24;
  }

  readDouble(): number {
    const pos = this.pos;
    this.pos += 8;
    if (this.pos > this.arr.length) {
      return 0;
    }
    FLOAT_VIEW.setUint32(
      0,
      this.arr[pos]! |
        (this.arr[pos + 1]! << 8) |
        (this.arr[pos + 2]! << 16) |
        (this.arr[pos + 3]! << 24),
      true
    );
    FLOAT_VIEW.setUint32(
      4,
      this.arr[pos + 4]! |
        (this.arr[pos + 5]! << 8) |
        (this.arr[pos + 6]! << 16) |
        (this.arr[pos + 7]! << 24),
      true
    );
    return FLOAT_VIEW.getFloat64(0, true);
  }

  skipDouble(): void {
    this.pos += 8;
  }

  writeDouble(d: number): void {
    const pos = this.pos;
    this.pos += 8;
    if (this.pos > this.arr.length) {
      return;
    }
    FLOAT_VIEW.setFloat64(0, d, true);
    const a = FLOAT_VIEW.getUint32(0, true);
    const b = FLOAT_VIEW.getUint32(4, true);
    this.arr[pos]! = a & 0xff;
    this.arr[pos + 1]! = (a >> 8) & 0xff;
    this.arr[pos + 2]! = (a >> 16) & 0xff;
    this.arr[pos + 3]! = a >> 24;
    this.arr[pos + 4]! = b & 0xff;
    this.arr[pos + 5]! = (b >> 8) & 0xff;
    this.arr[pos + 6]! = (b >> 16) & 0xff;
    this.arr[pos + 7]! = b >> 24;
  }

  readFixed(len: number): Uint8Array {
    const pos = this.pos;
    this.pos += len;
    if (this.pos > this.arr.length) {
      return new Uint8Array();
    }
    return this.arr.slice(pos, pos + len);
  }

  skipFixed(len: number): void {
    this.pos += len;
  }

  writeFixed(buf: Uint8Array, len?: number): void {
    len = len ?? buf.length;
    const pos = this.pos;
    this.pos += len;
    if (this.pos > this.arr.length) {
      return;
    }
    this.arr.set(buf.subarray(0, len), pos);
  }

  readBytes(): Uint8Array {
    const len = this.readLong();
    if (len < 0) {
      this._invalidate();
      return new Uint8Array();
    }
    return this.readFixed(len);
  }

  skipBytes(): void {
    const len = this.readLong();
    if (len < 0) {
      this._invalidate();
      return;
    }
    this.pos += len;
  }

  writeBytes(buf: Uint8Array): void {
    const len = buf.length;
    this.writeLong(len);
    this.writeFixed(buf, len);
  }

  skipString(): void {
    const len = this.readLong();
    if (len < 0) {
      this._invalidate();
      return;
    }
    this.pos += len;
  }

  readString(): string {
    const len = this.readLong();
    if (len < 0) {
      this._invalidate();
      return '';
    }
    let pos = this.pos;
    this.pos += len;
    if (this.pos > this.arr.length) {
      return '';
    }

    const arr = this.arr;
    const end = pos + len;
    if (len > 24) {
      return decodeSlice(arr, pos, end);
    }

    let output = '';
    // Consume the string in 4-byte chunks. The performance benefit comes not
    // from *reading* in chunks, but calling fromCharCode with 4 characters per
    // call.
    while (pos + 3 < end) {
      const a = arr[pos]!,
        b = arr[pos + 1]!,
        c = arr[pos + 2]!,
        d = arr[pos + 3]!;
      // If the high bit of any character is set, it's a non-ASCII character.
      // Fall back to TextDecoder for the remaining characters.
      if ((a | b | c | d) & 0x80) {
        output += decodeSlice(arr, pos, end);
        return output;
      }
      output += String.fromCharCode(a, b, c, d);
      pos += 4;
    }

    // Handle the remainder of the string.
    while (pos < end) {
      const char = arr[pos]!;
      if (char & 0x80) {
        output += decodeSlice(arr, pos, end);
        return output;
      }
      output += String.fromCharCode(char);
      pos++;
    }

    return output;
  }

  writeString(s: string): void {
    const buf = this.arr;
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
      const pos = this.pos;
      this.pos += encodedLength;

      if (this.isValid() && typeof encoded != 'undefined') {
        buf.set(encoded, pos);
      }
    } else {
      // For small strings, this manual implementation is faster.

      // Set aside 1 byte to write the string length.
      let pos = this.pos + 1;
      const startPos = pos;
      const bufLen = buf.length;

      // This is not a micro-optimization: caching the string length for the
      // loop predicate really does make a difference!
      for (let i = 0; i < stringLen; i++) {
        let c1 = s.charCodeAt(i);
        let c2;
        if (c1 < 0x80) {
          if (pos < bufLen) {
            buf[pos] = c1;
          }
          pos++;
        } else if (c1 < 0x800) {
          if (pos + 1 < bufLen) {
            buf[pos] = (c1 >> 6) | 0xc0;
            buf[pos + 1] = (c1 & 0x3f) | 0x80;
          }
          pos += 2;
        } else if (
          (c1 & 0xfc00) === 0xd800 &&
          ((c2 = s.charCodeAt(i + 1)) & 0xfc00) === 0xdc00
        ) {
          c1 = 0x10000 + ((c1 & 0x03ff) << 10) + (c2 & 0x03ff);
          i++;
          if (pos + 3 < bufLen) {
            buf[pos] = (c1 >> 18) | 0xf0;
            buf[pos + 1] = ((c1 >> 12) & 0x3f) | 0x80;
            buf[pos + 2] = ((c1 >> 6) & 0x3f) | 0x80;
            buf[pos + 3] = (c1 & 0x3f) | 0x80;
          }
          pos += 4;
        } else {
          if (pos + 2 < bufLen) {
            buf[pos] = (c1 >> 12) | 0xe0;
            buf[pos + 1] = ((c1 >> 6) & 0x3f) | 0x80;
            buf[pos + 2] = (c1 & 0x3f) | 0x80;
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

  matchBoolean(tap: Tap): number {
    return this.arr[this.pos++]! - tap.arr[tap.pos++]!;
  }

  matchLong(tap: Tap): number {
    const n1 = this.readLong();
    const n2 = tap.readLong();
    return n1 === n2 ? 0 : n1 < n2 ? -1 : 1;
  }

  matchFloat(tap: Tap): number {
    const n1 = this.readFloat();
    const n2 = tap.readFloat();
    return n1 === n2 ? 0 : n1 < n2 ? -1 : 1;
  }

  matchDouble(tap: Tap): number {
    const n1 = this.readDouble();
    const n2 = tap.readDouble();
    return n1 === n2 ? 0 : n1 < n2 ? -1 : 1;
  }

  matchFixed(tap: Tap, len: number): number {
    return bufCompare(this.readFixed(len), tap.readFixed(len));
  }

  matchBytes(tap: Tap): number {
    const l1 = this.readLong();
    const p1 = this.pos;
    this.pos += l1;
    const l2 = tap.readLong();
    const p2 = tap.pos;
    tap.pos += l2;
    const b1 = this.arr.subarray(p1, this.pos);
    const b2 = tap.arr.subarray(p2, tap.pos);
    return bufCompare(b1, b2);
  }

  // Functions for supporting custom long classes.
  //
  // The two following methods allow the long implementations to not have to
  // worry about Avro's zigzag encoding, we directly expose longs as unpacked.

  unpackLongBytes(): Uint8Array {
    const res = new Uint8Array(8);
    let n = 0;
    let i = 0; // Byte index in target buffer.
    let j = 6; // Bit offset in current target buffer byte.
    const buf = this.arr;

    let b = buf[this.pos++]!;
    const neg = b & 1;
    res.fill(0);

    n |= (b & 0x7f) >> 1;
    while (b & 0x80) {
      b = buf[this.pos++]!;
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

  packLongBytes(buf: Uint8Array): void {
    const neg = (buf[7]! & 0x80) >> 7;
    const res = this.arr;
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

    const parts = [
      buf[0]! | (buf[1]! << 8) | (buf[2]! << 16),
      buf[3]! | (buf[4]! << 8) | (buf[5]! << 16),
      buf[6]! | (buf[7]! << 8),
    ];
    // Not reading more than 24 bits because we need to be able to combine the
    // "carry" bits from the previous part and JavaScript only supports bitwise
    // operations on 32 bit integers.
    while (m && !parts[--m]) {} // Skip trailing 0s.

    // Leading parts (if any), we never bail early here since we need the
    // continuation bit to be set.
    while (k < m) {
      n |= parts[k++]! << j;
      j += 24;
      while (j > 7) {
        res[this.pos++]! = (n & 0x7f) | 0x80;
        n >>= 7;
        j -= 7;
      }
    }

    // Final part, similar to normal packing aside from the initial offset.
    n |= parts[m]! << j;
    do {
      res[this.pos]! = n & 0x7f;
      n >>= 7;
    } while (n && (res[this.pos++]! |= 0x80));
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
function invert(buf: Uint8Array, len: number): void {
  while (len--) {
    buf[len] = ~buf[len]!;
  }
}

/**
 * Prints an object as a string; mostly used for printing objects in errors.
 * @param {object} obj The object to display.
 * @returns The object as JSON.
 */
function printJSON(obj: any): string {
  const seen = new Set();
  try {
    return JSON.stringify(obj, (_key, value) => {
      if (seen.has(value)) {
        return '[Circular]';
      }
      if (typeof value === 'object' && value !== null) {
        seen.add(value);
      }

      if (typeof BigInt !== 'undefined' && value instanceof BigInt) {
        return `[BigInt ${value.toString()}n]`;
      }
      return value;
    });
  } catch (err) {
    return '[object]';
  }
}
