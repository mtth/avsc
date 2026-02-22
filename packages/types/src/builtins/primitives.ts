import {invalidValueError, RealType} from './common.js';
import {Type, NullType} from '../interfaces.js';

/**
 * Base primitive Avro type.
 *
 * Most of the primitive types share the same cloning and resolution
 * mechanisms, provided by this class. This class also lets us conveniently
 * check whether a type is a primitive using `instanceof`.
 */
abstract class RealPrimitiveType extends RealType {
  constructor() {
    super({});
    this._branchConstructor = this._createBranchConstructor();
    Object.freeze(this);
  }

  _update(resolver, type) {
    if (type.typeName === this.typeName) {
      resolver._read = this._read;
    }
  }

  _copy(val) {
    this._check(val, undefined, (v, t) => { throw invalidValueError(v, t); });
    return val;
  }

  _deref() {
    return this.typeName;
  }

  compare(a, b) {
    return compareNumbers(a, b);
  }
}

/** Nulls. */
class RealNullType extends RealPrimitiveType implements NullType {
  override readonly typeName = 'null';

  _check(val, flags, hook) {
    const b = val === null;
    if (!b && hook) {
      hook(val, this);
    }
    return b;
  }

  _read() {
    return null;
  }

  _skip() {}

  _write(tap, val) {
    if (val !== null) {
      throw invalidValueError(val, this);
    }
  }

  _match() {
    return 0;
  }

  compare = _match;
}

/** Booleans. */
class BooleanType extends RealPrimitiveType {
  static typeName = 'boolean';

  _check(val, flags, hook) {
    const b = typeof val == 'boolean';
    if (!b && hook) {
      hook(val, this);
    }
    return b;
  }

  _read(tap) {
    return tap.readBoolean();
  }

  _skip(tap) {
    tap.skipBoolean();
  }

  _write(tap, val) {
    if (typeof val != 'boolean') {
      throw invalidValueError(val, this);
    }
    tap.writeBoolean(val);
  }

  _match(tap1, tap2) {
    return tap1.matchBoolean(tap2);
  }
}

/** Integers. */
class IntType extends RealPrimitiveType {
  static typeName = 'int';

  _check(val, flags, hook) {
    const b = val === (val | 0);
    if (!b && hook) {
      hook(val, this);
    }
    return b;
  }

  _read(tap) {
    return tap.readLong();
  }

  _skip(tap) {
    tap.skipLong();
  }

  _write(tap, val) {
    if (val !== (val | 0)) {
      throw invalidValueError(val, this);
    }
    tap.writeLong(val);
  }

  _match(tap1, tap2) {
    return tap1.matchLong(tap2);
  }
}

/**
 * Longs.
 *
 * We can't capture all the range unfortunately since JavaScript represents all
 * numbers internally as `double`s, so the default implementation plays safe
 * and throws rather than potentially silently change the data. See `__with` or
 * `AbstractLongType` below for a way to implement a custom long type.
 */
class LongType extends RealPrimitiveType {
  typeName = 'long';

  // TODO: rework AbstractLongType so we don't need to accept noFreeze here
  constructor(noFreeze) {
    super(noFreeze);
  }

  _check(val, flags, hook) {
    const b = typeof val == 'number' && val % 1 === 0 && isSafeLong(val);
    if (!b && hook) {
      hook(val, this);
    }
    return b;
  }

  _read(tap) {
    const n = tap.readLong();
    if (!isSafeLong(n)) {
      throw new Error('potential precision loss');
    }
    return n;
  }

  _skip(tap) {
    tap.skipLong();
  }

  _write(tap, val) {
    if (typeof val != 'number' || val % 1 || !isSafeLong(val)) {
      throw invalidValueError(val, this);
    }
    tap.writeLong(val);
  }

  _match(tap1, tap2) {
    return tap1.matchLong(tap2);
  }

  _update(resolver, type) {
    switch (type.typeName) {
      case 'int':
        resolver._read = type._read;
        break;
      case 'abstract:long':
      case 'long':
        resolver._read = this._read; // In case `type` is an `AbstractLongType`.
    }
  }

  static __with(methods, noUnpack) {
    methods = methods || {}; // Will give a more helpful error message.
    // We map some of the methods to a different name to be able to intercept
    // their input and output (otherwise we wouldn't be able to perform any
    // unpacking logic, and the type wouldn't work when nested).
    const mapping = {
      toBuffer: '_toBuffer',
      fromBuffer: '_fromBuffer',
      fromJSON: '_fromJSON',
      toJSON: '_toJSON',
      isValid: '_isValid',
      compare: 'compare',
    };
    const type = new AbstractLongType(noUnpack);
    Object.keys(mapping).forEach((name) => {
      if (methods[name] === undefined) {
        throw new Error(`missing method implementation: ${name}`);
      }
      type[mapping[name]] = methods[name];
    });
    return Object.freeze(type);
  }
}

/** Floats. */
class FloatType extends RealPrimitiveType {
  static typeName = 'float';

  _check(val, flags, hook) {
    const b = typeof val == 'number';
    if (!b && hook) {
      hook(val, this);
    }
    return b;
  }

  _read(tap) {
    return tap.readFloat();
  }

  _skip(tap) {
    tap.skipFloat();
  }

  _write(tap, val) {
    if (typeof val != 'number') {
      throw invalidValueError(val, this);
    }
    tap.writeFloat(val);
  }

  _match(tap1, tap2) {
    return tap1.matchFloat(tap2);
  }

  _update(resolver, type) {
    switch (type.typeName) {
      case 'float':
      case 'int':
        resolver._read = type._read;
        break;
      case 'abstract:long':
      case 'long':
        // No need to worry about precision loss here since we're always
        // rounding to float anyway.
        resolver._read = function (tap) {
          return tap.readLong();
        };
    }
  }
}

/** Doubles. */
export class RealDoubleType extends RealPrimitiveType {
  readonly typeName = 'double';

  _check(val, flags, hook) {
    const b = typeof val == 'number';
    if (!b && hook) {
      hook(val, this);
    }
    return b;
  }

  _read(tap) {
    return tap.readDouble();
  }

  _skip(tap) {
    tap.skipDouble();
  }

  _write(tap, val) {
    if (typeof val != 'number') {
      throw invalidValueError(val, this);
    }
    tap.writeDouble(val);
  }

  _match(tap1, tap2) {
    return tap1.matchDouble(tap2);
  }

  _update(resolver, type) {
    switch (type.typeName) {
      case 'double':
      case 'float':
      case 'int':
        resolver._read = type._read;
        break;
      case 'abstract:long':
      case 'long':
        // Similar to inside `FloatType`, no need to worry about precision loss
        // here since we're always rounding to double anyway.
        resolver._read = function (tap) {
          return tap.readLong();
        };
    }
  }
}

/** Strings. */
class StringType extends RealPrimitiveType {
  static typeName = 'string';

  _check(val, flags, hook) {
    const b = typeof val == 'string';
    if (!b && hook) {
      hook(val, this);
    }
    return b;
  }

  _read(tap) {
    return tap.readString();
  }

  _skip(tap) {
    tap.skipString();
  }

  _write(tap, val) {
    if (typeof val != 'string') {
      throw invalidValueError(val, this);
    }
    tap.writeString(val);
  }

  _match(tap1, tap2) {
    return tap1.matchBytes(tap2);
  }

  _update(resolver, type) {
    switch (type.typeName) {
      case 'bytes':
      case 'string':
        resolver._read = this._read;
    }
  }
}

/**
 * Bytes.
 *
 * These are represented in memory as `Uint8Array`s rather than binary-encoded
 * strings. This is more efficient (when decoding/encoding from bytes, the
 * common use-case), idiomatic, and convenient.
 *
 * Note the coercion in `_copy`.
 */
class BytesType extends RealPrimitiveType {
  static typeName = 'bytes';

  _check(val, flags, hook) {
    const b = isBufferLike(val);
    if (!b && hook) {
      hook(val, this);
    }
    return b;
  }

  _read(tap) {
    return tap.readBytes();
  }

  _skip(tap) {
    tap.skipBytes();
  }

  _write(tap, val) {
    if (!isBufferLike(val)) {
      throw invalidValueError(val, this);
    }
    tap.writeBytes(val);
  }

  _match(tap1, tap2) {
    return tap1.matchBytes(tap2);
  }

  _update(resolver, type) {
    switch (type.typeName) {
      case 'bytes':
      case 'string':
        resolver._read = this._read;
    }
  }

  _copy(obj, opts) {
    let buf;
    switch ((opts && opts.coerce) | 0) {
      case 3: // Coerce buffers to strings.
        this._check(obj, undefined, (v, t) => { throw invalidValueError(v, t); });
        return utils.bufferToBinaryString(obj);
      case 2: // Coerce strings to buffers.
        if (typeof obj != 'string') {
          throw new Error(`cannot coerce to buffer: ${j(obj)}`);
        }
        buf = utils.binaryStringToBuffer(obj);
        this._check(buf, undefined, (v, t) => { throw invalidValueError(v, t); });
        return buf;
      case 1: // Coerce buffer JSON representation to buffers.
        if (!isJsonBuffer(obj)) {
          throw new Error(`cannot coerce to buffer: ${j(obj)}`);
        }
        buf = new Uint8Array(obj.data);
        this._check(buf, undefined, (v, t) => { throw invalidValueError(v, t); });
        return buf;
      default: // Copy buffer.
        this._check(obj, undefined, (v, t) => { throw invalidValueError(v, t); });
        return new Uint8Array(obj);
    }
  }
}

BytesType.prototype.compare = utils.bufCompare;

/**
 * Avro enum type.
 *
 * Represented as strings (with allowed values from the set of symbols). Using
 * integers would be a reasonable option, but the performance boost is arguably
 * offset by the legibility cost and the extra deviation from the JSON encoding
 * convention.
 *
 * An integer representation can still be used (e.g. for compatibility with
 * TypeScript `enum`s) by overriding the `EnumType` with a `LongType` (e.g. via
 * `parse`'s registry).
 */
class EnumType extends Type {
  constructor(schema, opts) {
    super(schema, opts);
    if (!Array.isArray(schema.symbols) || !schema.symbols.length) {
      throw new Error(`invalid enum symbols: ${j(schema.symbols)}`);
    }
    this.symbols = Object.freeze(schema.symbols.slice());
    this._indices = {};
    this.symbols.forEach(function (symbol, i) {
      if (!utils.isValidName(symbol)) {
        throw new Error(`invalid ${this} symbol: ${j(symbol)}`);
      }
      if (this._indices[symbol] !== undefined) {
        throw new Error(`duplicate ${this} symbol: ${j(symbol)}`);
      }
      this._indices[symbol] = i;
    }, this);
    this.default = schema.default;
    if (
      this.default !== undefined &&
      this._indices[this.default] === undefined
    ) {
      throw new Error(`invalid ${this} default: ${j(this.default)}`);
    }
    this._branchConstructor = this._createBranchConstructor();
    Object.freeze(this);
  }

  _check(val, flags, hook) {
    const b = this._indices[val] !== undefined;
    if (!b && hook) {
      hook(val, this);
    }
    return b;
  }

  _read(tap) {
    const index = tap.readLong();
    const symbol = this.symbols[index];
    if (symbol === undefined) {
      throw new Error(`invalid ${this.name} enum index: ${index}`);
    }
    return symbol;
  }

  _skip(tap) {
    tap.skipLong();
  }

  _write(tap, val) {
    const index = this._indices[val];
    if (index === undefined) {
      throw invalidValueError(val, this);
    }
    tap.writeLong(index);
  }

  _match(tap1, tap2) {
    return tap1.matchLong(tap2);
  }

  compare(val1, val2) {
    return utils.compare(this._indices[val1], this._indices[val2]);
  }

  _update(resolver, type, opts) {
    const symbols = this.symbols;
    if (
      type.typeName === 'enum' &&
      hasCompatibleName(this, type, !opts.ignoreNamespaces) &&
      (type.symbols.every((s) => {
        return ~symbols.indexOf(s);
      }) ||
        this.default !== undefined)
    ) {
      resolver.symbols = type.symbols.map(function (s) {
        return this._indices[s] === undefined ? this.default : s;
      }, this);
      resolver._read = type._read;
    }
  }

  _copy(val) {
    this._check(val, undefined, throw invalidValueError);
    return val;
  }

  _deref(schema) {
    schema.symbols = this.symbols;
  }

  getSymbols() {
    return this.symbols;
  }
}

EnumType.prototype.typeName = 'enum';

/** Avro fixed type. Represented simply as a `Uint8Array`. */
class FixedType extends Type {
  constructor(schema, opts) {
    super(schema, opts);
    if (schema.size !== (schema.size | 0) || schema.size < 0) {
      throw new Error(`invalid ${this.branchName} size`);
    }
    this.size = schema.size | 0;
    this._branchConstructor = this._createBranchConstructor();
    Object.freeze(this);
  }

  _check(val, flags, hook) {
    const b = isBufferLike(val) && val.length === this.size;
    if (!b && hook) {
      hook(val, this);
    }
    return b;
  }

  _read(tap) {
    return tap.readFixed(this.size);
  }

  _skip(tap) {
    tap.skipFixed(this.size);
  }

  _write(tap, val) {
    if (!isBufferLike(val) || val.length !== this.size) {
      throw invalidValueError(val, this);
    }
    tap.writeFixed(val, this.size);
  }

  _match(tap1, tap2) {
    return tap1.matchFixed(tap2, this.size);
  }

  _update(resolver, type, opts) {
    if (
      type.typeName === 'fixed' &&
      this.size === type.size &&
      hasCompatibleName(this, type, !opts.ignoreNamespaces)
    ) {
      resolver.size = this.size;
      resolver._read = this._read;
    }
  }

  _deref(schema) {
    schema.size = this.size;
  }

  getSize() {
    return this.size;
  }
}

FixedType.prototype._copy = BytesType.prototype._copy;

FixedType.prototype.compare = utils.bufCompare;

FixedType.prototype.typeName = 'fixed';

/**
 * Check whether a long can be represented without precision loss. Two things to
 * note:
 *
 * + We are not using the `Number` constants for compatibility with older
 *   browsers.
 * + We divide the bounds by two to avoid rounding errors during zigzag encoding
 *   (see https://github.com/mtth/avsc/issues/455).
 */
function isSafeLong(n: number): boolean {
  return n >= -4503599627370496 && n <= 4503599627370496;
}

/**
 * Check whether an object is the JSON representation of a buffer.
 */
function isJsonBuffer(obj: unknown): boolean {
  // TODO: Update.
}
