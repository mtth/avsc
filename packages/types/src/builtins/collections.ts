import {RealType} from './common.js';
import {BaseType, MapType, ArrayType, Type} from '../interfaces.js';

/** Avro map. Represented as vanilla objects. */
class RealMapType extends RealType implements MapType {
  override readonly typeName = 'map';

  constructor(schema, opts) {
    super();
    if (!schema.values) {
      throw new Error(`missing map values: ${j(schema)}`);
    }
    this.valuesType = Type.forSchema(schema.values, opts);
    this._branchConstructor = this._createBranchConstructor();
    Object.freeze(this);
  }

  _check(val, flags, hook, path) {
    if (!val || typeof val != 'object' || Array.isArray(val)) {
      if (hook) {
        hook(val, this);
      }
      return false;
    }

    const keys = Object.keys(val);
    let b = true;
    if (hook) {
      // Slow path.
      const j = path.length;
      path.push('');
      for (let i = 0, l = keys.length; i < l; i++) {
        const key = (path[j] = keys[i]);
        if (!this.valuesType._check(val[key], flags, hook, path)) {
          b = false;
        }
      }
      path.pop();
    } else {
      for (let i = 0, l = keys.length; i < l; i++) {
        if (!this.valuesType._check(val[keys[i]], flags)) {
          return false;
        }
      }
    }
    return b;
  }

  _read(tap) {
    const values = this.valuesType;
    const val = {};
    let n;
    while ((n = readArraySize(tap))) {
      while (n--) {
        const key = tap.readString();
        val[key] = values._read(tap);
      }
    }
    return val;
  }

  _skip(tap) {
    const values = this.valuesType;
    let n;
    while ((n = tap.readLong())) {
      if (n < 0) {
        const len = tap.readLong();
        tap.pos += len;
      } else {
        while (n--) {
          tap.skipString();
          values._skip(tap);
        }
      }
    }
  }

  _write(tap, val) {
    if (!val || typeof val != 'object' || Array.isArray(val)) {
      throwInvalidError(val, this);
    }

    const values = this.valuesType;
    const keys = Object.keys(val);
    const n = keys.length;
    if (n) {
      tap.writeLong(n);
      for (let i = 0; i < n; i++) {
        const key = keys[i];
        tap.writeString(key);
        values._write(tap, val[key]);
      }
    }
    tap.writeLong(0);
  }

  override compare(): never {
    throw new Error('maps cannot be compared');
  }

  override _match() {
    return this.compare();
  }

  _update(rsv, type, opts) {
    if (type.typeName === 'map') {
      rsv.valuesType = this.valuesType.createResolver(type.valuesType, opts);
      rsv._read = this._read;
    }
  }

  _copy(val, opts) {
    if (val && typeof val == 'object' && !Array.isArray(val)) {
      const values = this.valuesType;
      const keys = Object.keys(val);
      const copy = {};
      for (let i = 0, l = keys.length; i < l; i++) {
        const key = keys[i];
        copy[key] = values._copy(val[key], opts);
      }
      return copy;
    }
    throwInvalidError(val, this);
  }

  valuesType(): Type {
    return this.valuesType;
  }

  _deref(schema, derefed, opts) {
    schema.values = this.valuesType._attrs(derefed, opts);
  }
}

/** Avro array. Represented as vanilla arrays. */
class RealArrayType extends RealType implements ArrayType {
  override readonly typeName = 'array';

  constructor(schema, opts) {
    super();
    if (!schema.items) {
      throw new Error(`missing array items: ${j(schema)}`);
    }
    this.itemsType = Type.forSchema(schema.items, opts);
    this._branchConstructor = this._createBranchConstructor();
    Object.freeze(this);
  }

  _check(val, flags, hook, path) {
    if (!Array.isArray(val)) {
      if (hook) {
        hook(val, this);
      }
      return false;
    }
    const items = this.itemsType;
    let b = true;
    if (hook) {
      // Slow path.
      const j = path.length;
      path.push('');
      for (let i = 0, l = val.length; i < l; i++) {
        path[j] = '' + i;
        if (!items._check(val[i], flags, hook, path)) {
          b = false;
        }
      }
      path.pop();
    } else {
      for (let i = 0, l = val.length; i < l; i++) {
        if (!items._check(val[i], flags)) {
          return false;
        }
      }
    }
    return b;
  }

  _read(tap) {
    const items = this.itemsType;
    let i = 0;
    let val, n;
    while ((n = tap.readLong())) {
      if (n < 0) {
        n = -n;
        tap.skipLong(); // Skip size.
      }
      // Initializing the array on the first batch gives a ~10% speedup. See
      // https://github.com/mtth/avsc/pull/338 for more context.
      val = val || new Array(n);
      while (n--) {
        val[i++] = items._read(tap);
      }
    }
    return val || [];
  }

  _skip(tap) {
    const items = this.itemsType;
    let n;
    while ((n = tap.readLong())) {
      if (n < 0) {
        const len = tap.readLong();
        tap.pos += len;
      } else {
        while (n--) {
          items._skip(tap);
        }
      }
    }
  }

  _write(tap, val) {
    if (!Array.isArray(val)) {
      throwInvalidError(val, this);
    }
    const items = this.itemsType;
    const n = val.length;
    if (n) {
      tap.writeLong(n);
      for (let i = 0; i < n; i++) {
        items._write(tap, val[i]);
      }
    }
    tap.writeLong(0);
  }

  _match(tap1, tap2) {
    let n1 = tap1.readLong();
    let n2 = tap2.readLong();
    while (n1 && n2) {
      const f = this.itemsType._match(tap1, tap2);
      if (f) {
        return f;
      }
      if (!--n1) {
        n1 = readArraySize(tap1);
      }
      if (!--n2) {
        n2 = readArraySize(tap2);
      }
    }
    return utils.compare(n1, n2);
  }

  _update(resolver, type, opts) {
    if (type.typeName === 'array') {
      resolver.itemsType = this.itemsType.createResolver(type.itemsType, opts);
      resolver._read = this._read;
    }
  }

  _copy(val, opts) {
    if (!Array.isArray(val)) {
      throwInvalidError(val, this);
    }
    const items = new Array(val.length);
    for (let i = 0, l = val.length; i < l; i++) {
      items[i] = this.itemsType._copy(val[i], opts);
    }
    return items;
  }

  _deref(schema, derefed, opts) {
    schema.items = this.itemsType._attrs(derefed, opts);
  }

  compare(val1, val2) {
    const n1 = val1.length;
    const n2 = val2.length;
    let f;
    for (let i = 0, l = Math.min(n1, n2); i < l; i++) {
      if ((f = this.itemsType.compare(val1[i], val2[i]))) {
        return f;
      }
    }
    return utils.compare(n1, n2);
  }

  getItemsType() {
    return this.itemsType;
  }
}
