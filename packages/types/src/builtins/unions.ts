import {isBufferLike} from '../binary.js';
import {UnionType, Type} from '../interfaces.js';
import {RealType} from './common.js';

/** Base "abstract" Avro union type. */
class RealUnionType extends RealType implements UnionType {
  constructor(schema, opts) {
    super();

    if (!Array.isArray(schema)) {
      throw new Error(`non-array union schema: ${j(schema)}`);
    }
    if (!schema.length) {
      throw new Error('empty union');
    }
    this.types = Object.freeze(
      schema.map((obj) => {
        return Type.forSchema(obj, opts);
      })
    );

    this._branchIndices = {};
    this.types.forEach(function (type, i) {
      if (Type.isType(type, 'union')) {
        throw new Error('unions cannot be directly nested');
      }
      const branch = type.branchName;
      if (this._branchIndices[branch] !== undefined) {
        throw new Error(`duplicate union branch name: ${j(branch)}`);
      }
      this._branchIndices[branch] = i;
    }, this);
  }

  _skip(tap) {
    this.types[tap.readLong()]._skip(tap);
  }

  _match(tap1, tap2) {
    const n1 = tap1.readLong();
    const n2 = tap2.readLong();
    if (n1 === n2) {
      return this.types[n1]._match(tap1, tap2);
    }
    return n1 < n2 ? -1 : 1;
  }

  _deref(schema, derefed, opts) {
    return this.types.map((t) => {
      return t._attrs(derefed, opts);
    });
  }

  getTypes() {
    return this.types;
  }
}

// Cannot be defined as a class method because it's used as a constructor.
// https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Functions/Method_definitions#method_definitions_are_not_constructable
UnionType.prototype._branchConstructor = function () {
  throw new Error('unions cannot be directly wrapped');
};

function generateProjectionIndexer(projectionFn) {
  return (val) => {
    const index = projectionFn(val);
    if (typeof index !== 'number') {
      throw new Error(`Projected index '${index}' is not valid`);
    }
    return index;
  };
}

function generateDefaultIndexer(types, self) {
  const dynamicBranches = [];
  const bucketIndices = {};

  const getBranchIndex = (any, index) => {
    const logicalBranches = dynamicBranches;
    for (let i = 0, l = logicalBranches.length; i < l; i++) {
      const branch = logicalBranches[i];
      if (branch.type._check(any)) {
        if (index === undefined) {
          index = branch.index;
        } else {
          // More than one branch matches the value so we aren't guaranteed to
          // infer the correct type. We throw rather than corrupt data. This can
          // be fixed by "tightening" the logical types.
          throw new Error('ambiguous conversion');
        }
      }
    }
    return index;
  };

  types.forEach(function (type, index) {
    if (Type.isType(type, 'abstract', 'logical')) {
      dynamicBranches.push({index, type});
    } else {
      const bucket = getTypeBucket(type);
      if (bucketIndices[bucket] !== undefined) {
        throw new Error(`ambiguous unwrapped union: ${j(self)}`);
      }
      bucketIndices[bucket] = index;
    }
  });
  return (val) => {
    let index = bucketIndices[getValueBucket(val)];
    if (dynamicBranches.length) {
      // Slower path, we must run the value through all branches.
      index = getBranchIndex(val, index);
    }
    return index;
  };
}

/**
 * "Natural" union type.
 *
 * This representation doesn't require a wrapping object and is therefore
 * simpler and generally closer to what users expect. However it cannot be used
 * to represent all Avro unions since some lead to ambiguities (e.g. if two
 * number types are in the union).
 *
 * Currently, this union supports at most one type in each of the categories
 * below:
 *
 * + `null`
 * + `boolean`
 * + `int`, `long`, `float`, `double`
 * + `string`, `enum`
 * + `bytes`, `fixed`
 * + `array`
 * + `map`, `record`
 */
export class RealUnwrappedUnionType extends RealUnionType {
  override readonly typeName = 'union:unwrapped';

  constructor(schema, opts, /* @private parameter */ _projectionFn) {
    super(schema, opts);

    if (!_projectionFn && opts && typeof opts.wrapUnions === 'function') {
      _projectionFn = opts.wrapUnions(this.types);
    }
    this._getIndex = _projectionFn
      ? generateProjectionIndexer(_projectionFn)
      : generateDefaultIndexer(this.types, this);

    Object.freeze(this);
  }

  _check(val, flags, hook, path) {
    const index = this._getIndex(val);
    const b = index !== undefined;
    if (b) {
      return this.types[index]._check(val, flags, hook, path);
    }
    if (hook) {
      hook(val, this);
    }
    return b;
  }

  _read(tap) {
    const index = tap.readLong();
    const branchType = this.types[index];
    if (branchType) {
      return branchType._read(tap);
    }
    throw new Error(`invalid union index: ${index}`);
  }

  _write(tap, val) {
    const index = this._getIndex(val);
    if (index === undefined) {
      throw invalidValueError(val, this);
    }
    tap.writeLong(index);
    if (val !== null) {
      this.types[index]._write(tap, val);
    }
  }

  _update(resolver, type, opts) {
    for (let i = 0, l = this.types.length; i < l; i++) {
      let typeResolver;
      try {
        typeResolver = this.types[i].createResolver(type, opts);
      } catch (err) {
        continue;
      }
      resolver._read = function (tap) {
        return typeResolver._read(tap);
      };
      return;
    }
  }

  _copy(val, opts) {
    const coerce = opts && opts.coerce | 0;
    const wrap = opts && opts.wrap | 0;
    let index;
    if (wrap === 2) {
      // We are parsing a default, so always use the first branch's type.
      index = 0;
    } else {
      switch (coerce) {
        case 1:
          // Using the `coerceBuffers` option can cause corruption and erroneous
          // failures with unwrapped unions (in rare cases when the union also
          // contains a record which matches a buffer's JSON representation).
          if (isJsonBuffer(val)) {
            const bufIndex = this.types.findIndex(
              (t) => getTypeBucket(t) === 'buffer'
            );
            if (bufIndex !== -1) {
              index = bufIndex;
            }
          }
          index ??= this._getIndex(val);
          break;
        case 2:
          // Decoding from JSON, we must unwrap the value.
          if (val === null) {
            index = this._getIndex(null);
          } else if (typeof val === 'object') {
            const keys = Object.keys(val);
            if (keys.length === 1) {
              index = this._branchIndices[keys[0]];
              val = val[keys[0]];
            }
          }
          break;
        default:
          index = this._getIndex(val);
      }
      if (index === undefined) {
        throw invalidValueError(val, this);
      }
    }
    const type = this.types[index];
    if (val === null || wrap === 3) {
      return type._copy(val, opts);
    }
    switch (coerce) {
      case 3: {
        // Encoding to JSON, we wrap the value.
        const obj = {};
        obj[type.branchName] = type._copy(val, opts);
        return obj;
      }
      default:
        return type._copy(val, opts);
    }
  }

  compare(val1, val2) {
    const index1 = this._getIndex(val1);
    const index2 = this._getIndex(val2);
    if (index1 === undefined) {
      throw invalidValueError(val1, this);
    } else if (index2 === undefined) {
      throw invalidValueError(val2, this);
    } else if (index1 === index2) {
      return this.types[index1].compare(val1, val2);
    } else {
      return utils.compare(index1, index2);
    }
  }
}

/**
 * Compatible union type.
 *
 * Values of this type are represented in memory similarly to their JSON
 * representation (i.e. inside an object with single key the name of the
 * contained type).
 *
 * This is not ideal, but is the most efficient way to unambiguously support
 * all unions. Here are a few reasons why the wrapping object is necessary:
 *
 * + Unions with multiple number types would have undefined behavior, unless
 *   numbers are wrapped (either everywhere, leading to large performance and
 *   convenience costs; or only when necessary inside unions, making it hard to
 *   understand when numbers are wrapped or not).
 * + Fixed types would have to be wrapped to be distinguished from bytes.
 * + Using record's constructor names would work (after a slight change to use
 *   the fully qualified name), but would mean that generic objects could no
 *   longer be valid records (making it inconvenient to do simple things like
 *   creating new records).
 */
export class RealWrappedUnionType extends RealUnionType {
  override readonly typeName = 'union:wrapped';

  constructor(schema, opts) {
    super(schema, opts);
    Object.freeze(this);
  }

  _check(val, flags, hook, path) {
    let b = false;
    if (val === null) {
      // Shortcut type lookup in this case.
      b = this._branchIndices['null'] !== undefined;
    } else if (typeof val == 'object') {
      const keys = Object.keys(val);
      if (keys.length === 1) {
        // We require a single key here to ensure that writes are correct and
        // efficient as soon as a record passes this check.
        const name = keys[0];
        const index = this._branchIndices[name];
        if (index !== undefined) {
          if (hook) {
            // Slow path.
            path.push(name);
            b = this.types[index]._check(val[name], flags, hook, path);
            path.pop();
            return b;
          }
          return this.types[index]._check(val[name], flags);
        }
      }
    }
    if (!b && hook) {
      hook(val, this);
    }
    return b;
  }

  _read(tap) {
    const type = this.types[tap.readLong()];
    if (!type) {
      throw new Error('invalid union index');
    }
    const Branch = type._branchConstructor;
    if (Branch === null) {
      return null;
    }
    return new Branch(type._read(tap));
  }

  _write(tap, val) {
    let index;
    if (val === null) {
      index = this._branchIndices['null'];
      if (index === undefined) {
        throw invalidValueError(val, this);
      }
      tap.writeLong(index);
    } else {
      let keys = Object.keys(val),
        name;
      if (keys.length === 1) {
        name = keys[0];
        index = this._branchIndices[name];
      }
      if (index === undefined) {
        throw invalidValueError(val, this);
      }
      tap.writeLong(index);
      this.types[index]._write(tap, val[name]);
    }
  }

  _update(resolver, type, opts) {
    for (let i = 0, l = this.types.length; i < l; i++) {
      let typeResolver;
      try {
        typeResolver = this.types[i].createResolver(type, opts);
      } catch (err) {
        continue;
      }
      const Branch = this.types[i]._branchConstructor;
      if (Branch) {
        // The loop exits after the first function is created.

        resolver._read = function (tap) {
          return new Branch(typeResolver._read(tap));
        };
      } else {
        resolver._read = function () {
          return null;
        };
      }
      return;
    }
  }

  _copy(val, opts) {
    const wrap = opts && opts.wrap | 0;
    if (wrap === 2) {
      const firstType = this.types[0];
      // Promote into first type (used for schema defaults).
      if (val === null && firstType.typeName === 'null') {
        return null;
      }
      return new firstType._branchConstructor(firstType._copy(val, opts));
    }
    if (val === null && this._branchIndices['null'] !== undefined) {
      return null;
    }

    let i, l, obj;
    if (typeof val == 'object') {
      const keys = Object.keys(val);
      if (keys.length === 1) {
        const name = keys[0];
        i = this._branchIndices[name];
        if (i === undefined && opts.qualifyNames) {
          // We are a bit more flexible than in `_check` here since we have
          // to deal with other serializers being less strict, so we fall
          // back to looking up unqualified names.
          let j, type;
          for (j = 0, l = this.types.length; j < l; j++) {
            type = this.types[j];
            if (type.name && name === utils.unqualify(type.name)) {
              i = j;
              break;
            }
          }
        }
        if (i !== undefined) {
          obj = this.types[i]._copy(val[name], opts);
        }
      }
    }
    if (wrap === 1 && obj === undefined) {
      // Try promoting into first match (convenience, slow).
      i = 0;
      l = this.types.length;
      while (i < l && obj === undefined) {
        try {
          obj = this.types[i]._copy(val, opts);
        } catch (err) {
          i++;
        }
      }
    }
    if (obj !== undefined) {
      return wrap === 3 ? obj : new this.types[i]._branchConstructor(obj);
    }
    throw invalidValueError(val, this);
  }

  compare(val1, val2) {
    const name1 = val1 === null ? 'null' : Object.keys(val1)[0];
    const name2 = val2 === null ? 'null' : Object.keys(val2)[0];
    const index = this._branchIndices[name1];
    if (name1 === name2) {
      return name1 === 'null'
        ? 0
        : this.types[index].compare(val1[name1], val2[name1]);
    }
    return utils.compare(index, this._branchIndices[name2]);
  }
}

/** Get a type's bucket when included inside an unwrapped union. */
function getTypeBucket(type: Type): string {
  const typeName = type.typeName;
  switch (typeName) {
    case 'double':
    case 'float':
    case 'int':
    case 'long':
      return 'number';
    case 'bytes':
    case 'fixed':
      return 'buffer';
    case 'enum':
      return 'string';
    case 'map':
    case 'error':
    case 'record':
      return 'object';
    default:
      return typeName;
  }
}

/** Infer a value's bucket (see unwrapped unions for more details). */
function getValueBucket(val: unknown): string {
  if (val === null) {
    return 'null';
  }
  const bucket = typeof val;
  if (bucket === 'object') {
    // Could be bytes, fixed, array, map, or record.
    if (Array.isArray(val)) {
      return 'array';
    } else if (isBufferLike(val)) {
      return 'buffer';
    }
  }
  return bucket;
}

/** Check whether a collection of types leads to an ambiguous union. */
function isAmbiguous(types: ReadonlyArray<Type>): boolean {
  const buckets = {};
  for (let i = 0, l = types.length; i < l; i++) {
    const type = types[i];
    if (!Type.isType(type, 'logical')) {
      const bucket = getTypeBucket(type);
      if (buckets[bucket]) {
        return true;
      }
      buckets[bucket] = true;
    }
  }
  return false;
}
