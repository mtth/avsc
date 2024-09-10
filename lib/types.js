// TODO: Make it easier to implement custom types. This will likely require
// exposing the `Tap` object, perhaps under another name. Probably worth a
// major release.
// TODO: Allow configuring when to write the size when writing arrays and maps,
// and customizing their block size.
// TODO: Code-generate `compare` and `clone` record and union methods.

'use strict';

/**
 * This module defines all Avro data types and their serialization logic.
 *
 */

let utils = require('./utils'),
    platform = require('./platform');

// Convenience imports.
let {Tap, isBufferLike} = utils;
let debug = platform.debuglog('avsc:types');
let j = utils.printJSON;

// All non-union concrete (i.e. non-logical) Avro types.
// Defined after all the type classes are defined.
let TYPES;

// Random generator.
let RANDOM = new utils.Lcg();

// Encoding tap (shared for performance).
let TAP = Tap.withCapacity(1024);

// Currently active logical type, used for name redirection.
let LOGICAL_TYPE = null;

// Underlying types of logical types currently being instantiated. This is used
// to be able to reference names (i.e. for branches) during instantiation.
let UNDERLYING_TYPES = [];

/**
 * "Abstract" base Avro type.
 *
 * This class' constructor will register any named types to support recursive
 * schemas. All type values are represented in memory similarly to their JSON
 * representation, except for:
 *
 * + `bytes` and `fixed` which are represented as `Buffer`s.
 * + `union`s which will be "unwrapped" unless the `wrapUnions` option is set.
 *
 *  See individual subclasses for details.
 */
class Type {
  constructor (schema, opts) {
    let type;
    if (LOGICAL_TYPE) {
      type = LOGICAL_TYPE;
      UNDERLYING_TYPES.push([LOGICAL_TYPE, this]);
      LOGICAL_TYPE = null;
    } else {
      type = this;
    }

    // Lazily instantiated hash string. It will be generated the first time the
    // type's default fingerprint is computed (for example when using `equals`).
    // We use a mutable object since types are frozen after instantiation.
    this._hash = new Hash();
    this.name = undefined;
    this.aliases = undefined;
    this.doc = (schema && schema.doc) ? '' + schema.doc : undefined;

    if (schema) {
      // This is a complex (i.e. non-primitive) type.
      let name = schema.name;
      let namespace = schema.namespace === undefined ?
        opts && opts.namespace :
        schema.namespace;
      if (name !== undefined) {
        // This isn't an anonymous type.
        name = maybeQualify(name, namespace);
        if (isPrimitive(name)) {
          // Avro doesn't allow redefining primitive names.
          throw new Error(`cannot rename primitive type: ${j(name)}`);
        }
        let registry = opts && opts.registry;
        if (registry) {
          if (registry[name] !== undefined) {
            throw new Error(`duplicate type name: ${name}`);
          }
          registry[name] = type;
        }
      } else if (opts && opts.noAnonymousTypes) {
        throw new Error(`missing name property in schema: ${j(schema)}`);
      }
      this.name = name;
      this.aliases = schema.aliases ?
        schema.aliases.map((s) => { return maybeQualify(s, namespace); }) :
        [];
    }
  }

  static forSchema (schema, opts) {
    opts = Object.assign({}, opts);
    opts.registry = opts.registry || {};

    let UnionType = (function (wrapUnions) {
      if (wrapUnions === true) {
        wrapUnions = 'always';
      } else if (wrapUnions === false) {
        wrapUnions = 'never';
      } else if (wrapUnions === undefined) {
        wrapUnions = 'auto';
      } else if (typeof wrapUnions == 'string') {
        wrapUnions = wrapUnions.toLowerCase();
      }
      switch (wrapUnions) {
        case 'always':
          return WrappedUnionType;
        case 'never':
          return UnwrappedUnionType;
        case 'auto':
          return undefined; // Determined dynamically later on.
        default:
          throw new Error(`invalid wrap unions option: ${j(wrapUnions)}`);
      }
    })(opts.wrapUnions);

    if (schema === null) {
      // Let's be helpful for this common error.
      throw new Error('invalid type: null (did you mean "null"?)');
    }

    if (Type.isType(schema)) {
      return schema;
    }

    let type;
    if (opts.typeHook && (type = opts.typeHook(schema, opts))) {
      if (!Type.isType(type)) {
        throw new Error(`invalid typehook return value: ${j(type)}`);
      }
      return type;
    }

    if (typeof schema == 'string') { // Type reference.
      schema = maybeQualify(schema, opts.namespace);
      type = opts.registry[schema];
      if (type) {
        // Type was already defined, return it.
        return type;
      }
      if (isPrimitive(schema)) {
        // Reference to a primitive type. These are also defined names by
        // default so we create the appropriate type and it to the registry for
        // future reference.
        type = Type.forSchema({type: schema}, opts);
        opts.registry[schema] = type;
        return type;
      }
      throw new Error(`undefined type name: ${schema}`);
    }

    if (schema.logicalType && opts.logicalTypes && !LOGICAL_TYPE) {
      let DerivedType = opts.logicalTypes[schema.logicalType];
      // TODO: check to ensure DerivedType was derived from LogicalType via ES6
      // subclassing; otherwise it will not work properly
      if (DerivedType) {
        let namespace = opts.namespace;
        let registry = {};
        Object.keys(opts.registry).forEach((key) => {
          registry[key] = opts.registry[key];
        });
        try {
          debug('instantiating logical type for %s', schema.logicalType);
          return new DerivedType(schema, opts);
        } catch (err) {
          debug('failed to instantiate logical type for %s', schema.logicalType);
          if (opts.assertLogicalTypes) {
            // The spec mandates that we fall through to the underlying type if
            // the logical type is invalid. We provide this option to ease
            // debugging.
            throw err;
          }
          LOGICAL_TYPE = null;
          opts.namespace = namespace;
          opts.registry = registry;
        }
      }
    }

    if (Array.isArray(schema)) { // Union.
      // We temporarily clear the logical type since we instantiate the branch's
      // types before the underlying union's type (necessary to decide whether
      // the union is ambiguous or not).
      let logicalType = LOGICAL_TYPE;
      LOGICAL_TYPE = null;
      let types = schema.map((obj) => {
        return Type.forSchema(obj, opts);
      });
      if (!UnionType) {
        UnionType = isAmbiguous(types) ? WrappedUnionType : UnwrappedUnionType;
      }
      LOGICAL_TYPE = logicalType;
      type = new UnionType(types, opts);
    } else { // New type definition.
      type = (function (typeName) {
        let Type = TYPES[typeName];
        if (Type === undefined) {
          throw new Error(`unknown type: ${j(typeName)}`);
        }
        return new Type(schema, opts);
      })(schema.type);
    }
    return type;
  }

  static forValue (val, opts) {
    opts = Object.assign({}, opts);

    // Sentinel used when inferring the types of empty arrays.
    opts.emptyArrayType = opts.emptyArrayType || Type.forSchema({
      type: 'array', items: 'null'
    });

    // Optional custom inference hook.
    if (opts.valueHook) {
      let type = opts.valueHook(val, opts);
      if (type !== undefined) {
        if (!Type.isType(type)) {
          throw new Error(`invalid value hook return value: ${j(type)}`);
        }
        return type;
      }
    }

    // Default inference logic.
    switch (typeof val) {
      case 'string':
        return Type.forSchema('string', opts);
      case 'boolean':
        return Type.forSchema('boolean', opts);
      case 'number':
        if ((val | 0) === val) {
          return Type.forSchema('int', opts);
        } else if (Math.abs(val) < 9007199254740991) {
          return Type.forSchema('float', opts);
        }
        return Type.forSchema('double', opts);
      case 'object': {
        if (val === null) {
          return Type.forSchema('null', opts);
        } else if (Array.isArray(val)) {
          if (!val.length) {
            return opts.emptyArrayType;
          }
          return Type.forSchema({
            type: 'array',
            items: Type.forTypes(
              val.map((v) => { return Type.forValue(v, opts); }),
              opts
            )
          }, opts);
        } else if (isBufferLike(val)) {
          return Type.forSchema('bytes', opts);
        }
        let fieldNames = Object.keys(val);
        if (fieldNames.some((s) => { return !utils.isValidName(s); })) {
          // We have to fall back to a map.
          return Type.forSchema({
            type: 'map',
            values: Type.forTypes(fieldNames.map((s) => {
              return Type.forValue(val[s], opts);
            }), opts)
          }, opts);
        }
        return Type.forSchema({
          type: 'record',
          fields: fieldNames.map((s) => {
            return {name: s, type: Type.forValue(val[s], opts)};
          })
        }, opts);
      }
      default:
        throw new Error(`cannot infer type from: ${j(val)}`);
    }
  }

  static forTypes (types, opts) {
    if (!types.length) {
      throw new Error('no types to combine');
    }
    if (types.length === 1) {
      return types[0]; // Nothing to do.
    }
    opts = Object.assign({}, opts);

    // Extract any union types, with special care for wrapped unions (see
    // below).
    let expanded = [];
    let numWrappedUnions = 0;
    let isValidWrappedUnion = true;
    types.forEach((type) => {
      switch (type.typeName) {
        case 'union:unwrapped':
          isValidWrappedUnion = false;
          expanded = expanded.concat(type.types);
          break;
        case 'union:wrapped':
          numWrappedUnions++;
          expanded = expanded.concat(type.types);
          break;
        case 'null':
          expanded.push(type);
          break;
        default:
          isValidWrappedUnion = false;
          expanded.push(type);
      }
    });
    if (numWrappedUnions) {
      if (!isValidWrappedUnion) {
        // It is only valid to combine wrapped unions when no other type is
        // present other than wrapped unions and nulls (otherwise the values of
        // others wouldn't be valid in the resulting union).
        throw new Error('cannot combine wrapped union');
      }
      let branchTypes = {};
      expanded.forEach((type) => {
        let name = type.branchName;
        let branchType = branchTypes[name];
        if (!branchType) {
          branchTypes[name] = type;
        } else if (!type.equals(branchType)) {
          throw new Error('inconsistent branch type');
        }
      });
      let wrapUnions = opts.wrapUnions;
      let unionType;
      opts.wrapUnions = true;
      try {
        unionType = Type.forSchema(Object.keys(branchTypes).map((name) => {
          return branchTypes[name];
        }), opts);
      } catch (err) {
        opts.wrapUnions = wrapUnions;
        throw err;
      }
      opts.wrapUnions = wrapUnions;
      return unionType;
    }

    // Group types by category, similar to the logic for unwrapped unions.
    let bucketized = {};
    expanded.forEach((type) => {
      let bucket = getTypeBucket(type);
      let bucketTypes = bucketized[bucket];
      if (!bucketTypes) {
        bucketized[bucket] = bucketTypes = [];
      }
      bucketTypes.push(type);
    });

    // Generate the "augmented" type for each group.
    let buckets = Object.keys(bucketized);
    let augmented = buckets.map((bucket) => {
      let bucketTypes = bucketized[bucket];
      if (bucketTypes.length === 1) {
        return bucketTypes[0];
      } else {
        switch (bucket) {
          case 'null':
          case 'boolean':
            return bucketTypes[0];
          case 'number':
            return combineNumbers(bucketTypes);
          case 'string':
            return combineStrings(bucketTypes, opts);
          case 'buffer':
            return combineBuffers(bucketTypes, opts);
          case 'array':
            // Remove any sentinel arrays (used when inferring from empty
            // arrays) to avoid making things nullable when they shouldn't be.
            bucketTypes = bucketTypes.filter((t) => {
              return t !== opts.emptyArrayType;
            });
            if (!bucketTypes.length) {
              // We still don't have a real type, just return the sentinel.
              return opts.emptyArrayType;
            }
            return Type.forSchema({
              type: 'array',
              items: Type.forTypes(bucketTypes.map((t) => {
                return t.itemsType;
              }), opts)
            }, opts);
          default:
            return combineObjects(bucketTypes, opts);
        }
      }
    });

    if (augmented.length === 1) {
      return augmented[0];
    } else {
      // We return an (unwrapped) union of all augmented types.
      return Type.forSchema(augmented, opts);
    }
  }

  static isType (/* any, [prefix] ... */) {
    let l = arguments.length;
    if (!l) {
      return false;
    }

    let any = arguments[0];
    if (
      !any ||
      typeof any._update != 'function' ||
      typeof any.fingerprint != 'function'
    ) {
      // Not fool-proof, but most likely good enough.
      return false;
    }

    if (l === 1) {
      // No type names specified, we are done.
      return true;
    }

    // We check if at least one of the prefixes matches.
    let typeName = any.typeName;
    for (let i = 1; i < l; i++) {
      if (typeName.indexOf(arguments[i]) === 0) {
        return true;
      }
    }
    return false;
  }

  static __reset (size) {
    debug('resetting type buffer to %d', size);
    TAP.reinitialize(size);
  }

  get branchName () {
    let type = Type.isType(this, 'logical') ? this.underlyingType : this;
    if (type.name) {
      return type.name;
    }
    if (Type.isType(type, 'abstract')) {
      return type._concreteTypeName;
    }
    return Type.isType(type, 'union') ? undefined : type.typeName;
  }

  clone (val, opts) {
    if (opts) {
      opts = {
        coerce: !!opts.coerceBuffers | 0, // Coerce JSON to Buffer.
        fieldHook: opts.fieldHook,
        qualifyNames: !!opts.qualifyNames,
        skip: !!opts.skipMissingFields,
        wrap: !!opts.wrapUnions | 0 // Wrap first match into union.
      };
      return this._copy(val, opts);
    } else {
      // If no modifications are required, we can get by with a serialization
      // roundtrip (generally much faster than a standard deep copy).
      return this.fromBuffer(this.toBuffer(val));
    }
  }

  compareBuffers (buf1, buf2) {
    return this._match(Tap.fromBuffer(buf1), Tap.fromBuffer(buf2));
  }

  createResolver (type, opts) {
    if (!Type.isType(type)) {
      // More explicit error message than the "incompatible type" thrown
      // otherwise (especially because of the overridden `toJSON` method).
      throw new Error(`not a type: ${j(type)}`);
    }

    if (!Type.isType(this, 'union', 'logical') && Type.isType(type, 'logical')) {
      // Trying to read a logical type as a built-in: unwrap the logical type.
      // Note that we exclude unions to support resolving into unions containing
      // logical types.
      return this.createResolver(type.underlyingType, opts);
    }

    opts = Object.assign({}, opts);
    opts.registry = opts.registry || {};

    let resolver, key;
    if (
      Type.isType(this, 'record', 'error') &&
      Type.isType(type, 'record', 'error')
    ) {
      // We allow conversions between records and errors.
      key = this.name + ':' + type.name; // ':' is illegal in Avro type names.
      resolver = opts.registry[key];
      if (resolver) {
        return resolver;
      }
    }

    resolver = new Resolver(this);
    if (key) { // Register resolver early for recursive schemas.
      opts.registry[key] = resolver;
    }

    if (Type.isType(type, 'union')) {
      let resolvers = type.types.map(function (t) {
        return this.createResolver(t, opts);
      }, this);
      resolver._read = function (tap) {
        let index = tap.readLong();
        let resolver = resolvers[index];
        if (resolver === undefined) {
          throw new Error(`invalid union index: ${index}`);
        }
        return resolvers[index]._read(tap);
      };
    } else {
      this._update(resolver, type, opts);
    }

    if (!resolver._read) {
      throw new Error(`cannot read ${type} as ${this}`);
    }
    return Object.freeze(resolver);
  }

  decode (buf, pos, resolver) {
    let tap = Tap.fromBuffer(buf, pos);
    let val = readValue(this, tap, resolver);
    if (!tap.isValid()) {
      return {value: undefined, offset: -1};
    }
    return {value: val, offset: tap.pos};
  }

  encode (val, buf, pos) {
    let tap = Tap.fromBuffer(buf, pos);
    this._write(tap, val);
    if (!tap.isValid()) {
      // Don't throw as there is no way to predict this. We also return the
      // number of missing bytes to ease resizing.
      return buf.length - tap.pos;
    }
    return tap.pos;
  }

  equals (type, opts) {
    let canon = ( // Canonical equality.
      Type.isType(type) &&
      utils.bufCompare(this.fingerprint(), type.fingerprint()) === 0
    );
    if (!canon || !(opts && opts.strict)) {
      return canon;
    }
    return (
      JSON.stringify(this.schema({exportAttrs: true})) ===
      JSON.stringify(type.schema({exportAttrs: true}))
    );
  }

  fingerprint (algorithm) {
    if (!algorithm) {
      if (!this._hash.hash) {
        let schemaStr = JSON.stringify(this.schema());
        this._hash.hash = utils.getHash(schemaStr);
      }
      return this._hash.hash;
    } else {
      return utils.getHash(JSON.stringify(this.schema()), algorithm);
    }
  }

  fromBuffer (buf, resolver, noCheck) {
    let tap = Tap.fromBuffer(buf, 0);
    let val = readValue(this, tap, resolver, noCheck);
    if (!tap.isValid()) {
      throw new Error('truncated buffer');
    }
    if (!noCheck && tap.pos < buf.length) {
      throw new Error('trailing data');
    }
    return val;
  }

  fromString (str) {
    return this._copy(JSON.parse(str), {coerce: 2});
  }

  inspect () {
    let typeName = this.typeName;
    let className = getClassName(typeName);
    if (isPrimitive(typeName)) {
      // The class name is sufficient to identify the type.
      return `<${className}>`;
    } else {
      // We add a little metadata for convenience.
      let obj = this.schema({exportAttrs: true, noDeref: true});
      if (typeof obj == 'object' && !Type.isType(this, 'logical')) {
        obj.type = undefined; // Would be redundant with constructor name.
      }
      return `<${className} ${j(obj)}>`;
    }
  }

  isValid (val, opts) {
    // We only have a single flag for now, so no need to complicate things.
    let flags = (opts && opts.noUndeclaredFields) | 0;
    let errorHook = opts && opts.errorHook;
    let hook, path;
    if (errorHook) {
      path = [];
      hook = function (any, type) {
        errorHook.call(this, path.slice(), any, type, val);
      };
    }
    return this._check(val, flags, hook, path);
  }

  schema (opts) {
    // Copy the options to avoid mutating the original options object when we
    // add the registry of dereferenced types.
    return this._attrs({}, {
      exportAttrs: !!(opts && opts.exportAttrs),
      noDeref: !!(opts && opts.noDeref)
    });
  }

  toBuffer (val) {
    TAP.pos = 0;
    this._write(TAP, val);
    if (TAP.isValid()) {
      return TAP.toBuffer();
    }
    let buf = new Uint8Array(TAP.pos);
    this._write(Tap.fromBuffer(buf), val);
    return buf;
  }

  toJSON () {
    // Convenience to allow using `JSON.stringify(type)` to get a type's schema.
    return this.schema({exportAttrs: true});
  }

  toString (val) {
    if (val === undefined) {
      // Consistent behavior with standard `toString` expectations.
      return JSON.stringify(this.schema({noDeref: true}));
    }
    return JSON.stringify(this._copy(val, {coerce: 3}));
  }

  wrap (val) {
    let Branch = this._branchConstructor;
    return Branch === null ? null : new Branch(val);
  }

  _attrs (derefed, opts) {
    // This function handles a lot of the common logic to schema generation
    // across types, for example keeping track of which types have already been
    // de-referenced (i.e. derefed).
    let name = this.name;
    if (name !== undefined) {
      if (opts.noDeref || derefed[name]) {
        return name;
      }
      derefed[name] = true;
    }
    let schema = {};
    // The order in which we add fields to the `schema` object matters here.
    // Since JS objects are unordered, this implementation (unfortunately)
    // relies on engines returning properties in the same order that they are
    // inserted in. This is not in the JS spec, but can be "somewhat" safely
    // assumed (see http://stackoverflow.com/q/5525795/1062617).
    if (this.name !== undefined) {
      schema.name = name;
    }
    schema.type = this.typeName;
    let derefedSchema = this._deref(schema, derefed, opts);
    if (derefedSchema !== undefined) {
      // We allow the original schema to be overridden (this will happen for
      // primitive types and logical types).
      schema = derefedSchema;
    }
    if (opts.exportAttrs) {
      if (this.aliases && this.aliases.length) {
        schema.aliases = this.aliases;
      }
      if (this.doc !== undefined) {
        schema.doc = this.doc;
      }
    }
    return schema;
  }

  _createBranchConstructor () {
    let name = this.branchName;
    if (name === 'null') {
      return null;
    }
    let attr = ~name.indexOf('.') ? 'this[\'' + name + '\']' : 'this.' + name;
    let body = 'return function Branch$(val) { ' + attr + ' = val; };';
    // eslint-disable-next-line no-new-func
    let Branch = (new Function(body))();
    Branch.type = this;
    // eslint-disable-next-line no-new-func
    Branch.prototype.unwrap = new Function('return ' + attr + ';');
    Branch.prototype.unwrapped = Branch.prototype.unwrap; // Deprecated.
    return Branch;
  }

  _peek (tap) {
    let pos = tap.pos;
    let val = this._read(tap);
    tap.pos = pos;
    return val;
  }

  compare () { utils.abstractFunction(); }
  random () { utils.abstractFunction(); }
  _check () { utils.abstractFunction(); }
  _copy () { utils.abstractFunction(); }
  _deref () { utils.abstractFunction(); }
  _match () { utils.abstractFunction(); }
  _read () { utils.abstractFunction(); }
  _skip () { utils.abstractFunction(); }
  _update () { utils.abstractFunction(); }
  _write () { utils.abstractFunction(); }
}

// "Deprecated" getters (will be explicitly deprecated in 5.1).

Type.prototype.getAliases = function () { return this.aliases; };

Type.prototype.getFingerprint = Type.prototype.fingerprint;

Type.prototype.getName = function (asBranch) {
  return (this.name || !asBranch) ? this.name : this.branchName;
};

Type.prototype.getSchema = Type.prototype.schema;

Type.prototype.getTypeName = function () { return this.typeName; };

// Implementations.

/**
 * Base primitive Avro type.
 *
 * Most of the primitive types share the same cloning and resolution
 * mechanisms, provided by this class. This class also lets us conveniently
 * check whether a type is a primitive using `instanceof`.
 */
class PrimitiveType extends Type {
  constructor (noFreeze) {
    super();
    this._branchConstructor = this._createBranchConstructor();
    if (!noFreeze) {
      // Abstract long types can't be frozen at this stage.
      Object.freeze(this);
    }
  }

  _update (resolver, type) {
    if (type.typeName === this.typeName) {
      resolver._read = this._read;
    }
  }

  _copy (val) {
    this._check(val, undefined, throwInvalidError);
    return val;
  }

  _deref () { return this.typeName; }

  compare (a, b) {
    return utils.compare(a, b);
  }
}

/** Nulls. */
class NullType extends PrimitiveType {
  _check (val, flags, hook) {
    let b = val === null;
    if (!b && hook) {
      hook(val, this);
    }
    return b;
  }

  _read () { return null; }

  _skip () {}

  _write (tap, val) {
    if (val !== null) {
      throwInvalidError(val, this);
    }
  }

  _match () { return 0; }
}

NullType.prototype.compare = NullType.prototype._match;

NullType.prototype.typeName = 'null';

NullType.prototype.random = NullType.prototype._read;

/** Booleans. */
class BooleanType extends PrimitiveType {
  _check (val, flags, hook) {
    let b = typeof val == 'boolean';
    if (!b && hook) {
      hook(val, this);
    }
    return b;
  }

  _read (tap) { return tap.readBoolean(); }

  _skip (tap) { tap.skipBoolean(); }

  _write (tap, val) {
    if (typeof val != 'boolean') {
      throwInvalidError(val, this);
    }
    tap.writeBoolean(val);
  }

  _match (tap1, tap2) {
    return tap1.matchBoolean(tap2);
  }

  random () { return RANDOM.nextBoolean(); }
}

BooleanType.prototype.typeName = 'boolean';

/** Integers. */
class IntType extends PrimitiveType {
  _check (val, flags, hook) {
    let b = val === (val | 0);
    if (!b && hook) {
      hook(val, this);
    }
    return b;
  }

  _read (tap) { return tap.readLong(); }

  _skip (tap) { tap.skipLong(); }

  _write (tap, val) {
    if (val !== (val | 0)) {
      throwInvalidError(val, this);
    }
    tap.writeLong(val);
  }

  _match (tap1, tap2) {
    return tap1.matchLong(tap2);
  }

  random () { return RANDOM.nextInt(1000) | 0; }
}

IntType.prototype.typeName = 'int';

/**
 * Longs.
 *
 * We can't capture all the range unfortunately since JavaScript represents all
 * numbers internally as `double`s, so the default implementation plays safe
 * and throws rather than potentially silently change the data. See `__with` or
 * `AbstractLongType` below for a way to implement a custom long type.
 */
class LongType extends PrimitiveType {
  // TODO: rework AbstractLongType so we don't need to accept noFreeze here
  constructor (noFreeze) { super(noFreeze); }

  _check (val, flags, hook) {
    let b = typeof val == 'number' && val % 1 === 0 && isSafeLong(val);
    if (!b && hook) {
      hook(val, this);
    }
    return b;
  }

  _read (tap) {
    let n = tap.readLong();
    if (!isSafeLong(n)) {
      throw new Error('potential precision loss');
    }
    return n;
  }

  _skip (tap) { tap.skipLong(); }

  _write (tap, val) {
    if (typeof val != 'number' || val % 1 || !isSafeLong(val)) {
      throwInvalidError(val, this);
    }
    tap.writeLong(val);
  }

  _match (tap1, tap2) {
    return tap1.matchLong(tap2);
  }

  _update (resolver, type) {
    switch (type.typeName) {
      case 'int':
        resolver._read = type._read;
        break;
      case 'abstract:long':
      case 'long':
        resolver._read = this._read; // In case `type` is an `AbstractLongType`.
    }
  }

  random () { return RANDOM.nextInt(); }

  static __with (methods, noUnpack) {
    methods = methods || {}; // Will give a more helpful error message.
    // We map some of the methods to a different name to be able to intercept
    // their input and output (otherwise we wouldn't be able to perform any
    // unpacking logic, and the type wouldn't work when nested).
    let mapping = {
      toBuffer: '_toBuffer',
      fromBuffer: '_fromBuffer',
      fromJSON: '_fromJSON',
      toJSON: '_toJSON',
      isValid: '_isValid',
      compare: 'compare'
    };
    let type = new AbstractLongType(noUnpack);
    Object.keys(mapping).forEach((name) => {
      if (methods[name] === undefined) {
        throw new Error(`missing method implementation: ${name}`);
      }
      type[mapping[name]] = methods[name];
    });
    return Object.freeze(type);
  }
}

LongType.prototype.typeName = 'long';

/** Floats. */
class FloatType extends PrimitiveType {
  _check (val, flags, hook) {
    let b = typeof val == 'number';
    if (!b && hook) {
      hook(val, this);
    }
    return b;
  }

  _read (tap) { return tap.readFloat(); }

  _skip (tap) { tap.skipFloat(); }

  _write (tap, val) {
    if (typeof val != 'number') {
      throwInvalidError(val, this);
    }
    tap.writeFloat(val);
  }

  _match (tap1, tap2) {
    return tap1.matchFloat(tap2);
  }

  _update (resolver, type) {
    switch (type.typeName) {
      case 'float':
      case 'int':
        resolver._read = type._read;
        break;
      case 'abstract:long':
      case 'long':
        // No need to worry about precision loss here since we're always
        // rounding to float anyway.
        resolver._read = function (tap) { return tap.readLong(); };
    }
  }

  random () { return RANDOM.nextFloat(1e3); }
}

FloatType.prototype.typeName = 'float';

/** Doubles. */
class DoubleType extends PrimitiveType {
  _check (val, flags, hook) {
    let b = typeof val == 'number';
    if (!b && hook) {
      hook(val, this);
    }
    return b;
  }

  _read (tap) { return tap.readDouble(); }

  _skip (tap) { tap.skipDouble(); }

  _write (tap, val) {
    if (typeof val != 'number') {
      throwInvalidError(val, this);
    }
    tap.writeDouble(val);
  }

  _match (tap1, tap2) {
    return tap1.matchDouble(tap2);
  }

  _update (resolver, type) {
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
        resolver._read = function (tap) { return tap.readLong(); };
    }
  }

  random () { return RANDOM.nextFloat(); }
}

DoubleType.prototype.typeName = 'double';

/** Strings. */
class StringType extends PrimitiveType {
  _check (val, flags, hook) {
    let b = typeof val == 'string';
    if (!b && hook) {
      hook(val, this);
    }
    return b;
  }

  _read (tap) { return tap.readString(); }

  _skip (tap) { tap.skipString(); }

  _write (tap, val) {
    if (typeof val != 'string') {
      throwInvalidError(val, this);
    }
    tap.writeString(val);
  }

  _match (tap1, tap2) {
    return tap1.matchBytes(tap2);
  }

  _update (resolver, type) {
    switch (type.typeName) {
      case 'bytes':
      case 'string':
        resolver._read = this._read;
    }
  }

  random () {
    return RANDOM.nextString(
      Math.floor(-Math.log(RANDOM.nextFloat()) * 16) + 1
    );
  }
}

StringType.prototype.typeName = 'string';

/**
 * Bytes.
 *
 * These are represented in memory as `Buffer`s rather than binary-encoded
 * strings. This is more efficient (when decoding/encoding from bytes, the
 * common use-case), idiomatic, and convenient.
 *
 * Note the coercion in `_copy`.
 */
class BytesType extends PrimitiveType {
  _check (val, flags, hook) {
    let b = isBufferLike(val);
    if (!b && hook) {
      hook(val, this);
    }
    return b;
  }

  _read (tap) { return tap.readBytes(); }

  _skip (tap) { tap.skipBytes(); }

  _write (tap, val) {
    if (!isBufferLike(val)) {
      throwInvalidError(val, this);
    }
    tap.writeBytes(val);
  }

  _match (tap1, tap2) {
    return tap1.matchBytes(tap2);
  }

  _update (resolver, type) {
    switch (type.typeName) {
      case 'bytes':
      case 'string':
        resolver._read = this._read;
    }
  }

  _copy (obj, opts) {
    let buf;
    switch ((opts && opts.coerce) | 0) {
      case 3: // Coerce buffers to strings.
        this._check(obj, undefined, throwInvalidError);
        return obj.toString('binary');
      case 2: // Coerce strings to buffers.
        if (typeof obj != 'string') {
          throw new Error(`cannot coerce to buffer: ${j(obj)}`);
        }
        buf = Buffer.from(obj, 'binary');
        this._check(buf, undefined, throwInvalidError);
        return buf;
      case 1: // Coerce buffer JSON representation to buffers.
        if (!isJsonBuffer(obj)) {
          throw new Error(`cannot coerce to buffer: ${j(obj)}`);
        }
        buf = Buffer.from(obj.data);
        this._check(buf, undefined, throwInvalidError);
        return buf;
      default: // Copy buffer.
        this._check(obj, undefined, throwInvalidError);
        return Buffer.from(obj);
    }
  }

  random () {
    return RANDOM.nextBuffer(RANDOM.nextInt(32));
  }
}

BytesType.prototype.compare = utils.bufCompare;

BytesType.prototype.typeName = 'bytes';

/** Base "abstract" Avro union type. */
class UnionType extends Type {
  constructor (schema, opts) {
    super();

    if (!Array.isArray(schema)) {
      throw new Error(`non-array union schema: ${j(schema)}`);
    }
    if (!schema.length) {
      throw new Error('empty union');
    }
    this.types = Object.freeze(schema.map((obj) => {
      return Type.forSchema(obj, opts);
    }));

    this._branchIndices = {};
    this.types.forEach(function (type, i) {
      if (Type.isType(type, 'union')) {
        throw new Error('unions cannot be directly nested');
      }
      let branch = type.branchName;
      if (this._branchIndices[branch] !== undefined) {
        throw new Error(`duplicate union branch name: ${j(branch)}`);
      }
      this._branchIndices[branch] = i;
    }, this);
  }

  _skip (tap) {
    this.types[tap.readLong()]._skip(tap);
  }

  _match (tap1, tap2) {
    let n1 = tap1.readLong();
    let n2 = tap2.readLong();
    if (n1 === n2) {
      return this.types[n1]._match(tap1, tap2);
    } else {
      return n1 < n2 ? -1 : 1;
    }
  }

  _deref (schema, derefed, opts) {
    return this.types.map((t) => { return t._attrs(derefed, opts); });
  }

  getTypes () { return this.types; }
}

// Cannot be defined as a class method because it's used as a constructor.
// https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Functions/Method_definitions#method_definitions_are_not_constructable
UnionType.prototype._branchConstructor = function () {
  throw new Error('unions cannot be directly wrapped');
};

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
class UnwrappedUnionType extends UnionType {
  constructor (schema, opts) {
    super(schema, opts);

    this._dynamicBranches = null;
    this._bucketIndices = {};
    this.types.forEach(function (type, index) {
      if (Type.isType(type, 'abstract', 'logical')) {
        if (!this._dynamicBranches) {
          this._dynamicBranches = [];
        }
        this._dynamicBranches.push({index, type});
      } else {
        let bucket = getTypeBucket(type);
        if (this._bucketIndices[bucket] !== undefined) {
          throw new Error(`ambiguous unwrapped union: ${j(this)}`);
        }
        this._bucketIndices[bucket] = index;
      }
    }, this);

    Object.freeze(this);
  }

  _getIndex (val) {
    let index = this._bucketIndices[getValueBucket(val)];
    if (this._dynamicBranches) {
      // Slower path, we must run the value through all branches.
      index = this._getBranchIndex(val, index);
    }
    return index;
  }

  _getBranchIndex (any, index) {
    let logicalBranches = this._dynamicBranches;
    for (let i = 0, l = logicalBranches.length; i < l; i++) {
      let branch = logicalBranches[i];
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
  }

  _check (val, flags, hook, path) {
    let index = this._getIndex(val);
    let b = index !== undefined;
    if (b) {
      return this.types[index]._check(val, flags, hook, path);
    }
    if (hook) {
      hook(val, this);
    }
    return b;
  }

  _read (tap) {
    let index = tap.readLong();
    let branchType = this.types[index];
    if (branchType) {
      return branchType._read(tap);
    } else {
      throw new Error(`invalid union index: ${index}`);
    }
  }

  _write (tap, val) {
    let index = this._getIndex(val);
    if (index === undefined) {
      throwInvalidError(val, this);
    }
    tap.writeLong(index);
    if (val !== null) {
      this.types[index]._write(tap, val);
    }
  }

  _update (resolver, type, opts) {
    for (let i = 0, l = this.types.length; i < l; i++) {
      let typeResolver;
      try {
        typeResolver = this.types[i].createResolver(type, opts);
      } catch (err) {
        continue;
      }
      resolver._read = function (tap) { return typeResolver._read(tap); };
      return;
    }
  }

  _copy (val, opts) {
    let coerce = opts && opts.coerce | 0;
    let wrap = opts && opts.wrap | 0;
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
          if (isJsonBuffer(val) && this._bucketIndices.buffer !== undefined) {
            index = this._bucketIndices.buffer;
          } else {
            index = this._getIndex(val);
          }
          break;
        case 2:
          // Decoding from JSON, we must unwrap the value.
          if (val === null) {
            index = this._bucketIndices['null'];
          } else if (typeof val === 'object') {
            let keys = Object.keys(val);
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
        throwInvalidError(val, this);
      }
    }
    let type = this.types[index];
    if (val === null || wrap === 3) {
      return type._copy(val, opts);
    } else {
      switch (coerce) {
        case 3: {
          // Encoding to JSON, we wrap the value.
          let obj = {};
          obj[type.branchName] = type._copy(val, opts);
          return obj;
        }
        default:
          return type._copy(val, opts);
      }
    }
  }

  compare (val1, val2) {
    let index1 = this._getIndex(val1);
    let index2 = this._getIndex(val2);
    if (index1 === undefined) {
      throwInvalidError(val1, this);
    } else if (index2 === undefined) {
      throwInvalidError(val2, this);
    } else if (index1 === index2) {
      return this.types[index1].compare(val1, val2);
    } else {
      return utils.compare(index1, index2);
    }
  }

  random () {
    let index = RANDOM.nextInt(this.types.length);
    return this.types[index].random();
  }
}

UnwrappedUnionType.prototype.typeName = 'union:unwrapped';

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
class WrappedUnionType extends UnionType {
  constructor (schema, opts) {
    super(schema, opts);
    Object.freeze(this);
  }

  _check (val, flags, hook, path) {
    let b = false;
    if (val === null) {
      // Shortcut type lookup in this case.
      b = this._branchIndices['null'] !== undefined;
    } else if (typeof val == 'object') {
      let keys = Object.keys(val);
      if (keys.length === 1) {
        // We require a single key here to ensure that writes are correct and
        // efficient as soon as a record passes this check.
        let name = keys[0];
        let index = this._branchIndices[name];
        if (index !== undefined) {
          if (hook) {
            // Slow path.
            path.push(name);
            b = this.types[index]._check(val[name], flags, hook, path);
            path.pop();
            return b;
          } else {
            return this.types[index]._check(val[name], flags);
          }
        }
      }
    }
    if (!b && hook) {
      hook(val, this);
    }
    return b;
  }

  _read (tap) {
    let type = this.types[tap.readLong()];
    if (!type) {
      throw new Error('invalid union index');
    }
    let Branch = type._branchConstructor;
    if (Branch === null) {
      return null;
    } else {
      return new Branch(type._read(tap));
    }
  }

  _write (tap, val) {
    let index;
    if (val === null) {
      index = this._branchIndices['null'];
      if (index === undefined) {
        throwInvalidError(val, this);
      }
      tap.writeLong(index);
    } else {
      let keys = Object.keys(val), name;
      if (keys.length === 1) {
        name = keys[0];
        index = this._branchIndices[name];
      }
      if (index === undefined) {
        throwInvalidError(val, this);
      }
      tap.writeLong(index);
      this.types[index]._write(tap, val[name]);
    }
  }

  _update (resolver, type, opts) {
    for (let i = 0, l = this.types.length; i < l; i++) {
      let typeResolver;
      try {
        typeResolver = this.types[i].createResolver(type, opts);
      } catch (err) {
        continue;
      }
      let Branch = this.types[i]._branchConstructor;
      if (Branch) {
        // The loop exits after the first function is created.
        // eslint-disable-next-line no-loop-func
        resolver._read = function (tap) {
          return new Branch(typeResolver._read(tap));
        };
      } else {
        resolver._read = function () { return null; };
      }
      return;
    }
  }

  _copy (val, opts) {
    let wrap = opts && opts.wrap | 0;
    if (wrap === 2) {
      let firstType = this.types[0];
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
      let keys = Object.keys(val);
      if (keys.length === 1) {
        let name = keys[0];
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
    throwInvalidError(val, this);
  }

  compare (val1, val2) {
    let name1 = val1 === null ? 'null' : Object.keys(val1)[0];
    let name2 = val2 === null ? 'null' : Object.keys(val2)[0];
    let index = this._branchIndices[name1];
    if (name1 === name2) {
      return name1 === 'null' ?
        0 :
        this.types[index].compare(val1[name1], val2[name1]);
    } else {
      return utils.compare(index, this._branchIndices[name2]);
    }
  }

  random () {
    let index = RANDOM.nextInt(this.types.length);
    let type = this.types[index];
    let Branch = type._branchConstructor;
    if (!Branch) {
      return null;
    }
    return new Branch(type.random());
  }
}

WrappedUnionType.prototype.typeName = 'union:wrapped';

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
  constructor (schema, opts) {
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

  _check (val, flags, hook) {
    let b = this._indices[val] !== undefined;
    if (!b && hook) {
      hook(val, this);
    }
    return b;
  }

  _read (tap) {
    let index = tap.readLong();
    let symbol = this.symbols[index];
    if (symbol === undefined) {
      throw new Error(`invalid ${this.name} enum index: ${index}`);
    }
    return symbol;
  }

  _skip (tap) { tap.skipLong(); }

  _write (tap, val) {
    let index = this._indices[val];
    if (index === undefined) {
      throwInvalidError(val, this);
    }
    tap.writeLong(index);
  }

  _match (tap1, tap2) {
    return tap1.matchLong(tap2);
  }

  compare (val1, val2) {
    return utils.compare(this._indices[val1], this._indices[val2]);
  }

  _update (resolver, type, opts) {
    let symbols = this.symbols;
    if (
      type.typeName === 'enum' &&
      hasCompatibleName(this, type, !opts.ignoreNamespaces) &&
      (
        type.symbols.every((s) => { return ~symbols.indexOf(s); }) ||
        this.default !== undefined
      )
    ) {
      resolver.symbols = type.symbols.map(function (s) {
        return this._indices[s] === undefined ? this.default : s;
      }, this);
      resolver._read = type._read;
    }
  }

  _copy (val) {
    this._check(val, undefined, throwInvalidError);
    return val;
  }

  _deref (schema) {
    schema.symbols = this.symbols;
  }

  getSymbols () { return this.symbols; }

  random () {
    return RANDOM.choice(this.symbols);
  }
}

EnumType.prototype.typeName = 'enum';

/** Avro fixed type. Represented simply as a `Buffer`. */
class FixedType extends Type {
  constructor (schema, opts) {
    super(schema, opts);
    if (schema.size !== (schema.size | 0) || schema.size < 0) {
      throw new Error(`invalid ${this.branchName} size`);
    }
    this.size = schema.size | 0;
    this._branchConstructor = this._createBranchConstructor();
    Object.freeze(this);
  }

  _check (val, flags, hook) {
    let b = isBufferLike(val) && val.length === this.size;
    if (!b && hook) {
      hook(val, this);
    }
    return b;
  }

  _read (tap) {
    return tap.readFixed(this.size);
  }

  _skip (tap) {
    tap.skipFixed(this.size);
  }

  _write (tap, val) {
    if (!isBufferLike(val) || val.length !== this.size) {
      throwInvalidError(val, this);
    }
    tap.writeFixed(val, this.size);
  }

  _match (tap1, tap2) {
    return tap1.matchFixed(tap2, this.size);
  }

  _update (resolver, type, opts) {
    if (
      type.typeName === 'fixed' &&
      this.size === type.size &&
      hasCompatibleName(this, type, !opts.ignoreNamespaces)
    ) {
      resolver.size = this.size;
      resolver._read = this._read;
    }
  }

  _deref (schema) { schema.size = this.size; }

  getSize () { return this.size; }

  random () {
    return RANDOM.nextBuffer(this.size);
  }
}

FixedType.prototype._copy = BytesType.prototype._copy;

FixedType.prototype.compare = utils.bufCompare;

FixedType.prototype.typeName = 'fixed';

/** Avro map. Represented as vanilla objects. */
class MapType extends Type {
  constructor (schema, opts) {
    super();
    if (!schema.values) {
      throw new Error(`missing map values: ${j(schema)}`);
    }
    this.valuesType = Type.forSchema(schema.values, opts);
    this._branchConstructor = this._createBranchConstructor();
    Object.freeze(this);
  }

  _check (val, flags, hook, path) {
    if (!val || typeof val != 'object' || Array.isArray(val)) {
      if (hook) {
        hook(val, this);
      }
      return false;
    }

    let keys = Object.keys(val);
    let b = true;
    if (hook) {
      // Slow path.
      let j = path.length;
      path.push('');
      for (let i = 0, l = keys.length; i < l; i++) {
        let key = path[j] = keys[i];
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

  _read (tap) {
    let values = this.valuesType;
    let val = {};
    let n;
    while ((n = readArraySize(tap))) {
      while (n--) {
        let key = tap.readString();
        val[key] = values._read(tap);
      }
    }
    return val;
  }

  _skip (tap) {
    let values = this.valuesType;
    let n;
    while ((n = tap.readLong())) {
      if (n < 0) {
        let len = tap.readLong();
        tap.pos += len;
      } else {
        while (n--) {
          tap.skipString();
          values._skip(tap);
        }
      }
    }
  }

  _write (tap, val) {
    if (!val || typeof val != 'object' || Array.isArray(val)) {
      throwInvalidError(val, this);
    }

    let values = this.valuesType;
    let keys = Object.keys(val);
    let n = keys.length;
    if (n) {
      tap.writeLong(n);
      for (let i = 0; i < n; i++) {
        let key = keys[i];
        tap.writeString(key);
        values._write(tap, val[key]);
      }
    }
    tap.writeLong(0);
  }

  _match () {
    throw new Error('maps cannot be compared');
  }

  _update (rsv, type, opts) {
    if (type.typeName === 'map') {
      rsv.valuesType = this.valuesType.createResolver(type.valuesType, opts);
      rsv._read = this._read;
    }
  }

  _copy (val, opts) {
    if (val && typeof val == 'object' && !Array.isArray(val)) {
      let values = this.valuesType;
      let keys = Object.keys(val);
      let copy = {};
      for (let i = 0, l = keys.length; i < l; i++) {
        let key = keys[i];
        copy[key] = values._copy(val[key], opts);
      }
      return copy;
    }
    throwInvalidError(val, this);
  }

  getValuesType () { return this.valuesType; }

  random () {
    let val = {};
    for (let i = 0, l = RANDOM.nextInt(10); i < l; i++) {
      val[RANDOM.nextString(RANDOM.nextInt(20))] = this.valuesType.random();
    }
    return val;
  }

  _deref (schema, derefed, opts) {
    schema.values = this.valuesType._attrs(derefed, opts);
  }
}

MapType.prototype.compare = MapType.prototype._match;

MapType.prototype.typeName = 'map';

/** Avro array. Represented as vanilla arrays. */
class ArrayType extends Type {
  constructor (schema, opts) {
    super();
    if (!schema.items) {
      throw new Error(`missing array items: ${j(schema)}`);
    }
    this.itemsType = Type.forSchema(schema.items, opts);
    this._branchConstructor = this._createBranchConstructor();
    Object.freeze(this);
  }

  _check (val, flags, hook, path) {
    if (!Array.isArray(val)) {
      if (hook) {
        hook(val, this);
      }
      return false;
    }
    let items = this.itemsType;
    let b = true;
    if (hook) {
      // Slow path.
      let j = path.length;
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

  _read (tap) {
    let items = this.itemsType;
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

  _skip (tap) {
    let items = this.itemsType;
    let n;
    while ((n = tap.readLong())) {
      if (n < 0) {
        let len = tap.readLong();
        tap.pos += len;
      } else {
        while (n--) {
          items._skip(tap);
        }
      }
    }
  }

  _write (tap, val) {
    if (!Array.isArray(val)) {
      throwInvalidError(val, this);
    }
    let items = this.itemsType;
    let n = val.length;
    if (n) {
      tap.writeLong(n);
      for (let i = 0; i < n; i++) {
        items._write(tap, val[i]);
      }
    }
    tap.writeLong(0);
  }

  _match (tap1, tap2) {
    let n1 = tap1.readLong();
    let n2 = tap2.readLong();
    while (n1 && n2) {
      let f = this.itemsType._match(tap1, tap2);
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

  _update (resolver, type, opts) {
    if (type.typeName === 'array') {
      resolver.itemsType = this.itemsType.createResolver(type.itemsType, opts);
      resolver._read = this._read;
    }
  }

  _copy (val, opts) {
    if (!Array.isArray(val)) {
      throwInvalidError(val, this);
    }
    let items = new Array(val.length);
    for (let i = 0, l = val.length; i < l; i++) {
      items[i] = this.itemsType._copy(val[i], opts);
    }
    return items;
  }

  _deref (schema, derefed, opts) {
    schema.items = this.itemsType._attrs(derefed, opts);
  }

  compare (val1, val2) {
    let n1 = val1.length;
    let n2 = val2.length;
    let f;
    for (let i = 0, l = Math.min(n1, n2); i < l; i++) {
      if ((f = this.itemsType.compare(val1[i], val2[i]))) {
        return f;
      }
    }
    return utils.compare(n1, n2);
  }

  getItemsType () { return this.itemsType; }

  random () {
    let arr = [];
    for (let i = 0, l = RANDOM.nextInt(10); i < l; i++) {
      arr.push(this.itemsType.random());
    }
    return arr;
  }
}

ArrayType.prototype.typeName = 'array';

/**
 * Avro record.
 *
 * Values are represented as instances of a programmatically generated
 * constructor (similar to a "specific record"), available via the
 * `getRecordConstructor` method. This "specific record class" gives
 * significant speedups over using generics objects.
 *
 * Note that vanilla objects are still accepted as valid as long as their
 * fields match (this makes it much more convenient to do simple things like
 * update nested records).
 *
 * This type is also used for errors (similar, except for the extra `Error`
 * constructor call) and for messages (see comment below).
 */
class RecordType extends Type {
  constructor (schema, opts) {
    opts = Object.assign({}, opts);

    if (schema.namespace !== undefined) {
      opts.namespace = schema.namespace;
    } else if (schema.name) {
      // Fully qualified names' namespaces are used when no explicit namespace
      // attribute was specified.
      let ns = utils.impliedNamespace(schema.name);
      if (ns !== undefined) {
        opts.namespace = ns;
      }
    }
    super(schema, opts);

    if (!Array.isArray(schema.fields)) {
      throw new Error(`non-array record fields: ${j(schema.fields)}`);
    }
    if (utils.hasDuplicates(schema.fields, (f) => { return f.name; })) {
      throw new Error(`duplicate field name:${j(schema.fields)}`);
    }
    this._fieldsByName = {};
    this.fields = Object.freeze(schema.fields.map(function (f) {
      let field = new Field(f, opts);
      this._fieldsByName[field.name] = field;
      return field;
    }, this));
    this._branchConstructor = this._createBranchConstructor();
    this._isError = schema.type === 'error';
    this.recordConstructor = this._createConstructor(
      opts.errorStackTraces,
      opts.omitRecordMethods
    );
    this._read = this._createReader();
    this._skip = this._createSkipper();
    this._write = this._createWriter();
    this._check = this._createChecker();

    Object.freeze(this);
  }

  _getConstructorName () {
    return this.name ?
      utils.capitalize(utils.unqualify(this.name)) :
      this._isError ? 'Error$' : 'Record$';
  }

  _createConstructor (errorStack, plainRecords) {
    let outerArgs = [];
    let innerArgs = [];
    let ds = []; // Defaults.
    let innerBody = '';
    let stackField;
    for (let i = 0, l = this.fields.length; i < l; i++) {
      let field = this.fields[i];
      let defaultValue = field.defaultValue;
      let hasDefault = defaultValue() !== undefined;
      let name = field.name;
      if (
        errorStack && this._isError && name === 'stack' &&
        Type.isType(field.type, 'string') && !hasDefault
      ) {
        // We keep track of whether we've encountered a valid stack field (in
        // particular, without a default) to populate a stack trace below.
        stackField = field;
      }
      innerArgs.push('v' + i);
      innerBody += '  ';
      if (!hasDefault) {
        innerBody += 'this.' + name + ' = v' + i + ';\n';
      } else {
        innerBody += 'if (v' + i + ' === undefined) { ';
        innerBody += 'this.' + name + ' = d' + ds.length + '(); ';
        innerBody += '} else { this.' + name + ' = v' + i + '; }\n';
        outerArgs.push('d' + ds.length);
        ds.push(defaultValue);
      }
    }
    if (stackField) {
      // We should populate a stack trace.
      innerBody += '  if (this.stack === undefined) { ';
      /* istanbul ignore else */
      if (typeof Error.captureStackTrace == 'function') {
        // v8 runtimes, the easy case.
        innerBody += 'Error.captureStackTrace(this, this.constructor);';
      } else {
        // A few other runtimes (e.g. SpiderMonkey), might not work everywhere.
        innerBody += 'this.stack = Error().stack;';
      }
      innerBody += ' }\n';
    }
    let outerBody = 'return function ' + this._getConstructorName() + '(';
    outerBody += innerArgs.join() + ') {\n' + innerBody + '};';
    // eslint-disable-next-line no-new-func
    let Record = new Function(outerArgs.join(), outerBody).apply(undefined, ds);
    if (plainRecords) {
      return Record;
    }

    let self = this;
    Record.getType = function () { return self; };
    Record.type = self;
    if (this._isError) {
      Record.prototype = Object.create(Error.prototype, {
        constructor: {
          value: Record,
          enumerable: false,
          writable: true,
          configurable: true
        }
      });
      Record.prototype.name = this._getConstructorName();
    }
    Record.prototype.clone = function (o) { return self.clone(this, o); };
    Record.prototype.compare = function (v) { return self.compare(this, v); };
    Record.prototype.isValid = function (o) { return self.isValid(this, o); };
    Record.prototype.toBuffer = function () { return self.toBuffer(this); };
    Record.prototype.toString = function () { return self.toString(this); };
    Record.prototype.wrap = function () { return self.wrap(this); };
    Record.prototype.wrapped = Record.prototype.wrap; // Deprecated.
    return Record;
  }

  _createChecker () {
    let names = [];
    let values = [];
    let name = this._getConstructorName();
    let body = 'return function check' + name + '(v, f, h, p) {\n';
    body += '  if (\n';
    body += '    v === null ||\n';
    body += '    typeof v != \'object\' ||\n';
    body += '    (f && !this._checkFields(v))\n';
    body += '  ) {\n';
    body += '    if (h) { h(v, this); }\n';
    body += '    return false;\n';
    body += '  }\n';
    if (!this.fields.length) {
      // Special case, empty record. We handle this directly.
      body += '  return true;\n';
    } else {
      let field;
      for (let i = 0, l = this.fields.length; i < l; i++) {
        field = this.fields[i];
        names.push('t' + i);
        values.push(field.type);
        if (field.defaultValue() !== undefined) {
          body += '  var v' + i + ' = v.' + field.name + ';\n';
        }
      }
      body += '  if (h) {\n';
      body += '    var b = 1;\n';
      body += '    var j = p.length;\n';
      body += '    p.push(\'\');\n';
      for (let i = 0, l = this.fields.length; i < l; i++) {
        field = this.fields[i];
        body += '    p[j] = \'' + field.name + '\';\n';
        body += '    b &= ';
        if (field.defaultValue() === undefined) {
          body += 't' + i + '._check(v.' + field.name + ', f, h, p);\n';
        } else {
          body += 'v' + i + ' === undefined || ';
          body += 't' + i + '._check(v' + i + ', f, h, p);\n';
        }
      }
      body += '    p.pop();\n';
      body += '    return !!b;\n';
      body += '  } else {\n    return (\n      ';
      body += this.fields.map((field, i) => {
        return field.defaultValue() === undefined ?
          't' + i + '._check(v.' + field.name + ', f)' :
          '(v' + i + ' === undefined || t' + i + '._check(v' + i + ', f))';
      }).join(' &&\n      ');
      body += '\n    );\n  }\n';
    }
    body += '};';
    // eslint-disable-next-line no-new-func
    return new Function(names.join(), body).apply(undefined, values);
  }

  _createReader () {
    let names = [];
    let values = [this.recordConstructor];
    for (let i = 0, l = this.fields.length; i < l; i++) {
      names.push('t' + i);
      values.push(this.fields[i].type);
    }
    let name = this._getConstructorName();
    let body = 'return function read' + name + '(t) {\n';
    body += '  return new ' + name + '(\n    ';
    body += names.map((s) => { return s + '._read(t)'; }).join(',\n    ');
    body += '\n  );\n};';
    names.unshift(name);
    // We can do this since the JS spec guarantees that function arguments are
    // evaluated from left to right.
    // eslint-disable-next-line no-new-func
    return new Function(names.join(), body).apply(undefined, values);
  }

  _createSkipper () {
    let args = [];
    let body = 'return function skip' + this._getConstructorName() + '(t) {\n';
    let values = [];
    for (let i = 0, l = this.fields.length; i < l; i++) {
      args.push('t' + i);
      values.push(this.fields[i].type);
      body += '  t' + i + '._skip(t);\n';
    }
    body += '}';
    // eslint-disable-next-line no-new-func
    return new Function(args.join(), body).apply(undefined, values);
  }

  _createWriter () {
    // We still do default handling here, in case a normal JS object is passed.
    let args = [];
    let name = this._getConstructorName();
    let body = 'return function write' + name + '(t, v) {\n';
    let values = [];
    for (let i = 0, l = this.fields.length; i < l; i++) {
      let field = this.fields[i];
      args.push('t' + i);
      values.push(field.type);
      body += '  ';
      if (field.defaultValue() === undefined) {
        body += 't' + i + '._write(t, v.' + field.name + ');\n';
      } else {
        let value = field.type.toBuffer(field.defaultValue());
        args.push('d' + i);
        values.push(value);
        body += 'var v' + i + ' = v.' + field.name + ';\n';
        body += 'if (v' + i + ' === undefined) {\n';
        body += '    t.writeFixed(d' + i + ', ' + value.length + ');\n';
        body += '  } else {\n    t' + i + '._write(t, v' + i + ');\n  }\n';
      }
    }
    body += '}';
    // eslint-disable-next-line no-new-func
    return new Function(args.join(), body).apply(undefined, values);
  }

  _update (resolver, type, opts) {
    if (!hasCompatibleName(this, type, !opts.ignoreNamespaces)) {
      throw new Error(`no alias found for ${type.name}`);
    }

    let rFields = this.fields;
    let wFields = type.fields;
    let wFieldsMap = utils.toMap(wFields, (f) => { return f.name; });

    let innerArgs = []; // Arguments for reader constructor.
    let resolvers = {}; // Resolvers keyed by writer field name.
    for (let i = 0; i < rFields.length; i++) {
      let field = rFields[i];
      let names = getAliases(field);
      let matches = [];
      for (let j = 0; j < names.length; j++) {
        let name = names[j];
        if (wFieldsMap[name]) {
          matches.push(name);
        }
      }
      if (matches.length > 1) {
        throw new Error(
          `ambiguous aliasing for ${type.name}.${field.name} (${matches.join(', ')})`
        );
      }
      if (!matches.length) {
        if (field.defaultValue() === undefined) {
          throw new Error(
            `no matching field for default-less ${type.name}.${field.name}`
          );
        }
        innerArgs.push('undefined');
      } else {
        let name = matches[0];
        let fieldResolver = {
          resolver: field.type.createResolver(wFieldsMap[name].type, opts),
          name: '_' + field.name, // Reader field name.
        };
        if (!resolvers[name]) {
          resolvers[name] = [fieldResolver];
        } else {
          resolvers[name].push(fieldResolver);
        }
        innerArgs.push(fieldResolver.name);
      }
    }

    // See if we can add a bypass for unused fields at the end of the record.
    let lazyIndex = -1;
    let i = wFields.length;
    while (i && resolvers[wFields[--i].name] === undefined) {
      lazyIndex = i;
    }

    let uname = this._getConstructorName();
    let args = [uname];
    let values = [this.recordConstructor];
    let body = '  return function read' + uname + '(t, b) {\n';
    for (let i = 0; i < wFields.length; i++) {
      if (i === lazyIndex) {
        body += '  if (!b) {\n';
      }
      let field = type.fields[i];
      let name = field.name;
      if (resolvers[name] === undefined) {
        body += (~lazyIndex && i >= lazyIndex) ? '    ' : '  ';
        args.push('r' + i);
        values.push(field.type);
        body += 'r' + i + '._skip(t);\n';
      } else {
        let j = resolvers[name].length;
        while (j--) {
          body += (~lazyIndex && i >= lazyIndex) ? '    ' : '  ';
          args.push('r' + i + 'f' + j);
          let fieldResolver = resolvers[name][j];
          values.push(fieldResolver.resolver);
          body += 'var ' + fieldResolver.name + ' = ';
          body += 'r' + i + 'f' + j + '._' + (j ? 'peek' : 'read') + '(t);\n';
        }
      }
    }
    if (~lazyIndex) {
      body += '  }\n';
    }
    body += '  return new ' + uname + '(' + innerArgs.join() + ');\n};';

    // eslint-disable-next-line no-new-func
    resolver._read = new Function(args.join(), body).apply(undefined, values);
  }

  _match (tap1, tap2) {
    let fields = this.fields;
    for (let i = 0, l = fields.length; i < l; i++) {
      let field = fields[i];
      let order = field._order;
      let type = field.type;
      if (order) {
        order *= type._match(tap1, tap2);
        if (order) {
          return order;
        }
      } else {
        type._skip(tap1);
        type._skip(tap2);
      }
    }
    return 0;
  }

  _checkFields (obj) {
    let keys = Object.keys(obj);
    for (let i = 0, l = keys.length; i < l; i++) {
      if (!this._fieldsByName[keys[i]]) {
        return false;
      }
    }
    return true;
  }

  _copy (val, opts) {
    let hook = opts && opts.fieldHook;
    let values = [undefined];
    for (let i = 0, l = this.fields.length; i < l; i++) {
      let field = this.fields[i];
      let value = val[field.name];
      if (value === undefined &&
          Object.prototype.hasOwnProperty.call(field, 'defaultValue')) {
        value = field.defaultValue();
      }
      if ((opts && !opts.skip) || value !== undefined) {
        value = field.type._copy(value, opts);
      }
      if (hook) {
        value = hook(field, value, this);
      }
      values.push(value);
    }
    let Record = this.recordConstructor;
    return new (Record.bind.apply(Record, values))();
  }

  _deref (schema, derefed, opts) {
    schema.fields = this.fields.map((field) => {
      let fieldType = field.type;
      let fieldSchema = {
        name: field.name,
        type: fieldType._attrs(derefed, opts)
      };
      if (opts.exportAttrs) {
        let val = field.defaultValue();
        if (val !== undefined) {
          // We must both unwrap all unions and coerce buffers to strings.
          fieldSchema['default'] = fieldType._copy(val, {coerce: 3, wrap: 3});
        }
        let fieldOrder = field.order;
        if (fieldOrder !== 'ascending') {
          fieldSchema.order = fieldOrder;
        }
        let fieldAliases = field.aliases;
        if (fieldAliases.length) {
          fieldSchema.aliases = fieldAliases;
        }
        let fieldDoc = field.doc;
        if (fieldDoc !== undefined) {
          fieldSchema.doc = fieldDoc;
        }
      }
      return fieldSchema;
    });
  }

  compare (val1, val2) {
    let fields = this.fields;
    for (let i = 0, l = fields.length; i < l; i++) {
      let field = fields[i];
      let name = field.name;
      let order = field._order;
      let type = field.type;
      if (order) {
        order *= type.compare(val1[name], val2[name]);
        if (order) {
          return order;
        }
      }
    }
    return 0;
  }

  random () {
    let fields = this.fields.map((f) => { return f.type.random(); });
    fields.unshift(undefined);
    let Record = this.recordConstructor;
    return new (Record.bind.apply(Record, fields))();
  }

  field (name) {
    return this._fieldsByName[name];
  }

  getField (name) {
    return this._fieldsByName[name];
  }

  getFields () { return this.fields; }

  getRecordConstructor () {
    return this.recordConstructor;
  }

  get typeName () { return this._isError ? 'error' : 'record'; }
}

/** Derived type abstract class. */
class LogicalType extends Type {
  constructor (schema, opts) {
    super();
    this._logicalTypeName = schema.logicalType;
    LOGICAL_TYPE = this;
    try {
      this._underlyingType = Type.forSchema(schema, opts);
    } finally {
      LOGICAL_TYPE = null;
      // Remove the underlying type now that we're done instantiating. Note that
      // in some (rare) cases, it might not have been inserted; for example, if
      // this constructor was manually called with an already instantiated type.
      let l = UNDERLYING_TYPES.length;
      if (l && UNDERLYING_TYPES[l - 1][0] === this) {
        UNDERLYING_TYPES.pop();
      }
    }
    // We create a separate branch constructor for logical types to keep them
    // monomorphic.
    if (Type.isType(this.underlyingType, 'union')) {
      this._branchConstructor = this.underlyingType._branchConstructor;
    } else {
      this._branchConstructor = this.underlyingType._createBranchConstructor();
    }
    // We don't freeze derived types to allow arbitrary properties. Implementors
    // can still do so in the subclass' constructor at their convenience.
  }

  get typeName () { return 'logical:' + this._logicalTypeName; }

  get underlyingType () {
    if (this._underlyingType) {
      return this._underlyingType;
    }
    // If the field wasn't present, it means the logical type isn't complete
    // yet: we're waiting on its underlying type to be fully instantiated. In
    // this case, it will be present in the `UNDERLYING_TYPES` array.
    for (let i = 0, l = UNDERLYING_TYPES.length; i < l; i++) {
      let arr = UNDERLYING_TYPES[i];
      if (arr[0] === this) {
        return arr[1];
      }
    }
    return undefined;
  }

  getUnderlyingType () {
    return this.underlyingType;
  }

  _read (tap) {
    return this._fromValue(this.underlyingType._read(tap));
  }

  _write (tap, any) {
    this.underlyingType._write(tap, this._toValue(any));
  }

  _check (any, flags, hook, path) {
    let val;
    try {
      val = this._toValue(any);
    } catch (err) {
      // Handled below.
    }
    if (val === undefined) {
      if (hook) {
        hook(any, this);
      }
      return false;
    }
    return this.underlyingType._check(val, flags, hook, path);
  }

  _copy (any, opts) {
    let type = this.underlyingType;
    switch (opts && opts.coerce) {
      case 3: // To string.
        return type._copy(this._toValue(any), opts);
      case 2: // From string.
        return this._fromValue(type._copy(any, opts));
      default: // Normal copy.
        return this._fromValue(type._copy(this._toValue(any), opts));
    }
  }

  _update (resolver, type, opts) {
    let _fromValue = this._resolve(type, opts);
    if (_fromValue) {
      resolver._read = function (tap) { return _fromValue(type._read(tap)); };
    }
  }

  compare (obj1, obj2) {
    let val1 = this._toValue(obj1);
    let val2 = this._toValue(obj2);
    return this.underlyingType.compare(val1, val2);
  }

  random () {
    return this._fromValue(this.underlyingType.random());
  }

  _deref (schema, derefed, opts) {
    let type = this.underlyingType;
    let isVisited = type.name !== undefined && derefed[type.name];
    schema = type._attrs(derefed, opts);
    if (!isVisited && opts.exportAttrs) {
      if (typeof schema == 'string') {
        schema = {type: schema};
      }
      schema.logicalType = this._logicalTypeName;
      this._export(schema);
    }
    return schema;
  }

  _skip (tap) {
    this.underlyingType._skip(tap);
  }

  // Unlike the other methods below, `_export` has a reasonable default which we
  // can provide (not exporting anything).
  _export (/* schema */) {}

  // Methods to be implemented.
  _fromValue () { utils.abstractFunction(); }
  _toValue () { utils.abstractFunction(); }
  _resolve () { utils.abstractFunction(); }
}



// General helpers.

/**
 * Customizable long.
 *
 * This allows support of arbitrarily large long (e.g. larger than
 * `Number.MAX_SAFE_INTEGER`). See `LongType.__with` method above. Note that we
 * can't use a logical type because we need a "lower-level" hook here: passing
 * through through the standard long would cause a loss of precision.
 */
class AbstractLongType extends LongType {
  constructor (noUnpack) {
    super(true);
    this._noUnpack = !!noUnpack;
  }

  _check (val, flags, hook) {
    let b = this._isValid(val);
    if (!b && hook) {
      hook(val, this);
    }
    return b;
  }

  _read (tap) {
    let buf;
    if (this._noUnpack) {
      let pos = tap.pos;
      tap.skipLong();
      buf = tap.subarray(pos, tap.pos);
    } else {
      buf = tap.unpackLongBytes(tap);
    }
    if (tap.isValid()) {
      return this._fromBuffer(buf);
    }
  }

  _write (tap, val) {
    if (!this._isValid(val)) {
      throwInvalidError(val, this);
    }
    let buf = this._toBuffer(val);
    if (this._noUnpack) {
      tap.writeFixed(buf);
    } else {
      tap.packLongBytes(buf);
    }
  }

  _copy (val, opts) {
    switch (opts && opts.coerce) {
      case 3: // To string.
        return this._toJSON(val);
      case 2: // From string.
        return this._fromJSON(val);
      default: // Normal copy.
        // Slow but guarantees most consistent results. Faster alternatives
        // would require assumptions on the long class used (e.g. immutability).
        return this._fromJSON(this._toJSON(val));
    }
  }

  _deref () { return 'long'; }

  _update (resolver, type) {
    let self = this;
    switch (type.typeName) {
      case 'int':
        resolver._read = function (tap) {
          return self._fromJSON(type._read(tap));
        };
        break;
      case 'abstract:long':
      case 'long':
        resolver._read = function (tap) { return self._read(tap); };
    }
  }

  random () {
    return this._fromJSON(LongType.prototype.random());
  }

  // Methods to be implemented by the user.
  _fromBuffer () { utils.abstractFunction(); }
  _toBuffer () { utils.abstractFunction(); }
  _fromJSON () { utils.abstractFunction(); }
  _toJSON () { utils.abstractFunction(); }
  _isValid () { utils.abstractFunction(); }
  compare () { utils.abstractFunction(); }
}

AbstractLongType.prototype.typeName = 'abstract:long';
// Must be defined *before* calling the constructor
AbstractLongType.prototype._concreteTypeName = 'long';

/** A record field. */
class Field {
  constructor (schema, opts) {
    let name = schema.name;
    if (typeof name != 'string' || !utils.isValidName(name)) {
      throw new Error(`invalid field name: ${name}`);
    }

    this.name = name;
    this.type = Type.forSchema(schema.type, opts);
    this.aliases = schema.aliases || [];
    this.doc = schema.doc !== undefined ? '' + schema.doc : undefined;

    this._order = (function (order) {
      switch (order) {
        case 'ascending':
          return 1;
        case 'descending':
          return -1;
        case 'ignore':
          return 0;
        default:
          throw new Error(`invalid order: ${j(order)}`);
      }
    })(schema.order === undefined ? 'ascending' : schema.order);

    let value = schema['default'];
    if (value !== undefined) {
      // We need to convert defaults back to a valid format (unions are
      // disallowed in default definitions, only the first type of each union is
      // allowed instead).
      // http://apache-avro.679487.n3.nabble.com/field-union-default-in-Java-td1175327.html
      let type = this.type;
      let val;
      try {
        val = type._copy(value, {coerce: 2, wrap: 2});
      } catch (err) {
        let msg = `incompatible field default ${j(value)} (${err.message})`;
        if (Type.isType(type, 'union')) {
          let t = j(type.types[0]);
          msg += `, union defaults must match the first branch's type (${t})`;
        }
        throw new Error(msg);
      }
      // The clone call above will throw an error if the default is invalid.
      if (isPrimitive(type.typeName) && type.typeName !== 'bytes') {
        // These are immutable.
        this.defaultValue = function () { return val; };
      } else {
        this.defaultValue = function () { return type._copy(val); };
      }
    }

    Object.freeze(this);
  }

  defaultValue () {} // Undefined default.

  getDefault () {}

  getAliases () { return this.aliases; }

  getName () { return this.name; }

  getOrder () { return this.order; }

  getType () { return this.type; }

  get order () {
    return ['descending', 'ignore', 'ascending'][this._order + 1];
  }
}

/**
 * Resolver to read a writer's schema as a new schema.
 *
 * @param readerType {Type} The type to convert to.
 */
class Resolver {
  constructor (readerType) {
    // Add all fields here so that all resolvers share the same hidden class.
    this._readerType = readerType;
    this._read = null;
    this.itemsType = null;
    this.size = 0;
    this.symbols = null;
    this.valuesType = null;
  }

  inspect () { return '<Resolver>'; }
}

Resolver.prototype._peek = Type.prototype._peek;

/** Mutable hash container. */
class Hash {
  constructor () {
    this.hash = undefined;
  }
}

/**
 * Read a value from a tap.
 *
 * @param type {Type} The type to decode.
 * @param tap {Tap} The tap to read from. No checks are performed here.
 * @param resolver {Resolver} Optional resolver. It must match the input type.
 * @param lazy {Boolean} Skip trailing fields when using a resolver.
 */
function readValue(type, tap, resolver, lazy) {
  if (resolver) {
    if (resolver._readerType !== type) {
      throw new Error('invalid resolver');
    }
    return resolver._read(tap, lazy);
  } else {
    return type._read(tap);
  }
}

/**
 * Get all aliases for a type (including its name).
 *
 * @param obj {Type|Object} Typically a type or a field. Its aliases property
 * must exist and be an array.
 */
function getAliases(obj) {
  let names = {};
  if (obj.name) {
    names[obj.name] = true;
  }
  let aliases = obj.aliases;
  for (let i = 0, l = aliases.length; i < l; i++) {
    names[aliases[i]] = true;
  }
  return Object.keys(names);
}

/** Checks if a type can be read as another based on name resolution rules. */
function hasCompatibleName(reader, writer, strict) {
  if (!writer.name) {
    return true;
  }
  let name = strict ? writer.name : utils.unqualify(writer.name);
  let aliases = getAliases(reader);
  for (let i = 0, l = aliases.length; i < l; i++) {
    let alias = aliases[i];
    if (!strict) {
      alias = utils.unqualify(alias);
    }
    if (alias === name) {
      return true;
    }
  }
  return false;
}

/**
 * Check whether a type's name is a primitive.
 *
 * @param name {String} Type name (e.g. `'string'`, `'array'`).
 */
function isPrimitive(typeName) {
  // Since we use this module's own `TYPES` object, we can use `instanceof`.
  let type = TYPES[typeName];
  return type && type.prototype instanceof PrimitiveType;
}

/**
 * Return a type's class name from its Avro type name.
 *
 * We can't simply use `constructor.name` since it isn't supported in all
 * browsers.
 *
 * @param typeName {String} Type name.
 */
function getClassName(typeName) {
  if (typeName === 'error') {
    typeName = 'record';
  } else {
    let match = /^([^:]+):(.*)$/.exec(typeName);
    if (match) {
      if (match[1] === 'union') {
        typeName = match[2] + 'Union';
      } else {
        // Logical type.
        typeName = match[1];
      }
    }
  }
  return utils.capitalize(typeName) + 'Type';
}

/**
 * Get the number of elements in an array block.
 *
 * @param tap {Tap} A tap positioned at the beginning of an array block.
 */
function readArraySize(tap) {
  let n = tap.readLong();
  if (n < 0) {
    n = -n;
    tap.skipLong(); // Skip size.
  }
  return n;
}

/**
 * Check whether a long can be represented without precision loss.
 *
 * @param n {Number} The number.
 *
 * Two things to note:
 *
 * + We are not using the `Number` constants for compatibility with older
 *   browsers.
 * + We divide the bounds by two to avoid rounding errors during zigzag encoding
 *   (see https://github.com/mtth/avsc/issues/455).
 */
function isSafeLong(n) {
  return n >= -4503599627370496 && n <= 4503599627370496;
}

/**
 * Check whether an object is the JSON representation of a buffer.
 */
function isJsonBuffer(obj) {
  return obj && obj.type === 'Buffer' && Array.isArray(obj.data);
}

/**
 * Throw a somewhat helpful error on invalid object.
 *
 * @param path {Array} Passed from hook, but unused (because empty where this
 * function is used, since we aren't keeping track of it for effiency).
 * @param val {...} The object to reject.
 * @param type {Type} The type to check against.
 *
 * This method is mostly used from `_write` to signal an invalid object for a
 * given type. Note that this provides less information than calling `isValid`
 * with a hook since the path is not propagated (for efficiency reasons).
 */
function throwInvalidError(val, type) {
  throw new Error(`invalid ${j(type.schema())}: ${j(val)}`);
}

function maybeQualify(name, ns) {
  let unqualified = utils.unqualify(name);
  // Primitives are always in the global namespace.
  return isPrimitive(unqualified) ? unqualified : utils.qualify(name, ns);
}

/**
 * Get a type's bucket when included inside an unwrapped union.
 *
 * @param type {Type} Any type.
 */
function getTypeBucket(type) {
  let typeName = type.typeName;
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

/**
 * Infer a value's bucket (see unwrapped unions for more details).
 *
 * @param val {...} Any value.
 */
function getValueBucket(val) {
  if (val === null) {
    return 'null';
  }
  let bucket = typeof val;
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

/**
 * Check whether a collection of types leads to an ambiguous union.
 *
 * @param types {Array} Array of types.
 */
function isAmbiguous(types) {
  let buckets = {};
  for (let i = 0, l = types.length; i < l; i++) {
    let type = types[i];
    if (!Type.isType(type, 'logical')) {
      let bucket = getTypeBucket(type);
      if (buckets[bucket]) {
        return true;
      }
      buckets[bucket] = true;
    }
  }
  return false;
}

/**
 * Combine number types.
 *
 * Note that never have to create a new type here, we are guaranteed to be able
 * to reuse one of the input types as super-type.
 */
function combineNumbers(types) {
  let typeNames = ['int', 'long', 'float', 'double'];
  let superIndex = -1;
  let superType = null;
  for (let i = 0, l = types.length; i < l; i++) {
    let type = types[i];
    let index = typeNames.indexOf(type.typeName);
    if (index > superIndex) {
      superIndex = index;
      superType = type;
    }
  }
  return superType;
}

/**
 * Combine enums and strings.
 *
 * The order of the returned symbols is undefined and the returned enum is
 *
 */
function combineStrings(types, opts) {
  let symbols = {};
  for (let i = 0, l = types.length; i < l; i++) {
    let type = types[i];
    if (type.typeName === 'string') {
      // If at least one of the types is a string, it will be the supertype.
      return type;
    }
    let typeSymbols = type.symbols;
    for (let j = 0, m = typeSymbols.length; j < m; j++) {
      symbols[typeSymbols[j]] = true;
    }
  }
  return Type.forSchema({type: 'enum', symbols: Object.keys(symbols)}, opts);
}

/**
 * Combine bytes and fixed.
 *
 * This function is optimized to avoid creating new types when possible: in
 * case of a size mismatch between fixed types, it will continue looking
 * through the array to find an existing bytes type (rather than exit early by
 * creating one eagerly).
 */
function combineBuffers(types, opts) {
  let size = -1;
  for (let i = 0, l = types.length; i < l; i++) {
    let type = types[i];
    if (type.typeName === 'bytes') {
      return type;
    }
    if (size === -1) {
      size = type.size;
    } else if (type.size !== size) {
      // Don't create a bytes type right away, we might be able to reuse one
      // later on in the types array. Just mark this for now.
      size = -2;
    }
  }
  return size < 0 ? Type.forSchema('bytes', opts) : types[0];
}

/**
 * Combine maps and records.
 *
 * Field defaults are kept when possible (i.e. when no coercion to a map
 * happens), with later definitions overriding previous ones.
 */
function combineObjects(types, opts) {
  let allTypes = []; // Field and value types.
  let fieldTypes = {}; // Record field types grouped by field name.
  let fieldDefaults = {};
  let isValidRecord = true;

  // Check whether the final type will be a map or a record.
  for (let i = 0, l = types.length; i < l; i++) {
    let type = types[i];
    if (type.typeName === 'map') {
      isValidRecord = false;
      allTypes.push(type.valuesType);
    } else {
      let fields = type.fields;
      for (let j = 0, m = fields.length; j < m; j++) {
        let field = fields[j];
        let fieldName = field.name;
        let fieldType = field.type;
        allTypes.push(fieldType);
        if (isValidRecord) {
          if (!fieldTypes[fieldName]) {
            fieldTypes[fieldName] = [];
          }
          fieldTypes[fieldName].push(fieldType);
          let fieldDefault = field.defaultValue();
          if (fieldDefault !== undefined) {
            // Later defaults will override any previous ones.
            fieldDefaults[fieldName] = fieldDefault;
          }
        }
      }
    }
  }

  let fieldNames;
  if (isValidRecord) {
    // Check that no fields are missing and that we have the approriate
    // defaults for those which are.
    fieldNames = Object.keys(fieldTypes);
    for (let i = 0, l = fieldNames.length; i < l; i++) {
      let fieldName = fieldNames[i];
      if (
        fieldTypes[fieldName].length < types.length &&
        fieldDefaults[fieldName] === undefined
      ) {
        // At least one of the records is missing a field with no default.
        if (opts && opts.strictDefaults) {
          isValidRecord = false;
        } else {
          fieldTypes[fieldName].unshift(Type.forSchema('null', opts));
          fieldDefaults[fieldName] = null;
        }
      }
    }
  }

  let schema;
  if (isValidRecord) {
    schema = {
      type: 'record',
      fields: fieldNames.map((s) => {
        let fieldType = Type.forTypes(fieldTypes[s], opts);
        let fieldDefault = fieldDefaults[s];
        if (
          fieldDefault !== undefined &&
          ~fieldType.typeName.indexOf('union')
        ) {
          // Ensure that the default's corresponding type is first.
          let unionTypes = fieldType.types.slice();
          let i = 0, l = unionTypes.length;
          for (; i < l; i++) {
            if (unionTypes[i].isValid(fieldDefault)) {
              break;
            }
          }
          if (i > 0) {
            let unionType = unionTypes[0];
            unionTypes[0] = unionTypes[i];
            unionTypes[i] = unionType;
            fieldType = Type.forSchema(unionTypes, opts);
          }
        }
        return {
          name: s,
          type: fieldType,
          'default': fieldDefaults[s]
        };
      })
    };
  } else {
    schema = {
      type: 'map',
      values: Type.forTypes(allTypes, opts)
    };
  }
  return Type.forSchema(schema, opts);
}

TYPES = {
  'array': ArrayType,
  'boolean': BooleanType,
  'bytes': BytesType,
  'double': DoubleType,
  'enum': EnumType,
  'error': RecordType,
  'fixed': FixedType,
  'float': FloatType,
  'int': IntType,
  'long': LongType,
  'map': MapType,
  'null': NullType,
  'record': RecordType,
  'string': StringType
};

module.exports = {
  Type,
  getTypeBucket,
  getValueBucket,
  isPrimitive,
  builtins: (function () {
    let types = {
      LogicalType,
      UnwrappedUnionType,
      WrappedUnionType
    };
    let typeNames = Object.keys(TYPES);
    for (let i = 0, l = typeNames.length; i < l; i++) {
      let typeName = typeNames[i];
      types[getClassName(typeName)] = TYPES[typeName];
    }
    return types;
  })()
};
