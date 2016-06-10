/* jshint node: true */

// TODO: Use `toFastProperties` on type reverse indices.
// TODO: Allow configuring when to write the size when writing arrays and maps,
// and customizing their block size.
// TODO: Code-generate `compare` and `clone` record and union methods.

'use strict';

/**
 * This module defines all Avro data types and their serialization logic.
 *
 */

var utils = require('./utils'),
    buffer = require('buffer'), // For `SlowBuffer`.
    util = require('util');


// Convenience imports.
var Tap = utils.Tap;
var f = util.format;

// All non-union concrete (i.e. non-logical) Avro types.
var TYPES = {
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

// Valid (field, type, and symbol) name regex.
var NAME_PATTERN = /^[A-Za-z_][A-Za-z0-9_]*$/;

// Random generator.
var RANDOM = new utils.Lcg();

// Encoding tap (shared for performance).
var TAP = new Tap(new buffer.SlowBuffer(1024));

// Currently active logical type, used for name redirection.
var LOGICAL_TYPE = null;

// Variable used to decide whether to include logical attributes when getting a
// type's schema. A global variable is the simplest way to "pass an argument"
// to JSON stringify's replacer function.
var EXPORT_ATTRS = false;

/**
 * Schema parsing entry point.
 *
 * It isn't exposed directly but called from `parse` inside `index.js` (node)
 * or `avsc.js` (browserify) which each add convenience functionality.
 *
 */
function createType(attrs, opts) {
  if (attrs === null) {
    // Let's be helpful for this common error.
    throw new Error('invalid type: null (did you mean "null"?)');
  }
  if (Type.isType(attrs)) {
    return attrs;
  }

  opts = opts || {};
  opts.registry = opts.registry || {};

  var type;
  if (typeof attrs == 'string') { // Type reference.
    attrs = qualify(attrs, opts.namespace);
    type = opts.registry[attrs];
    if (type) {
      // Type was already defined, return it.
      return type;
    }
    if (isPrimitive(attrs)) {
      // Reference to a primitive type. These are also defined names by default
      // so we create the appropriate type and it to the registry for future
      // reference.
      return opts.registry[attrs] = createType({type: attrs}, opts);
    }
    throw new Error(f('undefined type name: %s', attrs));
  }

  if (opts.typeHook && (type = opts.typeHook(attrs, opts))) {
    if (!Type.isType(type)) {
      throw new Error(f('invalid typehook return value: %j', type));
    }
    return type;
  }

  if (attrs.logicalType && opts.logicalTypes && !LOGICAL_TYPE) {
    var DerivedType = opts.logicalTypes[attrs.logicalType];
    if (DerivedType) {
      var namespace = opts.namespace;
      var registry = {};
      Object.keys(opts.registry).forEach(function (key) {
        registry[key] = opts.registry[key];
      });
      try {
        return new DerivedType(attrs, opts);
      } catch (err) {
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

  if (Array.isArray(attrs)) { // Union.
    var UnionType = opts.wrapUnions ? WrappedUnionType : UnwrappedUnionType;
    type = new UnionType(attrs, opts);
  } else { // New type definition.
    type = (function (typeName) {
      var Type = TYPES[typeName];
      if (Type === undefined) {
        throw new Error(f('unknown type: %j', typeName));
      }
      return new Type(attrs, opts);
    })(attrs.type);
  }
  return type;
}

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
 *
 */
function Type(attrs, opts) {
  var type = LOGICAL_TYPE || this;
  LOGICAL_TYPE = null;

  // Lazily instantiated hash string. It will be generated the first time the
  // type's default fingerprint is computed (for example when using `equals`).
  this._hs = undefined;
  this._name = undefined;
  this._aliases = undefined;

  if (attrs) {
    // This is a complex (i.e. non-primitive) type.
    var name = attrs.name;
    var namespace = attrs.namespace === undefined ?
      opts && opts.namespace :
      attrs.namespace;
    if (name !== undefined) {
      // This isn't an anonymous type.
      name = qualify(name, namespace);
      if (isPrimitive(name)) {
        // Avro doesn't allow redefining primitive names.
        throw new Error(f('cannot rename primitive type: %j', name));
      }
      var registry = opts && opts.registry;
      if (registry) {
        if (registry[name] !== undefined) {
          throw new Error(f('duplicate type name: %s', name));
        }
        registry[name] = type;
      }
    } else if (opts && opts.noAnonymousTypes) {
      throw new Error(f('missing name property in schema: %j', attrs));
    }
    this._name = name;
    this._aliases = attrs.aliases ?
      attrs.aliases.map(function (s) { return qualify(s, namespace); }) :
      [];
  }
}

Type.isType = function (/* any, [prefix] ... */) {
  var l = arguments.length;
  if (!l) {
    return false;
  }

  var any = arguments[0];
  if (
    !any ||
    typeof any._update != 'function' ||
    typeof any.getTypeName != 'function'
  ) {
    // Not fool-proof, but most likely good enough.
    return false;
  }

  if (l === 1) {
    // No type names specified, we are done.
    return true;
  }

  // We check if at least one of the prefixes matches.
  var typeName = any.getTypeName();
  var i;
  for (i = 1; i < l; i++) {
    if (typeName.indexOf(arguments[i]) === 0) {
      return true;
    }
  }
  return false;
};

Type.__reset = function (size) { TAP.buf = new buffer.SlowBuffer(size); };

Type.prototype.createResolver = function (type, opts) {
  if (!Type.isType(type)) {
    // More explicit error message than the "incompatible type" thrown
    // otherwise (especially because of the overridden `toJSON` method).
    throw new Error(f('not a type: %j', type));
  }

  if (!Type.isType(this, 'logical') && Type.isType(type, 'logical')) {
    // Trying to read a logical type as a built-in: unwrap the logical type.
    return this.createResolver(type._underlyingType, opts);
  }

  opts = opts || {};
  opts.registry = opts.registry || {};

  var resolver, key;
  if (
    Type.isType(this, 'record', 'error') &&
    Type.isType(type, 'record', 'error')
  ) {
    // We allow conversions between records and errors.
    key = this._name + ':' + type._name; // ':' is illegal in Avro type names.
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
    var resolvers = type._types.map(function (t) {
      return this.createResolver(t, opts);
    }, this);
    resolver._read = function (tap) {
      var index = tap.readLong();
      var resolver = resolvers[index];
      if (resolver === undefined) {
        throw new Error(f('invalid union index: %s', index));
      }
      return resolvers[index]._read(tap);
    };
  } else {
    this._update(resolver, type, opts);
  }

  if (!resolver._read) {
    throw new Error(f('cannot read %s as %s', type, this));
  }
  return resolver;
};

Type.prototype.decode = function (buf, pos, resolver) {
  var tap = new Tap(buf, pos);
  var val = readValue(this, tap, resolver);
  if (!tap.isValid()) {
    return {value: undefined, offset: -1};
  }
  return {value: val, offset: tap.pos};
};

Type.prototype.encode = function (val, buf, pos) {
  var tap = new Tap(buf, pos);
  this._write(tap, val);
  if (!tap.isValid()) {
    // Don't throw as there is no way to predict this. We also return the
    // number of missing bytes to ease resizing.
    return buf.length - tap.pos;
  }
  return tap.pos;
};

Type.prototype.fromBuffer = function (buf, resolver, noCheck) {
  var tap = new Tap(buf);
  var val = readValue(this, tap, resolver, noCheck);
  if (!tap.isValid()) {
    throw new Error('truncated buffer');
  }
  if (!noCheck && tap.pos < buf.length) {
    throw new Error('trailing data');
  }
  return val;
};

Type.prototype.toBuffer = function (val) {
  TAP.pos = 0;
  this._write(TAP, val);
  if (!TAP.isValid()) {
    Type.__reset(2 * TAP.pos);
    TAP.pos = 0;
    this._write(TAP, val);
  }
  var buf = new Buffer(TAP.pos);
  TAP.buf.copy(buf, 0, 0, TAP.pos);
  return buf;
};

Type.prototype.fromString = function (str) {
  return this._copy(JSON.parse(str), {coerce: 2});
};

Type.prototype.toString = function (val) {
  if (val === undefined) {
    // Consistent behavior with standard `toString` expectations.
    return this.getSchema({noDeref: true});
  }
  return JSON.stringify(this._copy(val, {coerce: 3}));
};

Type.prototype.clone = function (val, opts) {
  if (opts) {
    opts = {
      coerce: !!opts.coerceBuffers | 0, // Coerce JSON to Buffer.
      fieldHook: opts.fieldHook,
      qualifyNames: !!opts.qualifyNames,
      wrap: !!opts.wrapUnions | 0 // Wrap first match into union.
    };
    return this._copy(val, opts);
  } else {
    // If no modifications are required, we can get by with a serialization
    // roundtrip (generally much faster than a standard deep copy).
    return this.fromBuffer(this.toBuffer(val));
  }
};

Type.prototype.isValid = function (val, opts) {
  // We only have a single flag for now, so no need to complicate things.
  var flags = (opts && opts.noUndeclaredFields) | 0;
  var errorHook = opts && opts.errorHook;
  var hook, path;
  if (errorHook) {
    path = [];
    hook = function (any, type) {
      errorHook.call(this, path.slice(), any, type, val);
    };
  }
  return this._check(val, flags, hook, path);
};

Type.prototype.compareBuffers = function (buf1, buf2) {
  return this._match(new Tap(buf1), new Tap(buf2));
};

Type.prototype.getName = function (asBranch) {
  var type = Type.isType(this, 'logical') ? this._underlyingType : this;
  if (type._name || !asBranch) {
    return type._name;
  }
  return Type.isType(this, 'union') ? undefined : type.getTypeName();
};

Type.prototype.getSchema = function (opts) { return stringify(this, opts); };

Type.prototype.equals = function (type) {
  return (
    Type.isType(type) &&
    this.getFingerprint().equals(type.getFingerprint())
  );
};

Type.prototype.getFingerprint = function (algorithm) {
  if (!algorithm) {
    if (!this._hs) {
      this._hs = utils.getHash(this.getSchema()).toString('binary');
    }
    return new Buffer(this._hs, 'binary');
  } else {
    return utils.getHash(this.getSchema(), algorithm);
  }
};

Type.prototype.inspect = function () {
  var typeName = this.getTypeName();
  var className = getClassName(typeName);
  if (isPrimitive(typeName)) {
    // The class name is sufficient to identify the type.
    return f('<%s>', className);
  } else {
    // We add a little metadata for convenience.
    var obj = JSON.parse(this.getSchema({exportAttrs: true, noDeref: true}));
    if (typeof obj == 'object' && !Type.isType(this, 'logical')) {
      obj.type = undefined; // Would be redundant with constructor name.
    }
    return f('<%s %j>', className, obj);
  }
};

Type.prototype._check = utils.abstractFunction;
Type.prototype._copy = utils.abstractFunction;
Type.prototype._match = utils.abstractFunction;
Type.prototype._read = utils.abstractFunction;
Type.prototype._skip = utils.abstractFunction;
Type.prototype._update = utils.abstractFunction;
Type.prototype._write = utils.abstractFunction;
Type.prototype.compare = utils.abstractFunction;
Type.prototype.getTypeName = utils.abstractFunction;
Type.prototype.random = utils.abstractFunction;

// Implementations.

/**
 * Base primitive Avro type.
 *
 * Most of the primitive types share the same cloning and resolution
 * mechanisms, provided by this class. This class also lets us conveniently
 * check whether a type is a primitive using `instanceof`.
 *
 */
function PrimitiveType() { Type.call(this); }
util.inherits(PrimitiveType, Type);

PrimitiveType.prototype._update = function (resolver, type) {
  if (type.constructor === this.constructor) {
    resolver._read = this._read;
  }
};

PrimitiveType.prototype._copy = function (val) {
  this._check(val, undefined, throwInvalidError);
  return val;
};

PrimitiveType.prototype.compare = utils.compare;

PrimitiveType.prototype.toJSON = function () { return this.getTypeName(); };

/**
 * Nulls.
 *
 */
function NullType() { PrimitiveType.call(this); }
util.inherits(NullType, PrimitiveType);

NullType.prototype._check = function (val, flags, hook) {
  var b = val === null;
  if (!b && hook) {
    hook(val, this);
  }
  return b;
};

NullType.prototype._read = function () { return null; };

NullType.prototype._skip = function () {};

NullType.prototype._write = function (tap, val) {
  if (val !== null) {
    throwInvalidError(val, this);
  }
};

NullType.prototype._match = function () { return 0; };

NullType.prototype.compare = NullType.prototype._match;

NullType.prototype.getTypeName = function () { return 'null'; };

NullType.prototype.random = NullType.prototype._read;

/**
 * Booleans.
 *
 */
function BooleanType() { PrimitiveType.call(this); }
util.inherits(BooleanType, PrimitiveType);

BooleanType.prototype._check = function (val, flags, hook) {
  var b = typeof val == 'boolean';
  if (!b && hook) {
    hook(val, this);
  }
  return b;
};

BooleanType.prototype._read = function (tap) { return tap.readBoolean(); };

BooleanType.prototype._skip = function (tap) { tap.skipBoolean(); };

BooleanType.prototype._write = function (tap, val) {
  if (typeof val != 'boolean') {
    throwInvalidError(val, this);
  }
  tap.writeBoolean(val);
};

BooleanType.prototype._match = function (tap1, tap2) {
  return tap1.matchBoolean(tap2);
};

BooleanType.prototype.getTypeName = function () { return 'boolean'; };

BooleanType.prototype.random = function () { return RANDOM.nextBoolean(); };

/**
 * Integers.
 *
 */
function IntType() { PrimitiveType.call(this); }
util.inherits(IntType, PrimitiveType);

IntType.prototype._check = function (val, flags, hook) {
  var b = val === (val | 0);
  if (!b && hook) {
    hook(val, this);
  }
  return b;
};

IntType.prototype._read = function (tap) { return tap.readInt(); };

IntType.prototype._skip = function (tap) { tap.skipInt(); };

IntType.prototype._write = function (tap, val) {
  if (val !== (val | 0)) {
    throwInvalidError(val, this);
  }
  tap.writeInt(val);
};

IntType.prototype._match = function (tap1, tap2) {
  return tap1.matchInt(tap2);
};

IntType.prototype.getTypeName = function () { return 'int'; };

IntType.prototype.random = function () { return RANDOM.nextInt(1000) | 0; };

/**
 * Longs.
 *
 * We can't capture all the range unfortunately since JavaScript represents all
 * numbers internally as `double`s, so the default implementation plays safe
 * and throws rather than potentially silently change the data. See `__with` or
 * `AbstractLongType` below for a way to implement a custom long type.
 *
 */
function LongType() { PrimitiveType.call(this); }
util.inherits(LongType, PrimitiveType);

LongType.prototype._check = function (val, flags, hook) {
  var b = typeof val == 'number' && val % 1 === 0 && isSafeLong(val);
  if (!b && hook) {
    hook(val, this);
  }
  return b;
};

LongType.prototype._read = function (tap) {
  var n = tap.readLong();
  if (!isSafeLong(n)) {
    throw new Error('potential precision loss');
  }
  return n;
};

LongType.prototype._skip = function (tap) { tap.skipLong(); };

LongType.prototype._write = function (tap, val) {
  if (typeof val != 'number' || val % 1 || !isSafeLong(val)) {
    throwInvalidError(val, this);
  }
  tap.writeLong(val);
};

LongType.prototype._match = function (tap1, tap2) {
  return tap1.matchLong(tap2);
};

LongType.prototype._update = function (resolver, type) {
  switch (type.getTypeName()) {
    case 'int':
    case 'long':
      resolver._read = type._read;
  }
};

LongType.prototype.getTypeName = function () { return 'long'; };

LongType.prototype.random = function () { return RANDOM.nextInt(); };

LongType.__with = function (methods, noUnpack) {
  methods = methods || {}; // Will give a more helpful error message.
  // We map some of the methods to a different name to be able to intercept
  // their input and output (otherwise we wouldn't be able to perform any
  // unpacking logic, and the type wouldn't work when nested).
  var mapping = {
    toBuffer: '_toBuffer',
    fromBuffer: '_fromBuffer',
    fromJSON: '_fromJSON',
    toJSON: '_toJSON',
    isValid: '_isValid',
    compare: 'compare'
  };
  var type = new AbstractLongType(noUnpack);
  Object.keys(mapping).forEach(function (name) {
    if (methods[name] === undefined) {
      throw new Error(f('missing method implementation: %s', name));
    }
    type[mapping[name]] = methods[name];
  });
  return type;
};

/**
 * Floats.
 *
 */
function FloatType() { PrimitiveType.call(this); }
util.inherits(FloatType, PrimitiveType);

FloatType.prototype._check = function (val, flags, hook) {
  var b = typeof val == 'number';
  if (!b && hook) {
    hook(val, this);
  }
  return b;
};

FloatType.prototype._read = function (tap) { return tap.readFloat(); };

FloatType.prototype._skip = function (tap) { tap.skipFloat(); };

FloatType.prototype._write = function (tap, val) {
  if (typeof val != 'number') {
    throwInvalidError(val, this);
  }
  tap.writeFloat(val);
};

FloatType.prototype._match = function (tap1, tap2) {
  return tap1.matchFloat(tap2);
};

FloatType.prototype._update = function (resolver, type) {
  switch (type.getTypeName()) {
    case 'float':
    case 'int':
    case 'long':
      resolver._read = type._read;
  }
};

FloatType.prototype.getTypeName = function () { return 'float'; };

FloatType.prototype.random = function () { return RANDOM.nextFloat(1e3); };

/**
 * Doubles.
 *
 */
function DoubleType() { PrimitiveType.call(this); }
util.inherits(DoubleType, PrimitiveType);

DoubleType.prototype._check = function (val, flags, hook) {
  var b = typeof val == 'number';
  if (!b && hook) {
    hook(val, this);
  }
  return b;
};

DoubleType.prototype._read = function (tap) { return tap.readDouble(); };

DoubleType.prototype._skip = function (tap) { tap.skipDouble(); };

DoubleType.prototype._write = function (tap, val) {
  if (typeof val != 'number') {
    throwInvalidError(val, this);
  }
  tap.writeDouble(val);
};

DoubleType.prototype._match = function (tap1, tap2) {
  return tap1.matchDouble(tap2);
};

DoubleType.prototype._update = function (resolver, type) {
  switch (type.getTypeName()) {
    case 'double':
    case 'float':
    case 'int':
    case 'long':
      resolver._read = type._read;
  }
};

DoubleType.prototype.getTypeName = function () { return 'double'; };

DoubleType.prototype.random = function () { return RANDOM.nextFloat(); };

/**
 * Strings.
 *
 */
function StringType() { PrimitiveType.call(this); }
util.inherits(StringType, PrimitiveType);

StringType.prototype._check = function (val, flags, hook) {
  var b = typeof val == 'string';
  if (!b && hook) {
    hook(val, this);
  }
  return b;
};

StringType.prototype._read = function (tap) { return tap.readString(); };

StringType.prototype._skip = function (tap) { tap.skipString(); };

StringType.prototype._write = function (tap, val) {
  if (typeof val != 'string') {
    throwInvalidError(val, this);
  }
  tap.writeString(val);
};

StringType.prototype._match = function (tap1, tap2) {
  return tap1.matchString(tap2);
};

StringType.prototype._update = function (resolver, type) {
  switch (type.getTypeName()) {
    case 'bytes':
    case 'string':
      resolver._read = this._read;
  }
};

StringType.prototype.getTypeName = function () { return 'string'; };

StringType.prototype.random = function () {
  return RANDOM.nextString(RANDOM.nextInt(32));
};

/**
 * Bytes.
 *
 * These are represented in memory as `Buffer`s rather than binary-encoded
 * strings. This is more efficient (when decoding/encoding from bytes, the
 * common use-case), idiomatic, and convenient.
 *
 * Note the coercion in `_copy`.
 *
 */
function BytesType() { PrimitiveType.call(this); }
util.inherits(BytesType, PrimitiveType);

BytesType.prototype._check = function (val, flags, hook) {
  var b = Buffer.isBuffer(val);
  if (!b && hook) {
    hook(val, this);
  }
  return b;
};

BytesType.prototype._read = function (tap) { return tap.readBytes(); };

BytesType.prototype._skip = function (tap) { tap.skipBytes(); };

BytesType.prototype._write = function (tap, val) {
  if (!Buffer.isBuffer(val)) {
    throwInvalidError(val, this);
  }
  tap.writeBytes(val);
};

BytesType.prototype._match = function (tap1, tap2) {
  return tap1.matchBytes(tap2);
};

BytesType.prototype._update = StringType.prototype._update;

BytesType.prototype._copy = function (obj, opts) {
  var buf;
  switch ((opts && opts.coerce) | 0) {
    case 3: // Coerce buffers to strings.
      this._check(obj, undefined, throwInvalidError);
      return obj.toString('binary');
    case 2: // Coerce strings to buffers.
      if (typeof obj != 'string') {
        throw new Error(f('cannot coerce to buffer: %j', obj));
      }
      buf = new Buffer(obj, 'binary');
      this._check(buf, undefined, throwInvalidError);
      return buf;
    case 1: // Coerce buffer JSON representation to buffers.
      if (!isJsonBuffer(obj)) {
        throw new Error(f('cannot coerce to buffer: %j', obj));
      }
      buf = new Buffer(obj.data);
      this._check(buf, undefined, throwInvalidError);
      return buf;
    default: // Copy buffer.
      this._check(obj, undefined, throwInvalidError);
      return new Buffer(obj);
  }
};

BytesType.prototype.compare = Buffer.compare;

BytesType.prototype.getTypeName = function () { return 'bytes'; };

BytesType.prototype.random = function () {
  return RANDOM.nextBuffer(RANDOM.nextInt(32));
};

/**
 * Base "abstract" Avro union type.
 *
 */
function UnionType(attrs, opts) {
  Type.call(this);

  if (!Array.isArray(attrs)) {
    throw new Error(f('non-array union schema: %j', attrs));
  }
  if (!attrs.length) {
    throw new Error('empty union');
  }
  this._types = attrs.map(function (obj) { return createType(obj, opts); });

  this._branchIndices = {};
  this._types.forEach(function (type, i) {
    if (Type.isType(type, 'union')) {
      throw new Error('unions cannot be directly nested');
    }
    var branch = type.getName(true);
    if (this._branchIndices[branch] !== undefined) {
      throw new Error(f('duplicate union branch name: %j', branch));
    }
    this._branchIndices[branch] = i;
  }, this);
}
util.inherits(UnionType, Type);

UnionType.prototype._skip = function (tap) {
  this._types[tap.readLong()]._skip(tap);
};

UnionType.prototype._match = function (tap1, tap2) {
  var n1 = tap1.readLong();
  var n2 = tap2.readLong();
  if (n1 === n2) {
    return this._types[n1]._match(tap1, tap2);
  } else {
    return n1 < n2 ? -1 : 1;
  }
};

UnionType.prototype.getTypes = function () { return this._types.slice(); };

UnionType.prototype.toJSON = function () { return this._types; };

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
 *
 */
function UnwrappedUnionType(attrs, opts) {
  UnionType.call(this, attrs, opts);

  this._logicalBranches = null;
  this._bucketIndices = {};
  this._types.forEach(function (type, index) {
    if (Type.isType(type, 'logical')) {
      if (!this._logicalBranches) {
        this._logicalBranches = [];
      }
      this._logicalBranches.push({index: index, type: type});
    } else {
      var bucket = getTypeBucket(type);
      if (this._bucketIndices[bucket] !== undefined) {
        throw new Error(f('ambiguous unwrapped union: %j', this));
      }
      this._bucketIndices[bucket] = index;
    }
  }, this);
}
util.inherits(UnwrappedUnionType, UnionType);

UnwrappedUnionType.prototype._getIndex = function (val) {
  var index = this._bucketIndices[getValueBucket(val)];
  if (this._logicalBranches) {
    // Slower path, we must run the value through all logical types.
    index = this._getLogicalIndex(val, index);
  }
  return index;
};

UnwrappedUnionType.prototype._getLogicalIndex = function (any, index) {
  var logicalBranches = this._logicalBranches;
  var i, l, branch;
  for (i = 0, l = logicalBranches.length; i < l; i++) {
    branch = logicalBranches[i];
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

UnwrappedUnionType.prototype._check = function (val, flags, hook, path) {
  var index = this._getIndex(val);
  var b = index !== undefined;
  if (b) {
    return this._types[index]._check(val, flags, hook, path);
  }
  if (hook) {
    hook(val, this);
  }
  return b;
};

UnwrappedUnionType.prototype._read = function (tap) {
  var index = tap.readLong();
  var branchType = this._types[index];
  if (branchType) {
    return branchType._read(tap);
  } else {
    throw new Error(f('invalid union index: %s', index));
  }
};

UnwrappedUnionType.prototype._write = function (tap, val) {
  var index = this._getIndex(val);
  if (index === undefined) {
    throwInvalidError(val, this);
  }
  tap.writeLong(index);
  if (val !== null) {
    this._types[index]._write(tap, val);
  }
};

UnwrappedUnionType.prototype._update = function (resolver, type, opts) {
  // jshint -W083
  // (The loop exits after the first function is created.)
  var i, l, typeResolver;
  for (i = 0, l = this._types.length; i < l; i++) {
    try {
      typeResolver = this._types[i].createResolver(type, opts);
    } catch (err) {
      continue;
    }
    resolver._read = function (tap) { return typeResolver._read(tap); };
    return;
  }
};

UnwrappedUnionType.prototype._copy = function (val, opts) {
  var coerce = opts && opts.coerce | 0;
  var wrap = opts && opts.wrap | 0;
  var index;
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
          var keys = Object.keys(val);
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
  var type = this._types[index];
  if (val === null || wrap === 3) {
    return type._copy(val, opts);
  } else {
    switch (coerce) {
      case 3:
        // Encoding to JSON, we wrap the value.
        var obj = {};
        obj[type.getName(true)] = type._copy(val, opts);
        return obj;
      default:
        return type._copy(val, opts);
    }
  }
};

UnwrappedUnionType.prototype.compare = function (val1, val2) {
  var index1 = this._getIndex(val1);
  var index2 = this._getIndex(val2);
  if (index1 === undefined) {
    throwInvalidError(val1, this);
  } else if (index2 === undefined) {
    throwInvalidError(val2, this);
  } else if (index1 === index2) {
    return this._types[index1].compare(val1, val2);
  } else {
    return utils.compare(index1, index2);
  }
};

UnwrappedUnionType.prototype.getTypeName = function () {
  return 'union:unwrapped';
};

UnwrappedUnionType.prototype.random = function () {
  var index = RANDOM.nextInt(this._types.length);
  return this._types[index].random();
};

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
 *
 */
function WrappedUnionType(attrs, opts) {
  UnionType.call(this, attrs, opts);

  this._constructors = this._types.map(function (type) {
    // jshint -W054
    var name = type.getName(true);
    if (name === 'null') {
      return null;
    }
    var body;
    if (~name.indexOf('.')) { // Qualified name.
      body = 'this[\'' + name + '\'] = val;';
    } else {
      body = 'this.' + name + ' = val;';
    }
    var constructor = new Function('val', body);
    constructor.getBranchType = function () { return type; };
    return constructor;
  });
}
util.inherits(WrappedUnionType, UnionType);

WrappedUnionType.prototype._check = function (val, flags, hook, path) {
  var b = false;
  if (val === null) {
    // Shortcut type lookup in this case.
    b = this._branchIndices['null'] !== undefined;
  } else if (typeof val == 'object') {
    var keys = Object.keys(val);
    if (keys.length === 1) {
      // We require a single key here to ensure that writes are correct and
      // efficient as soon as a record passes this check.
      var name = keys[0];
      var index = this._branchIndices[name];
      if (index !== undefined) {
        if (hook) {
          // Slow path.
          path.push(name);
          b = this._types[index]._check(val[name], flags, hook, path);
          path.pop();
          return b;
        } else {
          return this._types[index]._check(val[name], flags);
        }
      }
    }
  }
  if (!b && hook) {
    hook(val, this);
  }
  return b;
};

WrappedUnionType.prototype._read = function (tap) {
  var index = tap.readLong();
  var Class = this._constructors[index];
  if (Class) {
    return new Class(this._types[index]._read(tap));
  } else if (Class === null) {
    return null;
  } else {
    throw new Error(f('invalid union index: %s', index));
  }
};

WrappedUnionType.prototype._write = function (tap, val) {
  var index, keys, name;
  if (val === null) {
    index = this._branchIndices['null'];
    if (index === undefined) {
      throwInvalidError(val, this);
    }
    tap.writeLong(index);
  } else {
    keys = Object.keys(val);
    if (keys.length === 1) {
      name = keys[0];
      index = this._branchIndices[name];
    }
    if (index === undefined) {
      throwInvalidError(val, this);
    }
    tap.writeLong(index);
    this._types[index]._write(tap, val[name]);
  }
};

WrappedUnionType.prototype._update = function (resolver, type, opts) {
  // jshint -W083
  // (The loop exits after the first function is created.)
  var i, l, typeResolver, Class;
  for (i = 0, l = this._types.length; i < l; i++) {
    try {
      typeResolver = this._types[i].createResolver(type, opts);
    } catch (err) {
      continue;
    }
    Class = this._constructors[i];
    if (Class) {
      resolver._read = function (tap) {
        return new Class(typeResolver._read(tap));
      };
    } else {
      resolver._read = function () { return null; };
    }
    return;
  }
};

WrappedUnionType.prototype._copy = function (val, opts) {
  var wrap = opts && opts.wrap | 0;
  if (wrap === 2) {
    // Promote into first type (used for schema defaults).
    if (val === null && this._constructors[0] === null) {
      return null;
    }
    return new this._constructors[0](this._types[0]._copy(val, opts));
  }
  if (val === null && this._branchIndices['null'] !== undefined) {
    return null;
  }

  var i, l, obj;
  if (typeof val == 'object') {
    var keys = Object.keys(val);
    if (keys.length === 1) {
      var name = keys[0];
      i = this._branchIndices[name];
      if (i === undefined && opts.qualifyNames) {
        // We are a bit more flexible than in `_check` here since we have
        // to deal with other serializers being less strict, so we fall
        // back to looking up unqualified names.
        var j, type;
        for (j = 0, l = this._types.length; j < l; j++) {
          type = this._types[j];
          if (type._name && name === unqualify(type._name)) {
            i = j;
            break;
          }
        }
      }
      if (i !== undefined) {
        obj = this._types[i]._copy(val[name], opts);
      }
    }
  }
  if (wrap === 1 && obj === undefined) {
    // Try promoting into first match (convenience, slow).
    i = 0;
    l = this._types.length;
    while (i < l && obj === undefined) {
      try {
        obj = this._types[i]._copy(val, opts);
      } catch (err) {
        i++;
      }
    }
  }
  if (obj !== undefined) {
    return wrap === 3 ? obj : new this._constructors[i](obj);
  }
  throwInvalidError(val, this);
};

WrappedUnionType.prototype.compare = function (val1, val2) {
  var name1 = val1 === null ? 'null' : Object.keys(val1)[0];
  var name2 = val2 === null ? 'null' : Object.keys(val2)[0];
  var index = this._branchIndices[name1];
  if (name1 === name2) {
    return name1 === 'null' ?
      0 :
      this._types[index].compare(val1[name1], val2[name1]);
  } else {
    return utils.compare(index, this._branchIndices[name2]);
  }
};

WrappedUnionType.prototype.getTypeName = function () {
  return 'union:wrapped';
};

WrappedUnionType.prototype.random = function () {
  var index = RANDOM.nextInt(this._types.length);
  var Class = this._constructors[index];
  if (!Class) {
    return null;
  }
  return new Class(this._types[index].random());
};

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
 *
 */
function EnumType(attrs, opts) {
  Type.call(this, attrs, opts);

  if (!Array.isArray(attrs.symbols) || !attrs.symbols.length) {
    throw new Error(f('invalid enum symbols: %j', attrs.symbols));
  }
  this._symbols = attrs.symbols;

  this._indices = {};
  this._symbols.forEach(function (symbol, i) {
    if (!isValidName(symbol)) {
      throw new Error(f('invalid %s symbol: %j', this, symbol));
    }
    if (this._indices[symbol] !== undefined) {
      throw new Error(f('duplicate %s symbol: %j', this, symbol));
    }
    this._indices[symbol] = i;
  }, this);
}
util.inherits(EnumType, Type);

EnumType.prototype._check = function (val, flags, hook) {
  var b = this._indices[val] !== undefined;
  if (!b && hook) {
    hook(val, this);
  }
  return b;
};

EnumType.prototype._read = function (tap) {
  var index = tap.readLong();
  var symbol = this._symbols[index];
  if (symbol === undefined) {
    throw new Error(f('invalid %s enum index: %s', this._name, index));
  }
  return symbol;
};

EnumType.prototype._skip = function (tap) { tap.skipLong(); };

EnumType.prototype._write = function (tap, val) {
  var index = this._indices[val];
  if (index === undefined) {
    throwInvalidError(val, this);
  }
  tap.writeLong(index);
};

EnumType.prototype._match = function (tap1, tap2) {
  return tap1.matchLong(tap2);
};

EnumType.prototype.compare = function (val1, val2) {
  return utils.compare(this._indices[val1], this._indices[val2]);
};

EnumType.prototype._update = function (resolver, type) {
  var symbols = this._symbols;
  if (
    type.getTypeName() === 'enum' &&
    (!type._name || ~getAliases(this).indexOf(type._name)) &&
    type._symbols.every(function (s) { return ~symbols.indexOf(s); })
  ) {
    resolver._symbols = type._symbols;
    resolver._read = type._read;
  }
};

EnumType.prototype._copy = function (val) {
  this._check(val, undefined, throwInvalidError);
  return val;
};

EnumType.prototype.getAliases = function () { return this._aliases; };

EnumType.prototype.getSymbols = function () { return this._symbols.slice(); };

EnumType.prototype.getTypeName = function () { return 'enum'; };

EnumType.prototype.random = function () {
  return RANDOM.choice(this._symbols);
};

EnumType.prototype.toJSON = function () {
  return {
    name: this._name,
    type: this.getTypeName(),
    symbols: this._symbols,
    aliases: this._aliases
  };
};

/**
 * Avro fixed type.
 *
 * Represented simply as a `Buffer`.
 *
 */
function FixedType(attrs, opts) {
  Type.call(this, attrs, opts);

  if (attrs.size !== (attrs.size | 0) || attrs.size < 1) {
    throw new Error(f('invalid %s fixed size', this.getName(true)));
  }
  this._size = attrs.size | 0;
}
util.inherits(FixedType, Type);

FixedType.prototype._check = function (val, flags, hook) {
  var b = Buffer.isBuffer(val) && val.length === this._size;
  if (!b && hook) {
    hook(val, this);
  }
  return b;
};

FixedType.prototype._read = function (tap) {
  return tap.readFixed(this._size);
};

FixedType.prototype._skip = function (tap) {
  tap.skipFixed(this._size);
};

FixedType.prototype._write = function (tap, val) {
  if (!Buffer.isBuffer(val) || val.length !== this._size) {
    throwInvalidError(val, this);
  }
  tap.writeFixed(val, this._size);
};

FixedType.prototype._match = function (tap1, tap2) {
  return tap1.matchFixed(tap2, this._size);
};

FixedType.prototype.compare = Buffer.compare;

FixedType.prototype._update = function (resolver, type) {
  if (
    type.getTypeName() === 'fixed' &&
    this._size === type._size &&
    (!type._name || ~getAliases(this).indexOf(type._name))
  ) {
    resolver._size = this._size;
    resolver._read = this._read;
  }
};

FixedType.prototype._copy = BytesType.prototype._copy;

FixedType.prototype.getAliases = function () { return this._aliases; };

FixedType.prototype.getSize = function () { return this._size; };

FixedType.prototype.getTypeName = function () { return 'fixed'; };

FixedType.prototype.random = function () {
  return RANDOM.nextBuffer(this._size);
};

FixedType.prototype.toJSON = function () {
  return {
    name: this._name,
    type: this.getTypeName(),
    size: this._size,
    aliases: this._aliases
  };
};

/**
 * Avro map.
 *
 * Represented as vanilla objects.
 *
 */
function MapType(attrs, opts) {
  Type.call(this);

  if (!attrs.values) {
    throw new Error(f('missing map values: %j', attrs));
  }
  this._values = createType(attrs.values, opts);
}
util.inherits(MapType, Type);

MapType.prototype._check = function (val, flags, hook, path) {
  if (!val || typeof val != 'object' || Array.isArray(val)) {
    if (hook) {
      hook(val, this);
    }
    return false;
  }

  var keys = Object.keys(val);
  var b = true;
  var i, l, j, key;
  if (hook) {
    // Slow path.
    j = path.length;
    path.push('');
    for (i = 0, l = keys.length; i < l; i++) {
      key = path[j] = keys[i];
      if (!this._values._check(val[key], flags, hook, path)) {
        b = false;
      }
    }
    path.pop();
  } else {
    for (i = 0, l = keys.length; i < l; i++) {
      if (!this._values._check(val[keys[i]], flags)) {
        return false;
      }
    }
  }
  return b;
};

MapType.prototype._read = function (tap) {
  var values = this._values;
  var val = {};
  var n;
  while ((n = readArraySize(tap))) {
    while (n--) {
      var key = tap.readString();
      val[key] = values._read(tap);
    }
  }
  return val;
};

MapType.prototype._skip = function (tap) {
  var values = this._values;
  var len, n;
  while ((n = tap.readLong())) {
    if (n < 0) {
      len = tap.readLong();
      tap.pos += len;
    } else {
      while (n--) {
        tap.skipString();
        values._skip(tap);
      }
    }
  }
};

MapType.prototype._write = function (tap, val) {
  if (!val || typeof val != 'object' || Array.isArray(val)) {
    throwInvalidError(val, this);
  }

  var values = this._values;
  var keys = Object.keys(val);
  var n = keys.length;
  var i, key;
  if (n) {
    tap.writeLong(n);
    for (i = 0; i < n; i++) {
      key = keys[i];
      tap.writeString(key);
      values._write(tap, val[key]);
    }
  }
  tap.writeLong(0);
};

MapType.prototype._match = function () {
  throw new Error('maps cannot be compared');
};

MapType.prototype._update = function (resolver, type, opts) {
  if (type.getTypeName() === 'map') {
    resolver._values = this._values.createResolver(type._values, opts);
    resolver._read = this._read;
  }
};

MapType.prototype._copy = function (val, opts) {
  if (val && typeof val == 'object' && !Array.isArray(val)) {
    var values = this._values;
    var keys = Object.keys(val);
    var i, l, key;
    var copy = {};
    for (i = 0, l = keys.length; i < l; i++) {
      key = keys[i];
      copy[key] = values._copy(val[key], opts);
    }
    return copy;
  }
  throwInvalidError(val, this);
};

MapType.prototype.compare = MapType.prototype._match;

MapType.prototype.getTypeName = function () { return 'map'; };

MapType.prototype.getValuesType = function () { return this._values; };

MapType.prototype.random = function () {
  var val = {};
  var i, l;
  for (i = 0, l = RANDOM.nextInt(10); i < l; i++) {
    val[RANDOM.nextString(RANDOM.nextInt(20))] = this._values.random();
  }
  return val;
};

MapType.prototype.toJSON = function () {
  return {type: this.getTypeName(), values: this._values};
};

/**
 * Avro array.
 *
 * Represented as vanilla arrays.
 *
 */
function ArrayType(attrs, opts) {
  Type.call(this);

  if (!attrs.items) {
    throw new Error(f('missing array items: %j', attrs));
  }
  this._items = createType(attrs.items, opts);
}
util.inherits(ArrayType, Type);

ArrayType.prototype._check = function (val, flags, hook, path) {
  if (!Array.isArray(val)) {
    if (hook) {
      hook(val, this);
    }
    return false;
  }

  var b = true;
  var i, l, j;
  if (hook) {
    // Slow path.
    j = path.length;
    path.push('');
    for (i = 0, l = val.length; i < l; i++) {
      path[j] = '' + i;
      if (!this._items._check(val[i], flags, hook, path)) {
        b = false;
      }
    }
    path.pop();
  } else {
    for (i = 0, l = val.length; i < l; i++) {
      if (!this._items._check(val[i], flags)) {
        return false;
      }
    }
  }
  return b;
};

ArrayType.prototype._read = function (tap) {
  var items = this._items;
  var val = [];
  var i, n;
  while ((n = tap.readLong())) {
    if (n < 0) {
      n = -n;
      tap.skipLong(); // Skip size.
    }
    for (i = 0; i < n; i++) {
      val[i] = items._read(tap);
    }
  }
  return val;
};

ArrayType.prototype._skip = function (tap) {
  var len, n;
  while ((n = tap.readLong())) {
    if (n < 0) {
      len = tap.readLong();
      tap.pos += len;
    } else {
      while (n--) {
        this._items._skip(tap);
      }
    }
  }
};

ArrayType.prototype._write = function (tap, val) {
  if (!Array.isArray(val)) {
    throwInvalidError(val, this);
  }

  var n = val.length;
  var i;
  if (n) {
    tap.writeLong(n);
    for (i = 0; i < n; i++) {
      this._items._write(tap, val[i]);
    }
  }
  tap.writeLong(0);
};

ArrayType.prototype._match = function (tap1, tap2) {
  var n1 = tap1.readLong();
  var n2 = tap2.readLong();
  var f;
  while (n1 && n2) {
    f = this._items._match(tap1, tap2);
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
};

ArrayType.prototype._update = function (resolver, type, opts) {
  if (type.getTypeName() === 'array') {
    resolver._items = this._items.createResolver(type._items, opts);
    resolver._read = this._read;
  }
};

ArrayType.prototype._copy = function (val, opts) {
  if (!Array.isArray(val)) {
    throwInvalidError(val, this);
  }
  var items = new Array(val.length);
  var i, l;
  for (i = 0, l = val.length; i < l; i++) {
    items[i] = this._items._copy(val[i], opts);
  }
  return items;
};

ArrayType.prototype.compare = function (val1, val2) {
  var n1 = val1.length;
  var n2 = val2.length;
  var i, l, f;
  for (i = 0, l = Math.min(n1, n2); i < l; i++) {
    if ((f = this._items.compare(val1[i], val2[i]))) {
      return f;
    }
  }
  return utils.compare(n1, n2);
};

ArrayType.prototype.getItemsType = function () { return this._items; };

ArrayType.prototype.getTypeName = function () { return 'array'; };

ArrayType.prototype.random = function () {
  var arr = [];
  var i, l;
  for (i = 0, l = RANDOM.nextInt(10); i < l; i++) {
    arr.push(this._items.random());
  }
  return arr;
};

ArrayType.prototype.toJSON = function () {
  return {type: this.getTypeName(), items: this._items};
};

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
 *
 */
function RecordType(attrs, opts) {
  // Force creation of the options object in case we need to register this
  // record's name.
  opts = opts || {};

  // Save the namespace to restore it as we leave this record's scope.
  var namespace = opts.namespace;
  if (attrs.namespace !== undefined) {
    opts.namespace = attrs.namespace;
  } else if (attrs.name) {
    // Fully qualified names' namespaces are used when no explicit namespace
    // attribute was specified.
    var match = /^(.*)\.[^.]+$/.exec(attrs.name);
    if (match) {
      opts.namespace = match[1];
    }
  }
  Type.call(this, attrs, opts);

  if (!Array.isArray(attrs.fields)) {
    throw new Error(f('non-array record fields: %j', attrs.fields));
  }
  if (utils.hasDuplicates(attrs.fields, function (f) { return f.name; })) {
    throw new Error(f('duplicate field name: %j', attrs.fields));
  }
  this._fieldsMap = {};
  this._fields = attrs.fields.map(function (f) {
    var field = new Field(f, opts);
    this._fieldsMap[field.getName()] = field;
    return field;
  }, this);

  this._isError = attrs.type === 'error';
  this._constructor = this._createConstructor();
  this._read = this._createReader();
  this._skip = this._createSkipper();
  this._write = this._createWriter();
  this._check = this._createChecker();

  opts.namespace = namespace;
}
util.inherits(RecordType, Type);

RecordType.prototype._getConstructorName = function () {
  return this._name ?
    unqualify(this._name) :
    this._isError ? 'Error$' : 'Record$';
};

RecordType.prototype._createConstructor = function () {
  // jshint -W054
  var outerArgs = [];
  var innerArgs = [];
  var ds = []; // Defaults.
  var innerBody = this._isError ? '  Error.call(this);\n' : '';
  // Not calling `Error.captureStackTrace` because this wouldn't be compatible
  // with browsers other than Chrome.
  var i, l, field, name, getDefault;
  for (i = 0, l = this._fields.length; i < l; i++) {
    field = this._fields[i];
    getDefault = field.getDefault;
    name = field._name;
    innerArgs.push('v' + i);
    innerBody += '  ';
    if (getDefault() === undefined) {
      innerBody += 'this.' + name + ' = v' + i + ';\n';
    } else {
      innerBody += 'if (v' + i + ' === undefined) { ';
      innerBody += 'this.' + name + ' = d' + ds.length + '(); ';
      innerBody += '} else { this.' + name + ' = v' + i + '; }\n';
      outerArgs.push('d' + ds.length);
      ds.push(getDefault);
    }
  }
  var outerBody = 'return function ' + this._getConstructorName() + '(';
  outerBody += innerArgs.join() + ') {\n' + innerBody + '};';
  var Record = new Function(outerArgs.join(), outerBody).apply(undefined, ds);

  var self = this;
  Record.getType = function () { return self; };
  if (this._isError) {
    util.inherits(Record, Error);
    // Not setting the error's name on the prototype to be consistent with how
    // object fields are mapped to (only if defined in the schema as a field).
  }
  Record.prototype.clone = function (o) { return self.clone(this, o); };
  Record.prototype.compare = function (v) { return self.compare(this, v); };
  Record.prototype.isValid = function (o) { return self.isValid(this, o); };
  Record.prototype.toBuffer = function () { return self.toBuffer(this); };
  Record.prototype.toString = function () { return self.toString(this); };
  return Record;
};

RecordType.prototype._createChecker = function () {
  // jshint -W054
  var names = [];
  var values = [];
  var name = this._getConstructorName();
  var body = 'return function check' + name + '(v, f, h, p) {\n';
  body += '  if (\n';
  body += '    v === null ||\n';
  body += '    typeof v != \'object\' ||\n';
  body += '    (f && !this._checkFields(v))\n';
  body += '  ) {\n';
  body += '    if (h) { h(v, this); }\n';
  body += '    return false;\n';
  body += '  }\n';
  if (!this._fields.length) {
    // Special case, empty record. We handle this directly.
    body += '  return true;\n';
  } else {
    for (i = 0, l = this._fields.length; i < l; i++) {
      field = this._fields[i];
      names.push('t' + i);
      values.push(field._type);
      if (field.getDefault() !== undefined) {
        body += '  var v' + i + ' = v.' + field._name + ';\n';
      }
    }
    body += '  if (h) {\n';
    body += '    var b = 1;\n';
    body += '    var j = p.length;\n';
    body += '    p.push(\'\');\n';
    var i, l, field;
    for (i = 0, l = this._fields.length; i < l; i++) {
      field = this._fields[i];
      body += '    p[j] = \'' + field._name + '\';\n';
      body += '    b &= ';
      if (field.getDefault() === undefined) {
        body += 't' + i + '._check(v.' + field._name + ', f, h, p);\n';
      } else {
        body += 'v' + i + ' === undefined || ';
        body += 't' + i + '._check(v' + i + ', f, h, p);\n';
      }
    }
    body += '    p.pop();\n';
    body += '    return !!b;\n';
    body += '  } else {\n    return (\n      ';
    body += this._fields.map(function (field, i) {
      return field.getDefault() === undefined ?
        't' + i + '._check(v.' + field._name + ', f)' :
        '(v' + i + ' === undefined || t' + i + '._check(v' + i + ', f))';
    }).join(' &&\n      ');
    body += '\n    );\n  }\n';
  }
  body += '};';
  return new Function(names.join(), body).apply(undefined, values);
};

RecordType.prototype._createReader = function () {
  // jshint -W054
  var names = [];
  var values = [this._constructor];
  var i, l;
  for (i = 0, l = this._fields.length; i < l; i++) {
    names.push('t' + i);
    values.push(this._fields[i]._type);
  }
  var name = this._getConstructorName();
  var body = 'return function read' + name + '(t) {\n';
  body += '  return new ' + name + '(\n    ';
  body += names.map(function (s) { return s + '._read(t)'; }).join(',\n    ');
  body += '\n  );\n};';
  names.unshift(name);
  // We can do this since the JS spec guarantees that function arguments are
  // evaluated from left to right.
  return new Function(names.join(), body).apply(undefined, values);
};

RecordType.prototype._createSkipper = function () {
  // jshint -W054
  var args = [];
  var body = 'return function skip' + this._getConstructorName() + '(t) {\n';
  var values = [];
  var i, l;
  for (i = 0, l = this._fields.length; i < l; i++) {
    args.push('t' + i);
    values.push(this._fields[i]._type);
    body += '  t' + i + '._skip(t);\n';
  }
  body += '}';
  return new Function(args.join(), body).apply(undefined, values);
};

RecordType.prototype._createWriter = function () {
  // jshint -W054
  // We still do default handling here, in case a normal JS object is passed.
  var args = [];
  var name = this._getConstructorName();
  var body = 'return function write' + name + '(t, v) {\n';
  var values = [];
  var i, l, field, value;
  for (i = 0, l = this._fields.length; i < l; i++) {
    field = this._fields[i];
    args.push('t' + i);
    values.push(field._type);
    body += '  ';
    if (field.getDefault() === undefined) {
      body += 't' + i + '._write(t, v.' + field._name + ');\n';
    } else {
      value = field._type.toBuffer(field.getDefault()).toString('binary');
      // Convert the default value to a binary string ahead of time. We aren't
      // converting it to a buffer to avoid retaining too much memory. If we
      // had our own buffer pool, this could be an idea in the future.
      args.push('d' + i);
      values.push(value);
      body += 'var v' + i + ' = v.' + field._name + ';\n';
      body += 'if (v' + i + ' === undefined) {\n';
      body += '    t.writeBinary(d' + i + ', ' + value.length + ');\n';
      body += '  } else {\n    t' + i + '._write(t, v' + i + ');\n  }\n';
    }
  }
  body += '}';
  return new Function(args.join(), body).apply(undefined, values);
};

RecordType.prototype._update = function (resolver, type, opts) {
  // jshint -W054
  if (type._name && !~getAliases(this).indexOf(type._name)) {
    throw new Error(f('no alias found for %s', type._name));
  }

  var rFields = this._fields;
  var wFields = type._fields;
  var wFieldsMap = utils.toMap(wFields, function (f) { return f._name; });

  var innerArgs = []; // Arguments for reader constructor.
  var resolvers = {}; // Resolvers keyed by writer field name.
  var i, j, field, name, names, matches;
  for (i = 0; i < rFields.length; i++) {
    field = rFields[i];
    names = getAliases(field);
    matches = [];
    for (j = 0; j < names.length; j++) {
      name = names[j];
      if (wFieldsMap[name]) {
        matches.push(name);
      }
    }
    if (matches.length > 1) {
      throw new Error(f('multiple matches for %s field', field._name));
    }
    if (!matches.length) {
      if (field.getDefault() === undefined) {
        throw new Error(f('no match for default-less %s field', field._name));
      }
      innerArgs.push('undefined');
    } else {
      name = matches[0];
      resolvers[name] = {
        resolver: field._type.createResolver(wFieldsMap[name]._type, opts),
        name: field._name // Reader field name.
      };
      innerArgs.push(field._name);
    }
  }

  // See if we can add a bypass for unused fields at the end of the record.
  var lazyIndex = -1;
  i = wFields.length;
  while (i && resolvers[wFields[--i]._name] === undefined) {
    lazyIndex = i;
  }

  var uname = this._getConstructorName();
  var args = [uname];
  var values = [this._constructor];
  var body = '  return function read' + uname + '(t, b) {\n';
  for (i = 0; i < wFields.length; i++) {
    if (i === lazyIndex) {
      body += '  if (!b) {\n';
    }
    field = type._fields[i];
    name = field._name;
    body += (~lazyIndex && i >= lazyIndex) ? '    ' : '  ';
    if (resolvers[name] === undefined) {
      args.push('t' + i);
      values.push(field._type);
      body += 't' + i + '._skip(t);\n';
    } else {
      args.push('t' + i);
      values.push(resolvers[name].resolver);
      body += 'var ' + resolvers[name].name + ' = ';
      body += 't' + i + '._read(t);\n';
    }
  }
  if (~lazyIndex) {
    body += '  }\n';
  }
  body += '  return new ' + uname + '(' + innerArgs.join() + ');\n};';

  resolver._read = new Function(args.join(), body).apply(undefined, values);
};

RecordType.prototype._match = function (tap1, tap2) {
  var fields = this._fields;
  var i, l, field, order, type;
  for (i = 0, l = fields.length; i < l; i++) {
    field = fields[i];
    order = field._order;
    type = field._type;
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
};

RecordType.prototype._checkFields = function (obj) {
  var keys = Object.keys(obj);
  var i, l;
  for (i = 0, l = keys.length; i < l; i++) {
    if (!this._fieldsMap[keys[i]]) {
      return false;
    }
  }
  return true;
};

RecordType.prototype._copy = function (val, opts) {
  // jshint -W058
  var hook = opts && opts.fieldHook;
  var values = [undefined];
  var i, l, field, value;
  for (i = 0, l = this._fields.length; i < l; i++) {
    field = this._fields[i];
    value = val[field._name];
    if (value === undefined && field.hasOwnProperty('getDefault')) {
      value = field.getDefault();
    } else {
      value = field._type._copy(val[field._name], opts);
    }
    if (hook) {
      value = hook(field, value, this);
    }
    values.push(value);
  }
  return new (this._constructor.bind.apply(this._constructor, values));
};

RecordType.prototype.compare = function (val1, val2) {
  var fields = this._fields;
  var i, l, field, name, order, type;
  for (i = 0, l = fields.length; i < l; i++) {
    field = fields[i];
    name = field._name;
    order = field._order;
    type = field._type;
    if (order) {
      order *= type.compare(val1[name], val2[name]);
      if (order) {
        return order;
      }
    }
  }
  return 0;
};

RecordType.prototype.random = function () {
  // jshint -W058
  var fields = this._fields.map(function (f) { return f._type.random(); });
  fields.unshift(undefined);
  return new (this._constructor.bind.apply(this._constructor, fields));
};

RecordType.prototype.getAliases = function () { return this._aliases; };

RecordType.prototype.getField = function (name) {
  return this._fieldsMap[name];
};

RecordType.prototype.getFields = function () { return this._fields.slice(); };

RecordType.prototype.getRecordConstructor = function () {
  return this._constructor;
};

RecordType.prototype.getTypeName = function () {
  return this._isError ? 'error' : 'record';
};

RecordType.prototype.toJSON = function () {
  // The nested JSONification of fields isn't required for `getSchema` (it
  // would call it recursively anyway), but it makes other things simpler by
  // letting us return valid "canonical attributes" directly (e.g. meta types).
  return {
    name: this._name,
    type: this.getTypeName(),
    fields: this._fields.map(function (f) { return f.toJSON(); }),
    aliases: this._aliases
  };
};

/**
 * Derived type abstract class.
 *
 */
function LogicalType(attrs, opts) {
  this._logicalTypeName = attrs.logicalType;
  Type.call(this);
  LOGICAL_TYPE = this;
  this._underlyingType = createType(attrs, opts);
}
util.inherits(LogicalType, Type);

LogicalType.prototype.getTypeName = function () {
  return 'logical:' + this._logicalTypeName;
};

LogicalType.prototype.getUnderlyingType = function () {
  return this._underlyingType;
};

LogicalType.prototype._read = function (tap) {
  return this._fromValue(this._underlyingType._read(tap));
};

LogicalType.prototype._write = function (tap, any) {
  this._underlyingType._write(tap, this._toValue(any));
};

LogicalType.prototype._check = function (any, flags, hook, path) {
  try {
    var val = this._toValue(any);
  } catch (err) {
    // Handled below.
  }
  if (val === undefined) {
    if (hook) {
      hook(any, this);
    }
    return false;
  }
  return this._underlyingType._check(val, flags, hook, path);
};

LogicalType.prototype._copy = function (any, opts) {
  var type = this._underlyingType;
  switch (opts && opts.coerce) {
    case 3: // To string.
      return type._copy(this._toValue(any), opts);
    case 2: // From string.
      return this._fromValue(type._copy(any, opts));
    default: // Normal copy.
      return this._fromValue(type._copy(this._toValue(any), opts));
  }
};

LogicalType.prototype._update = function (resolver, type, opts) {
  var _fromValue = this._resolve(type, opts);
  if (_fromValue) {
    resolver._read = function (tap) { return _fromValue(type._read(tap)); };
  }
};

LogicalType.prototype.random = function () {
  return this._fromValue(this._underlyingType.random());
};

LogicalType.prototype.compare = function (obj1, obj2) {
  var val1 = this._toValue(obj1);
  var val2 = this._toValue(obj2);
  return this._underlyingType.compare(val1, val2);
};

LogicalType.prototype.toJSON = function () {
  var attrs = this.getUnderlyingType().toJSON();
  if (EXPORT_ATTRS) {
    if (typeof attrs == 'string') {
      attrs = {type: attrs};
    }
    attrs.logicalType = this._logicalTypeName;
    this._export(attrs);
  }
  return attrs;
};

// Unlike the other methods below, `_export` has a reasonable default which we
// can provide (not exporting anything).
LogicalType.prototype._export = function (/* attrs */) {};

// Methods to be implemented.
LogicalType.prototype._fromValue = utils.abstractFunction;
LogicalType.prototype._toValue = utils.abstractFunction;
LogicalType.prototype._resolve = utils.abstractFunction;


// General helpers.

/**
 * Customizable long.
 *
 * This allows support of arbitrarily large long (e.g. larger than
 * `Number.MAX_SAFE_INTEGER`). See `LongType.__with` method above. Note that we
 * can't use a logical type because we need a "lower-level" hook here: passing
 * through through the standard long would cause a loss of precision.
 *
 */
function AbstractLongType(noUnpack) {
  LongType.call(this);
  this._noUnpack = !!noUnpack;
}
util.inherits(AbstractLongType, LongType);

AbstractLongType.prototype._check = function (val, flags, hook) {
  var b = this._isValid(val);
  if (!b && hook) {
    hook(val, this);
  }
  return b;
};

AbstractLongType.prototype._read = function (tap) {
  var buf, pos;
  if (this._noUnpack) {
    pos = tap.pos;
    tap.skipLong();
    buf = tap.buf.slice(pos, tap.pos);
  } else {
    buf = tap.unpackLongBytes(tap);
  }
  if (tap.isValid()) {
    return this._fromBuffer(buf);
  }
};

AbstractLongType.prototype._write = function (tap, val) {
  if (!this._isValid(val)) {
    throwInvalidError(val, this);
  }
  var buf = this._toBuffer(val);
  if (this._noUnpack) {
    tap.writeFixed(buf);
  } else {
    tap.packLongBytes(buf);
  }
};

AbstractLongType.prototype._copy = function (val, opts) {
  switch (opts && opts.coerce) {
    case 3: // To string.
      return this._toJSON(val);
    case 2: // From string.
      return this._fromJSON(val);
    default: // Normal copy.
      // Slow but guarantees most consistent results. Faster alternatives would
      // require assumptions on the long class used (e.g. immutability).
      return this._fromJSON(JSON.parse(JSON.stringify(this._toJSON(val))));
  }
};

AbstractLongType.prototype.random = function () {
  return this._fromJSON(LongType.prototype.random());
};

// Methods to be implemented by the user.
AbstractLongType.prototype._fromBuffer = utils.abstractFunction;
AbstractLongType.prototype._toBuffer = utils.abstractFunction;
AbstractLongType.prototype._fromJSON = utils.abstractFunction;
AbstractLongType.prototype._toJSON = utils.abstractFunction;
AbstractLongType.prototype._isValid = utils.abstractFunction;
AbstractLongType.prototype.compare = utils.abstractFunction;

/**
 * Field.
 *
 * @param attrs {Object} The field's schema.
 * @para opts {Object} Schema parsing options (the same as `Type`s').
 *
 */
function Field(attrs, opts) {
  var name = attrs.name;
  if (typeof name != 'string' || !isValidName(name)) {
    throw new Error(f('invalid field name: %s', name));
  }

  this._name = name;
  this._type = createType(attrs.type, opts);
  this._aliases = attrs.aliases || [];

  this._order = (function (order) {
    switch (order) {
      case 'ascending':
        return 1;
      case 'descending':
        return -1;
      case 'ignore':
        return 0;
      default:
        throw new Error(f('invalid order: %j', order));
    }
  })(attrs.order === undefined ? 'ascending' : attrs.order);

  var value = attrs['default'];
  if (value !== undefined) {
    // We need to convert defaults back to a valid format (unions are
    // disallowed in default definitions, only the first type of each union is
    // allowed instead).
    // http://apache-avro.679487.n3.nabble.com/field-union-default-in-Java-td1175327.html
    var type = this._type;
    var val = type._copy(value, {coerce: 2, wrap: 2});
    // The clone call above will throw an error if the default is invalid.
    if (isPrimitive(type.getTypeName()) && type.getTypeName() !== 'bytes') {
      // These are immutable.
      this.getDefault = function () { return val; };
    } else {
      this.getDefault = function () { return type._copy(val); };
    }
  }
}

Field.prototype.getAliases = function () { return this._aliases; };

Field.prototype.getDefault = function () {}; // Undefined default.

Field.prototype.getName = function () { return this._name; };

Field.prototype.getOrder = function () {
  return ['descending', 'ignore', 'ascending'][this._order + 1];
};

Field.prototype.getType = function () { return this._type; };

Field.prototype.toJSON = function () {
  var val = this.getDefault();
  if (val !== undefined) {
    // We must both unwrap all unions and coerce buffers to strings.
    val = this._type._copy(val, {coerce: 3, wrap: 3});
  }
  return {
    name: this._name,
    type: this._type,
    'default': val,
    order: this.getOrder(),
    aliases: this._aliases
  };
};

Field.prototype.inspect = Field.prototype.toJSON;

/**
 * Resolver to read a writer's schema as a new schema.
 *
 * @param readerType {Type} The type to convert to.
 *
 */
function Resolver(readerType) {
  // Add all fields here so that all resolvers share the same hidden class.
  this._readerType = readerType;
  this._items = null;
  this._read = null;
  this._size = 0;
  this._symbols = null;
  this._values = null;
}

Resolver.prototype.inspect = function () { return '<Resolver>'; };

/**
 * Read a value from a tap.
 *
 * @param type {Type} The type to decode.
 * @param tap {Tap} The tap to read from. No checks are performed here.
 * @param resolver {Resolver} Optional resolver. It must match the input type.
 * @param lazy {Boolean} Skip trailing fields when using a resolver.
 *
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
 * Remove namespace from a name.
 *
 * @param name {String} Full or short name.
 *
 */
function unqualify(name) {
  var parts = name.split('.');
  return parts[parts.length - 1];
}

/**
 * Verify and return fully qualified name.
 *
 * @param name {String} Full or short name. It can be prefixed with a dot to
 * force global namespace.
 * @param namespace {String} Optional namespace.
 *
 */
function qualify(name, namespace) {
  if (~name.indexOf('.')) {
    name = name.replace(/^\./, ''); // Allow absolute referencing.
  } else if (namespace) {
    name = namespace + '.' + name;
  }
  name.split('.').forEach(function (part) {
    if (!isValidName(part)) {
      throw new Error(f('invalid name: %j', name));
    }
  });
  var tail = unqualify(name);
  // Primitives are always in the global namespace.
  return isPrimitive(tail) ? tail : name;
}

/**
 * Get all aliases for a type (including its name).
 *
 * @param obj {Type|Object} Typically a type or a field. Its aliases property
 * must exist and be an array.
 *
 */
function getAliases(obj) {
  var names = [];
  if (obj._name) {
    names.push(obj._name);
  }
  var aliases = obj._aliases;
  var i, l;
  for (i = 0, l = aliases.length; i < l; i++) {
    names.push(aliases[i]);
  }
  return names;
}

/**
 * Check whether a type's name is a primitive.
 *
 * @param name {String} Type name (e.g. `'string'`, `'array'`).
 *
 */
function isPrimitive(typeName) {
  // Since we use this module's own `TYPES` object, we can use `instanceof`.
  var type = TYPES[typeName];
  return type && type.prototype instanceof PrimitiveType;
}

/**
 * Return a type's class name from its Avro type name.
 *
 * We can't simply use `constructor.name` since it isn't supported in all
 * browsers.
 *
 * @param typeName {String} Type name.
 *
 */
function getClassName(typeName) {
  if (typeName === 'error') {
    typeName = 'record';
  } else {
    var match = /^([^:]+):(.*)$/.exec(typeName);
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
 *
 */
function readArraySize(tap) {
  var n = tap.readLong();
  if (n < 0) {
    n = -n;
    tap.skipLong(); // Skip size.
  }
  return n;
}

/**
 * Correctly stringify an object which contains types.
 *
 * @param obj {Object} The object to stringify. Typically, a type itself or an
 * object containing types. Any types inside will be expanded only once then
 * referenced by name.
 * @param opts {Object} Options:
 *  + `exportAttrs` {Boolean} Include field and logical type attributes.
 *  + `noDeref` {Boolean} Always reference types by name when possible,
 *    rather than expand it the first time it is encountered.
 *
 */
function stringify(obj, opts) {
  EXPORT_ATTRS = opts && opts.exportAttrs;
  var noDeref = opts && opts.noDeref;

  // Since JS objects are unordered, this implementation (unfortunately)
  // relies on engines returning properties in the same order that they are
  // inserted in. This is not in the JS spec, but can be "somewhat" safely
  // assumed (more here: http://stackoverflow.com/q/5525795/1062617).
  return (function (registry) {
    return JSON.stringify(obj, function (key, value) {
      if (value) {
        if (
          typeof value == 'object' &&
          value.hasOwnProperty('default') &&
          !value.hasOwnProperty('logicalType')
        ) {
          // This is a field.
          if (EXPORT_ATTRS) {
            return {
              name: value.name,
              type: value.type,
              'default': value['default'],
              order: value.order !== 'ascending' ? value.order : undefined,
              aliases: value.aliases.length ? value.aliases : undefined
            };
          } else {
            return {name: value.name, type: value.type};
          }
        } else if (value.aliases) {
          // This is a named type (enum, fixed, record, error).
          var name = value.name;
          if (name) {
            // If the type is anonymous, we always dereference it.
            if (noDeref || registry[name]) {
              return name;
            }
            registry[name] = true;
          }
          if (!EXPORT_ATTRS || !value.aliases.length) {
            value.aliases = undefined;
          }
        }
      }
      return value;
    });
  })({});
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
 * + We must remove one from each bound because of rounding errors.
 *
 */
function isSafeLong(n) {
  return n >= -9007199254740990 && n <= 9007199254740990;
}

/**
 * Check whether an object is the JSON representation of a buffer.
 *
 */
function isJsonBuffer(obj) {
  return obj && obj.type === 'Buffer' && Array.isArray(obj.data);
}

/**
 * Check whether a string is a valid Avro identifier.
 *
 */
function isValidName(str) { return NAME_PATTERN.test(str); }

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
 *
 */
function throwInvalidError(val, type) {
  throw new Error(f('invalid %s: %j', type, val));
}

/**
 * Get a type's bucket when included inside an unwrapped union.
 *
 * @param type {Type} Any type.
 *
 */
function getTypeBucket(type) {
  var typeName = type.getTypeName();
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
 *
 */
function getValueBucket(val) {
  if (val === null) {
    return 'null';
  }
  var bucket = typeof val;
  if (bucket === 'object') {
    // Could be bytes, fixed, array, map, or record.
    if (Array.isArray(val)) {
      return 'array';
    } else if (Buffer.isBuffer(val)) {
      return 'buffer';
    }
  }
  return bucket;
}


module.exports = {
  Type: Type,
  createType: createType,
  getTypeBucket: getTypeBucket,
  getValueBucket: getValueBucket,
  isValidName: isValidName,
  qualify: qualify,
  stringify: stringify,
  builtins: (function () {
    var types = {
      LogicalType: LogicalType,
      UnwrappedUnionType: UnwrappedUnionType,
      WrappedUnionType: WrappedUnionType
    };
    var typeNames = Object.keys(TYPES);
    var i, l, typeName;
    for (i = 0, l = typeNames.length; i < l; i++) {
      typeName = typeNames[i];
      types[getClassName(typeName)] = TYPES[typeName];
    }
    return types;
  })()
};
