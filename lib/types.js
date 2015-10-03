/* jshint node: true */

// TODO: Support JS keywords as record field names (e.g. `null`).
// TODO: Add logging using `debuglog` to help identify schema parsing errors.
// TODO: Add `decode` and `encode` methods back (see API for info).
// TODO: Add schema inference capabilities (as writable stream).
// TODO: Look into slow buffers for field defaults.

'use strict';


var Tap = require('./tap'),
    utils = require('./utils'),
    buffer = require('buffer'), // For `SlowBuffer`.
    crypto = require('crypto'),
    util = require('util');


// Avro primitive types.
var PRIMITIVES = [
  'null',
  'boolean',
  'int',
  'long',
  'float',
  'double',
  'bytes',
  'string'
];

// Allowed schema promotions.
var PROMOTIONS = {
  'int': ['long', 'float', 'double'],
  'long': ['float', 'double'],
  'float': ['double'],
  'string': ['bytes'],
  'bytes': ['string']
};

// Valid (field, type, and symbol) name regex.
var NAME_PATTERN = /^[A-Za-z_][A-Za-z0-9_]*$/;

// Custom error class, imported for convenience.
var AvscError = utils.AvscError;

// Random generator.
var RANDOM = new utils.Lcg();

// Encoding tap (shared for performance).
var TAP = new Tap(new buffer.SlowBuffer(1024));


/**
 * "Abstract" base Avro type class.
 *
 */
function Type(type) { this.type = type; }

/**
 * Create a registry with all the Avro primitive types defined.
 *
 * This function can be used for example to prime a custom registry to pass in
 * to `Type.fromSchema`.
 *
 */
Type.createRegistry = function () {
  var registry = {};
  var i, l, name;
  for (i = 0, l = PRIMITIVES.length; i < l; i++) {
    name = PRIMITIVES[i];
    registry[name] = new PrimitiveType(name);
  }
  return registry;
};

/**
 * Parse a schema and return the corresponding type.
 *
 * See `index.js` for option documentation.
 *
 */
Type.fromSchema = function (schema, opts) {
  opts = getOpts(schema, opts);

  var type;

  if (typeof schema == 'string') { // Type reference.
    if (
      opts.namespace &&
      !~schema.indexOf('.') &&
      !~PRIMITIVES.indexOf(schema) // Primitives can't be namespaced.
    ) {
      schema = opts.namespace + '.' + schema;
    }
    type = opts.registry[schema];
    if (type) {
      return type;
    }
    throw new AvscError('missing name: %s', schema);
  }

  type = (function () {
    if (schema instanceof Array) {
      return new UnionType(schema, opts);
    }
    switch (schema.type) {
      case 'null':
      case 'boolean':
      case 'int':
      case 'long':
      case 'float':
      case 'double':
      case 'bytes':
      case 'string':
        return new PrimitiveType(schema.type);
        // We create new primitives here to make type hooks more useful
        // (otherwise it wouldn't be possible to make modification to just one
        // primitive). Primitives referred to by name are still shared though
        // (the most common case).
      case 'array':
        return new ArrayType(schema, opts);
      case 'enum':
        return new EnumType(schema, opts);
      case 'fixed':
        return new FixedType(schema, opts);
      case 'map':
        return new MapType(schema, opts);
      case 'record':
        return new RecordType(schema, opts);
      default:
        throw new AvscError('unknown type: %j', schema.type);
    }
  })();

  if (opts.typeHook) {
    opts.typeHook.call(type, schema);
  }
  return type;
};

/**
 * Base decoding method.
 *
 * This method should always be called with a tap as context. For example:
 * `type._read.call(tap)`. It is also important to remember to check that the
 * tap is valid after a read.
 *
 */
Type.prototype._read = utils.abstractFunction;

/**
 * Skip reading a type.
 *
 * Similarly to `_read` above, this method should be called with a tap as
 * context: `type._skip.call(tap)`.
 *
 */
Type.prototype._skip = utils.abstractFunction;

/**
 * Encode a type instance.
 *
 * @param obj {Object} The object to encode.
 *
 * Similarly to `_read` above, this method should be called with a tap as
 * context: `type._write.call(tap, obj)`. The tap should be checked for
 * validity afterwards as well.
 *
 */
Type.prototype._write = utils.abstractFunction;

/**
 * Generate an resolver for the given type.
 *
 * @param type {Type} Writer type.
 * @param opts {Object} Options:
 *
 *  + `registry` {Object}
 *
 * This method should not be called directly! It is called internally by
 * `createResolver`, which handles things like unions properly. For convenience,
 * `createResolver` will also raise an error if `_createResolver` returns a
 * false-ish value.
 *
 */
Type.prototype._createResolver = utils.abstractFunction;

Type.prototype.clone = utils.abstractFunction;

Type.prototype.isValid = utils.abstractFunction;

Type.prototype.random = utils.abstractFunction;

Type.prototype.createResolver = function (type, opts) {
  opts = opts || {};
  opts.registry = opts.registry || {};

  var resolver, key;
  if (this.type === 'record' && type.type === 'record') {
    key = this.name + ':' + type.name; // ':' is illegal in Avro type names.
    resolver = opts.registry[key];
    if (resolver) {
      return resolver;
    }
  }

  resolver = new Resolver(this, type);

  if (key) { // Register resolver early for recursive schemas.
    opts.registry[key] = resolver;
  }

  if (type instanceof UnionType) {
    var resolvers = type.types.map(function (t) {
      return this.createResolver(t, opts);
    }, this);
    resolver._read = function () {
      var index = this.readLong();
      var resolver = resolvers[index];
      if (resolver === undefined) {
        throw new AvscError('invalid union index: %s', index);
      }
      return resolvers[index]._read.call(this);
    };
  } else {
    resolver._read = this._createResolver(type, opts);
  }

  if (!resolver._read) {
    throw new AvscError('incompatible types: %j %j', this, type);
  }
  return resolver;
};

Type.prototype.fromBuffer = function (buf, resolver, noCheck) {
  var tap = new Tap(buf);
  var reader;
  if (resolver) {
    if (resolver._readerType !== this) {
      throw new AvscError('invalid resolver');
    }
    reader = resolver._read;
  } else {
    reader = this._read;
  }
  var obj = reader.call(tap, noCheck); // Enable lazy reading when possible.
  if (!tap.isValid()) {
    throw new AvscError('truncated buffer');
  }
  if (!noCheck && tap.pos < buf.length) {
    throw new AvscError('trailing data');
  }
  return obj;
};

Type.prototype.toBuffer = function (obj, noCheck) {
  if (!noCheck) {
    checkValid(this, obj);
  }
  TAP.pos = 0;
  this._write.call(TAP, obj);
  if (!TAP.isValid()) {
    Type.__reset(2 * TAP.pos);
    TAP.pos = 0;
    this._write.call(TAP, obj);
  }
  var buf = new Buffer(TAP.pos);
  TAP.buf.copy(buf, 0, 0, TAP.pos);
  return buf;
};

Type.prototype.fromString = function (str) {
  return this.clone(JSON.parse(str), {coerceBuffers: true});
};

Type.prototype.toString = function (obj) {
  if (obj === undefined) {
    // Return the canonical version of the schema.
    // Since JS objects are unordered, this implementation (unfortunately)
    // relies on engines returning properties in the same order that they are
    // inserted in. This is not in the JS spec, but can be "somewhat" safely
    // assumed (more here: http://stackoverflow.com/q/5525795/1062617).
    return (function (type, registry) {
      return JSON.stringify(type, function (key, value) {
        if (this instanceof Type && !~this._attributes.indexOf(key)) {
          return undefined; // Strip non-canonical keys.
        }
        if (value instanceof Type && value.name) {
          var name = value.name;
          if (registry[name]) {
            return name;
          }
          registry[name] = true;
        }
        return value;
      });
    })(this, {});
  }
  return JSON.stringify(obj, function (key, value) {
    if (value && value.type === 'Buffer' && value.data instanceof Array) {
      return new Buffer(value.data).toString('binary');
    } else {
      return value;
    }
  });
};

Type.prototype.createFingerprint = function (algorithm) {
  algorithm = algorithm || 'md5';
  var hash = crypto.createHash(algorithm);
  hash.end(this.toString());
  return hash.read();
};

Type.__reset = function (size) { TAP.buf = new buffer.SlowBuffer(size); };

// Implementations.

/**
 * Primitive Avro types.
 *
 * These are grouped together and all instances are typically shared across a
 * single schema (since the default registry defines their name).
 *
 */
function PrimitiveType(name) {
  Type.call(this, name);

  var s = utils.capitalize(name);
  this._read = Tap.prototype['read' + s];
  this._skip = Tap.prototype['skip' + s];
  this._write = Tap.prototype['write' + s];

  switch (name) {
    case 'null':
      this.isValid = function (o) { return o === null; };
      this.random = function () { return null; };
      break;
    case 'boolean':
      this.isValid = function (o) { /* jshint -W018 */ return o === !!o; };
      this.random = function () { return RANDOM.nextBoolean(); };
      break;
    case 'int':
      this.isValid = function (o) { return o === (o | 0); };
      this.random = function () { return RANDOM.nextInt(1000) | 0; };
      break;
    case 'long':
      this.isValid = function (o) {
        return typeof o == 'number' &&
          o % 1 === 0 &&
          o <= Number.MAX_SAFE_INTEGER &&
          o >= Number.MIN_SAFE_INTEGER; // Can't capture full range sadly.
      };
      this.random = function () { return RANDOM.nextInt(); };
      break;
    case 'float':
      this.isValid = function (o) {
        return typeof o == 'number' && Math.abs(o) < 3.4028234e38;
      };
      this.random = function () { return RANDOM.nextFloat(1e3); };
      break;
    case 'double':
      this.isValid = function (o) { return typeof o == 'number'; };
      this.random = function () { return RANDOM.nextFloat(); };
      break;
    case 'string':
      this.isValid = function (o) { return typeof o == 'string'; };
      this.random = function () {
        return RANDOM.nextString(RANDOM.nextInt(32));
      };
      break;
    case 'bytes':
      this.isValid = Buffer.isBuffer;
      this.random = function () {
        return RANDOM.nextBuffer(RANDOM.nextInt(32));
      };
      break;
    default:
      throw new AvscError('invalid primitive type: %j', name);
  }
}
util.inherits(PrimitiveType, Type);

PrimitiveType.prototype.clone = function (obj, opts) {
  if (this.type === 'bytes') {
    obj = tryCloneBuffer(obj, opts && opts.coerceBuffers);
  }
  checkValid(this, obj);
  return obj;
};

PrimitiveType.prototype._createResolver = function (type) {
  var name = this.type;
  if (
    type instanceof PrimitiveType &&
    (type.type === name || utils.contains(PROMOTIONS[type.type], name))
  ) {
    return this._read;
  }
};

PrimitiveType.prototype.toJSON = function () { return this.type; };


/**
 * Abstract base Avro union type.
 *
 * There are two types below:
 *
 *  + Wrapped in a single entry map (defined by the Avro specification). This
 *    makes encoding unambiguous but is overkill in most cases.
 *  + Unwrapped, which is simpler and slightly faster.
 *
 */
function UnionType(schema, opts) {
  if (!(schema instanceof Array)) {
    throw new AvscError('non-array union schema: %j', schema);
  }
  if (!schema.length) {
    throw new AvscError('empty union');
  }

  opts = getOpts(schema, opts);

  Type.call(this, 'union');
  this.types = schema.map(function (o) { return Type.fromSchema(o, opts); });

  var longReader = Tap.prototype.readLong;
  var longWriter = Tap.prototype.writeLong;
  var readers = this.types.map(function (type) { return type._read; });
  var skippers = this.types.map(function (type) { return type._skip; });
  var writers = this.types.map(function (type) { return type._write; });
  var indices = {};
  var i, l, type, name;
  for (i = 0, l = this.types.length; i < l; i++) {
    type = this.types[i];
    name = type.name || type.type;
    if (indices[name] !== undefined) {
      throw new AvscError('duplicate union name: %j', name);
    }
    indices[name] = i;
  }

  var constructors = this.types.map(function (type) {
    // jshint -W054
    var name = type.name || type.type;
    if (name === 'null') {
      return null;
    }
    var body;
    if (~name.indexOf('.')) { // Qualified name.
      body = 'this[\'' + name + '\'] = obj;';
    } else {
      body = 'this.' + name + ' = obj;';
    }
    return new Function('obj', body);
  });

  this._read = function () {
    var index = longReader.call(this);
    var Class = constructors[index];
    if (Class) {
      return new Class(readers[index].call(this));
    } else if (Class === null) {
      return null;
    } else {
      throw new AvscError('invalid union index: %s', index);
    }
  };

  this._skip = function () { skippers[longReader.call(this)].call(this); };

  this._write = function (obj) {
    var name = obj === null ? 'null' : Object.keys(obj)[0];
    var index = indices[name];
    longWriter.call(this, index);
    if (obj !== null) {
      writers[index].call(this, obj[name]);
    }
  };

  this._createResolver = function (type, opts) {
    // jshint -W083
    // (The loop exits after the first function is created.)
    var i, l, resolver, Class;
    for (i = 0, l = this.types.length; i < l; i++) {
      try {
        resolver = this.types[i].createResolver(type, opts);
      } catch (err) {
        continue;
      }
      Class = constructors[i];
      if (Class) {
        return function () { return new Class(resolver._read.call(this)); };
      } else {
        return function () { return null; };
      }
    }
  };

  this.clone = function (obj, opts) {
    if (opts && opts.wrapUnions) {
      // Promote first union values to unions (useful when parsing defaults).
      if (obj === null && constructors[0] === null) {
        return null;
      }
      return new constructors[0](this.types[0].clone(obj, opts));
    } else {
      if (obj === null && indices['null'] !== undefined) {
        return null;
      }
      if (typeof obj == 'object') {
        var keys = Object.keys(obj);
        if (keys.length === 1) {
          var name = keys[0];
          var i = indices[name];
          if (i === undefined) {
            // We are a bit more flexible than in `isValid` here since we have
            // to deal with other serializers being less strict, so we fall
            // back to looking up unqualified names.
            var j, l, type;
            for (j = 0, l = this.types.length; j < l; j++) {
              type = this.types[j];
              if (type.name && name === unqualify(type.name)) {
                i = j;
                break;
              }
            }
          }
          if (i !== undefined) {
            return new constructors[i](this.types[i].clone(obj[name], opts));
          }
        }
      }
      checkValid(this, obj); // Will throw an exception.
    }
  };

  this.isValid = function (obj) {
    if (obj === null) {
      // Shortcut type lookup in this case.
      return indices['null'] !== undefined;
    }
    if (typeof obj != 'object') {
      return false;
    }
    var keys = Object.keys(obj);
    if (keys.length !== 1) {
      // We require a single key here to ensure that writes are correct and
      // efficient as soon as a record passes this check.
      return false;
    }
    var name = keys[0];
    var index = indices[name];
    if (index === undefined) {
      return false;
    }
    return this.types[index].isValid(obj[name]);
  };

  this.random = function () {
    var index = RANDOM.nextInt(this.types.length);
    var Class = constructors[index];
    if (!Class) {
      return null;
    }
    return new Class(this.types[index].random());
  };
}
util.inherits(UnionType, Type);

UnionType.prototype.toJSON = function () { return this.types; };


/**
 * Avro enum type.
 *
 */
function EnumType(schema, opts) {
  if (!(schema.symbols instanceof Array) || !schema.symbols.length) {
    throw new AvscError('invalid %j enum symbols: %j', schema.name, schema);
  }

  opts = getOpts(schema, opts);

  var self = this;
  var resolutions = resolveNames(schema, opts.namespace);
  var reader = Tap.prototype.readLong;
  var writer = Tap.prototype.writeLong;
  var indices = {};
  var i, l, symbol;
  for (i = 0, l = schema.symbols.length; i < l; i++) {
    symbol = schema.symbols[i];
    indices[symbol] = i;
    if (!NAME_PATTERN.test(symbol)) {
      throw new AvscError('invalid symbol name: %s', symbol);
    }
  }

  this.name = resolutions.name;
  Type.call(this, 'enum');
  this.symbols = schema.symbols;
  this.aliases = resolutions.aliases;
  this.doc = schema.doc;
  registerType(this, opts.registry);

  this._read = function () {
    var index = reader.call(this);
    var symbol = self.symbols[index];
    if (symbol === undefined) {
      throw new AvscError('invalid %s enum index: %s', self.name, index);
    }
    return symbol;
  };

  this._skip = Tap.prototype.skipLong;

  this._write = function (s) {
    var index = indices[s];
    writer.call(this, index);
  };

  this.isValid = function (s) {
    return typeof s == 'string' && indices[s] !== undefined;
  };
}
util.inherits(EnumType, Type);

EnumType.prototype.clone = function (obj) {
  checkValid(this, obj);
  return obj;
};

EnumType.prototype.random = function () {
  return RANDOM.choice(this.symbols);
};

EnumType.prototype._createResolver = function (type) {
  var symbols = this.symbols;
  if (
    type instanceof EnumType &&
    this.size === type.size &&
    ~getAliases(this).indexOf(type.name) &&
    type.symbols.every(function (s) { return ~symbols.indexOf(s); })
  ) {
    return type._read;
  }
};

EnumType.prototype._attributes = ['name', 'type', 'symbols'];


/**
 * Avro fixed type.
 *
 */
function FixedType(schema, opts) {
  if (schema.size !== (schema.size | 0) || schema.size < 1) {
    throw new AvscError('invalid %j fixed size: %j', schema.name, schema.size);
  }

  opts = getOpts(schema, opts);

  var self = this;
  var resolutions = resolveNames(schema, opts.namespace);
  var reader = Tap.prototype.readFixed;
  var skipper = Tap.prototype.skipFixed;
  var writer = Tap.prototype.writeFixed;

  this.name = resolutions.name;
  Type.call(this, 'fixed');
  this.size = schema.size;
  this.aliases = resolutions.aliases;
  registerType(this, opts.registry);

  this._read = function () { return reader.call(this, self.size); };

  this._skip = function () { skipper.call(this, self.size); };

  this._write = function (buf) { writer.call(this, buf, self.size); };
}
util.inherits(FixedType, Type);

FixedType.prototype.clone = function (obj, opts) {
  obj = tryCloneBuffer(obj, opts && opts.coerceBuffers);
  checkValid(this, obj);
  return obj;
};

FixedType.prototype.isValid = function (buf) {
  return Buffer.isBuffer(buf) && buf.length == this.size;
};

FixedType.prototype.random = function () {
  return RANDOM.nextBuffer(this.size);
};

FixedType.prototype._createResolver = function (type) {
  if (
    type instanceof FixedType &&
    this.size === type.size &&
    ~getAliases(this).indexOf(type.name)
  ) {
    return this._read;
  }
};

FixedType.prototype._attributes = ['name', 'type', 'size'];


/**
 * Avro map.
 *
 */
function MapType(schema, opts) {
  if (!schema.values) {
    throw new AvscError('missing map values: %j', schema);
  }

  opts = getOpts(schema, opts);

  Type.call(this, 'map');
  this.values = Type.fromSchema(schema.values, opts);

  var values = this.values;
  var reader = Tap.prototype.readMap;
  var skipper = Tap.prototype.skipMap;
  var writer = Tap.prototype.writeMap;

  this._read = function () { return reader.call(this, values._read); };

  this._skip = function () { skipper.call(this, values._skip); };

  this._write = function (obj) { writer.call(this, obj, values._write); };
}
util.inherits(MapType, Type);

MapType.prototype.clone = function (obj, opts) {
  if (obj && typeof obj == 'object' && !(obj instanceof Array)) {
    var keys = Object.keys(obj);
    var i, l, key;
    var copy = {};
    for (i = 0, l = keys.length; i < l; i++) {
      key = keys[i];
      copy[key] = this.values.clone(obj[key], opts);
    }
    return copy;
  }
  checkValid(this, obj); // Will throw.
};

MapType.prototype.isValid = function (obj) {
  if (!obj || typeof obj != 'object' || obj instanceof Array) {
    return false;
  }
  var keys = Object.keys(obj);
  var i, l;
  for (i = 0, l = keys.length; i < l; i++) {
    if (!this.values.isValid(obj[keys[i]])) {
      return false;
    }
  }
  return true;
};

MapType.prototype.random = function () {
  var obj = {};
  var i, l;
  for (i = 0, l = RANDOM.nextInt(10); i < l; i++) {
    obj[RANDOM.nextString(RANDOM.nextInt(20))] = this.values.random();
  }
  return obj;
};

MapType.prototype._createResolver = function (type, opts) {
  if (type instanceof MapType) {
    var reader = Tap.prototype.readMap;
    var resolver = this.values.createResolver(type.values, opts);
    return function () { return reader.call(this, resolver._read); };
  }
};

MapType.prototype._attributes = ['type', 'values'];


/**
 * Avro array.
 *
 */
function ArrayType(schema, opts) {
  if (!schema.items) {
    throw new AvscError('missing array items: %j', schema);
  }

  opts = getOpts(schema, opts);

  Type.call(this, 'array');
  this.items = Type.fromSchema(schema.items, opts);

  var items = this.items;
  var reader = Tap.prototype.readArray;
  var skipper = Tap.prototype.skipArray;
  var writer = Tap.prototype.writeArray;

  this._read = function () { return reader.call(this, items._read); };

  this._skip = function () { return skipper.call(this, items._skip); };

  this._write = function (arr) { writer.call(this, arr, items._write); };
}
util.inherits(ArrayType, Type);

ArrayType.prototype.clone = function (obj, opts) {
  if (obj instanceof Array) {
    var itemsType = this.items;
    return obj.map(function (elem) { return itemsType.clone(elem, opts); });
  }
  checkValid(this, obj); // Will throw.
};

ArrayType.prototype.isValid = function (obj) {
  if (!(obj instanceof Array)) {
    return false;
  }
  var itemsType = this.items;
  return obj.every(function (elem) { return itemsType.isValid(elem); });
};

ArrayType.prototype.random = function () {
  var arr = [];
  var i, l;
  for (i = 0, l = RANDOM.nextInt(10); i < l; i++) {
    arr.push(this.items.random());
  }
  return arr;
};

ArrayType.prototype._createResolver = function (type, opts) {
  if (type instanceof ArrayType) {
    var reader = Tap.prototype.readArray;
    var resolver = this.items.createResolver(type.items, opts);
    return function () { return reader.call(this, resolver._read); };
  }
};

ArrayType.prototype._attributes = ['type', 'items'];


/**
 * Avro record.
 *
 * A "specific record class" gets programmatically created for each record.
 * This gives significant speedups over using generics objects (~3 times
 * faster) and provides a custom constructor.
 *
 */
function RecordType(schema, opts) {
  opts = getOpts(schema, opts);

  var resolutions = resolveNames(schema, opts.namespace);

  this.name = resolutions.name;
  Type.call(this, 'record');
  this.doc = schema.doc;
  this.aliases = resolutions.aliases;
  registerType(this, opts.registry);

  if (!(schema.fields instanceof Array)) {
    throw new AvscError('non-array %s fields', this.name);
  }
  if (utils.hasDuplicates(schema.fields, function (f) { return f.name; })) {
    throw new AvscError('duplicate %s field name', this.name);
  }
  this.fields = schema.fields.map(function (f) { return new Field(f, opts); });

  this._construct = this._createConstructor();

  this._read = this._createReader();

  this._skip = this._createSkipper();

  this._write = this._createWriter();

  this.isValid = this._createChecker();
}
util.inherits(RecordType, Type);

RecordType.prototype.clone = function (obj, opts) {
  // jshint -W058
  var hook = opts && opts.fieldHook;
  var fields = this.fields.map(function (f) {
    var copy = f.type.clone(obj[f.name], opts);
    if (hook) {
      copy = hook.call(f, copy, this);
    }
    return copy;
  });
  fields.unshift(undefined);
  return new (this._construct.bind.apply(this._construct, fields));
};

RecordType.prototype.random = function () {
  // jshint -W058
  var fields = this.fields.map(function (f) { return f.type.random(); });
  fields.unshift(undefined);
  return new (this._construct.bind.apply(this._construct, fields));
};

RecordType.prototype.getRecordConstructor = function () {
  return this._construct;
};

RecordType.prototype._createConstructor = function () {
  // jshint -W054
  var outerArgs = [];
  var innerArgs = [];
  var innerBody = '';
  var ds = []; // Defaults.
  var i, l, field, name, getDefault;
  for (i = 0, l = this.fields.length; i < l; i++) {
    field = this.fields[i];
    getDefault = field.getDefault;
    name = field.name;
    innerArgs.push(name);
    innerBody += '  ';
    if (getDefault() === undefined) {
      innerBody += 'this.' + name + ' = ' + name + ';\n';
    } else {
      innerBody += 'if (' + name + ' === undefined) { ';
      innerBody += 'this.' + name + ' = d' + ds.length + '(); ';
      innerBody += '} else { this.' + name + ' = ' + name + '; }\n';
      outerArgs.push('d' + ds.length);
      ds.push(getDefault);
      // TODO: Optimize away function call for immutable defaults.
    }
  }
  var outerBody = 'return function ' + unqualify(this.name) + '(';
  outerBody += innerArgs.join() + ') {\n' + innerBody + '};';
  var Record = new Function(outerArgs.join(), outerBody).apply(undefined, ds);

  var self = this;
  Record.type = this;
  Record.prototype = {
    $isValid: function () { return self.isValid(this); },
    $toBuffer: function (noCheck) { return self.toBuffer(this, noCheck); },
    $toString: function () { return self.toString(this); },
    $type: this
  };
  // The names of these properties added to the prototype are prefixed with `$`
  // because it is an invalid property name in Avro but not in JavaScript.
  // (This way we are guaranteed not to be stepped over!)

  return Record;
};

RecordType.prototype._createReader = function () {
  // jshint -W054
  var uname = unqualify(this.name);
  var names = [];
  var values = [this._construct];
  var i, l;
  for (i = 0, l = this.fields.length; i < l; i++) {
    names.push('t' + i);
    values.push(this.fields[i].type);
  }
  var body = 'return function read' + uname + '() {\n';
  body += '  return new ' + uname + '(';
  body += names.map(function (t) { return t + '._read.call(this)'; }).join();
  body += ');\n};';
  names.unshift(uname);
  // We can do this since the JS spec guarantees that function arguments are
  // evaluated from left to right.
  return new Function(names.join(), body).apply(undefined, values);
};

RecordType.prototype._createSkipper = function () {
  // jshint -W054
  var args = [];
  var body = 'return function skip' + unqualify(this.name) + '() {\n';
  var values = [];
  var i, l;
  for (i = 0, l = this.fields.length; i < l; i++) {
    args.push('t' + i);
    values.push(this.fields[i].type);
    body += '  t' + i + '._skip.call(this);\n';
  }
  body += '}';
  return new Function(args.join(), body).apply(undefined, values);
};

RecordType.prototype._createWriter = function () {
  // jshint -W054
  // We still do default handling here, in case a normal JS object is passed.
  var args = [];
  var body = 'return function write' + unqualify(this.name) + '(obj) {\n';
  var values = [];
  var i, l, field, defaultValue;
  for (i = 0, l = this.fields.length; i < l; i++) {
    field = this.fields[i];
    args.push('t' + i);
    values.push(field.type);
    body += '  ';
    if (field.getDefault() === undefined) {
      body += 't' + i + '._write.call(this, obj.' + field.name + ');\n';
    } else {
      defaultValue = field.getDefault();
      // We use the default's value directly since it will be stored in the
      // writer's closure and won't be available to be modified.
      args.push('d' + i);
      values.push(defaultValue);
      body += 'var v' + i + ' = obj.' + field.name + '; ';
      body += 'if (v' + i + ' === undefined) { ';
      body += 'v' + i + ' = d' + i + ';';
      body += ' } t' + i + '._write.call(this, v' + i + ');\n';
    }
  }
  body += '}';
  return new Function(args.join(), body).apply(undefined, values);
};

RecordType.prototype._createChecker = function () {
  // jshint -W054
  var names = [];
  var values = [];
  var body = 'return function check' + unqualify(this.name) + '(obj) {\n';
  body += '  if (typeof obj != \'object\') { return false; }\n';
  var i, l, field;
  for (i = 0, l = this.fields.length; i < l; i++) {
    field = this.fields[i];
    names.push('t' + i);
    values.push(field.type);
    body += '  ';
    if (field.getDefault() === undefined) {
      body += 'if (!t' + i + '.isValid(obj.' + field.name + ')) { ';
      body += 'return false; }\n';
    } else {
      body += 'var v' + i + ' = obj.' + field.name + '; ';
      body += 'if (v' + i + ' !== undefined && ';
      body += '!t' + i + '.isValid(v' + i + ')) { ';
      body += 'return false; }\n';
    }
  }
  body += '  return true;\n};';
  return new Function(names.join(), body).apply(undefined, values);
};

RecordType.prototype._createResolver = function (type, opts) {
  // jshint -W054
  if (!~getAliases(this).indexOf(type.name)) {
    throw new AvscError('no alias for %s in %s', type.name, this.name);
  }

  var rFields = this.fields;
  var wFields = type.fields;
  var wFieldsMap = utils.toMap(wFields, function (f) { return f.name; });

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
      throw new AvscError('multiple matches for %s', field.name);
    }
    if (!matches.length) {
      if (field.getDefault() === undefined) {
        throw new AvscError('no match for default-less %s', field.name);
      }
      innerArgs.push('undefined');
    } else {
      name = matches[0];
      resolvers[name] = {
        resolver: field.type.createResolver(wFieldsMap[name].type, opts),
        name: field.name // Reader field name.
      };
      innerArgs.push(field.name);
    }
  }

  // See if we can add a bypass for unused fields at the end of the record.
  var lazyIndex = -1;
  i = wFields.length;
  while (i && resolvers[wFields[--i].name] === undefined) {
    lazyIndex = i;
  }

  var uname = unqualify(this.name);
  var args = [uname];
  var values = [this._construct];
  var body = '  return function read' + uname + '(lazy) {\n';
  for (i = 0; i < wFields.length; i++) {
    if (i === lazyIndex) {
      body += '  if (!lazy) {\n';
    }
    field = type.fields[i];
    name = field.name;
    body += i >= lazyIndex ? '    ' : '  ';
    if (resolvers[name] === undefined) {
      args.push('t' + i);
      values.push(field.type);
      body += 't' + i + '._skip.call(this);\n';
    } else {
      args.push('t' + i);
      values.push(resolvers[name].resolver);
      body += 'var ' + resolvers[name].name + ' = ';
      body += 't' + i + '._read.call(this);\n';
    }
  }
  if (~lazyIndex) {
    body += '  }\n';
  }
  body +=  '  return new ' + uname + '(' + innerArgs.join() + ');\n};';
  return new Function(args.join(), body).apply(undefined, values);
};

RecordType.prototype._attributes = ['name', 'type', 'fields'];

// General helpers.

/**
 * Field.
 *
 * @param schema {Object} The field's schema.
 * @para opts {Object} Schema parsing options (the same as `Type`s').
 *
 */
function Field(schema, opts) {
  var name = schema.name;
  if (typeof name != 'string' || !NAME_PATTERN.test(name)) {
    throw new AvscError('invalid field name: %s', name);
  }

  this.name = name;
  this.type = Type.fromSchema(schema.type, opts);
  this.aliases = schema.aliases;
  this.doc = schema.doc;
  this.order = schema.order; // Not used currently.

  var value = schema['default'];
  this['default'] = value;
  if (value !== undefined) {
    // Default might be mutable, so we wrap it inside a function. We also need
    // to convert it back to a valid format (unions are disallowed in default
    // definitions, only the first type of each union is allowed instead).
    // http://apache-avro.679487.n3.nabble.com/field-union-default-in-Java-td1175327.html
    var type = this.type;
    var obj = type.clone(value, {coerceBuffers: true, wrapUnions: true});
    // The clone call above will (correctly) throw an error if the default is
    // invalid.
    this.getDefault = function () { return type.clone(obj); };
  }
}

Field.prototype.getDefault = function () { return; }; // Undefined default.

Field.prototype.toJSON = function () {
  return {name: this.name, type: this.type, 'default': this['default']};
};

/**
 * Resolver to read a writer's schema as a new schema.
 *
 */
function Resolver(readerType, writerType) {
  this._readerType = readerType;
  this._writerType = writerType; // Not used currently.
  this._read = null; // Added afterwards (late binding to support recursion).
}

/**
 * Create default parsing options.
 *
 * @param opts {Object} Base options.
 *
 */
function getOpts(schema, opts) {
  if (schema === null) {
    // Let's be helpful for this common error.
    throw new AvscError('invalid type: null (did you mean "null"?)');
  }
  opts = opts || {};
  opts.registry = opts.registry || Type.createRegistry();
  opts.namespace = schema.namespace || opts.namespace;
  return opts;
}

/**
 * Resolve a schema's name and aliases.
 *
 * @param schema {Object} True schema (can't be a string).
 * @param namespace {String} Optional parent namespace.
 *
 */
function resolveNames(schema, namespace) {
  namespace = schema.namespace || namespace;

  var name = schema.name;
  if (!name) {
    throw new AvscError('no name in schema: %j', schema);
  }
  var resolutions = {name: resolve(name)};
  if (schema.aliases) {
    resolutions.aliases = schema.aliases.map(resolve);
  }
  return resolutions;

  function resolve(name) {
    if (!~name.indexOf('.') && namespace) {
      name = namespace + '.' + name;
    }
    var tail = unqualify(name);
    if (~PRIMITIVES.indexOf(tail)) {
      throw new AvscError('cannot rename: %s', tail);
    }
    return name;

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
 * Get all aliases for a type (including its name).
 *
 * @param obj {Type|Object} Typically a type or a field.
 *
 */
function getAliases(obj) {
  var names = [obj.name];
  var aliases = obj.aliases;
  var i, l;
  if (aliases) {
    for (i = 0, l = aliases.length; i < l; i++) {
      names.push(aliases[i]);
    }
  }
  return names;
}

/**
 * Register a type using its name.
 *
 * @param type {Type} The type to register. This must be a "named" type (one of
 * fixed, enum, or record). Its name must already have been resolved.
 * @param registry {Object} Where to store the types.
 *
 * Registering the name name multiple times for a different type will raise an
 * error. Note also that aliases aren't registered (aliases are only used when
 * adapting a reader's schema).
 *
 */
function registerType(type, registry) {
  var name = type.name;
  var prev = registry[name];
  if (prev !== undefined && prev !== type) {
    throw new AvscError('duplicate type name: %s', name);
  }
  registry[name] = type;
}

/**
 * Check whether an object is valid, and throw otherwise.
 *
 * @param type {Type} The type to try validating to.
 * @param obj {Object} The object to validate.
 *
 */
function checkValid(type, obj) {
  if (!type.isValid(obj)) {
    throw new AvscError('invalid %j: %j', type, obj);
  }
}

/**
 * Try cloning an object to a buffer.
 *
 * @param obj {Object} Object to copy to a buffer.
 * @param coerce {Boolean} Allow some conversions.
 *
 */
function tryCloneBuffer(obj, coerce) {
  if (Buffer.isBuffer(obj)) {
    // Check this first (should be most common case).
    return new Buffer(obj);
  }
  if (coerce) {
    if (typeof obj == 'string') {
      return new Buffer(obj, 'binary');
    } else if (obj.type === 'Buffer' && obj.data instanceof Array) {
      return new Buffer(obj.data);
    }
  }
  return obj;
}


module.exports = {
  ArrayType: ArrayType,
  EnumType: EnumType,
  FixedType: FixedType,
  MapType: MapType,
  PrimitiveType: PrimitiveType,
  RecordType: RecordType,
  Type: Type,
  UnionType: UnionType
};
