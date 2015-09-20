/* jshint node: true */

'use strict';

// TODO: Implement `typeHook`. (Called after each type is instantiated.)
// TODO: Add `equals` and `compare` method to each type.
// TODO: Fail when writing or reading fails with an error other than out of
// bounds. (Maybe use custom decode and encode errors?)
// TODO: Implement `asReaderOf` (including aliases support).
// TODO: Add regex check for valid type and field names.
// TODO: Add `decodeBuffers` option to coerce buffers into binary strings.
// TODO: Support JS keywords as record field names (e.g. `null`).

var Tap = require('./tap'),
    utils = require('./utils'),
    buffer = require('buffer'), // For `SlowBuffer`.
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

// Random generator.
var RANDOM = new utils.Lcg();


/**
 * Custom error, thrown when an invalid schema is encountered.
 *
 * @param message {String} Something useful. Any further arguments will be used
 * to format the message.
 *
 */
function AvscError(message) {

  Error.call(this);
  var l = arguments.length;
  if (l > 1) {
    var args = [message];
    var i;
    for (i = 1; i < l; i++) {
      args.push(arguments[i]);
    }
    this.message = util.format.apply(undefined, args);
  } else {
    this.message = message;
  }

}
util.inherits(AvscError, Error);


/**
 * Parse a schema.
 *
 * @param schema {Object|String} Schema (type object or type name string).
 * @param opts {Object} Parsing options. The following keys are currently
 * supported:
 *
 * + `namespace` Optional parent namespace.
 * + `registry` Optional registry of predefined type names.
 * + `unwrapUnions` By default, Avro expects all unions to be wrapped inside an
 *   object with a single key. Setting this to `true` will prevent this.
 *   (Defaults to `false`.)
 *
 */
function parse(schema, opts) {

  if (schema instanceof Type) {
    return schema;
  }

  opts = getOpts(schema, opts);

  var UnionType, type;

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

  if (schema instanceof Array) { // Union.
    UnionType = opts.unwrapUnions ? UnwrappedUnionType : WrappedUnionType;
    return new UnionType(schema, opts);
  }

  type = schema.type;
  switch (type) { // Non-union complex types.
    case 'null':
    case 'boolean':
    case 'int':
    case 'long':
    case 'float':
    case 'double':
    case 'bytes':
    case 'string':
      return opts.registry[type]; // Reuse primitive instances.
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
      throw new AvscError('unknown type: %j', type);
  }

}


/**
 * "Abstract" base Avro type class.
 *
 * Not meant to be instantiated directly, but via the `parse` function above.
 *
 */
function Type() {}

/**
 * Initialize registry, defining all primitives.
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
 * Base decoding method.
 *
 * This method should always be called with a tap as context. For example:
 * `type._read.call(tap)`. It is also important to remember to check that the
 * tap is valid after a read.
 *
 */
Type.prototype._read = function () { throw new Error('abstract'); };

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
Type.prototype._write = function (obj) {

  throw new Error('abstract'); // jshint unused: false

};

/**
 * Decode Avro bytes.
 *
 * @param buf {Buffer} Avro representation of an object.
 *
 */
Type.prototype.decode = function (buf) {

  var tap = new Tap(buf);
  var obj = this._read.call(tap);
  if (!tap.isValid()) {
    throw new AvscError('truncated buffer');
  }
  return obj;

};

/**
 * Encode object.
 *
 * @param obj {Object} The object to encode. Depending on the type, it can be a
 * number, a string, an array, or an object.
 * @param opts {Object} Optional encoding options:
 *
 *  + `buffer`, used to serialize the object into (a slice will be returned).
 *    If not passed, or if the serialized object doesn't fit into the passed
 *    buffer, a new one will be created.
 *  + `unsafe`, bypass validity checks.
 *
 */
Type.prototype.encode = function (obj, opts) {

  if ((!opts || !opts.unsafe) && !this.isValid(obj)) {
    throw new AvscError('invalid object');
  }

  var tap = new Tap((opts && opts.buffer) || new Buffer(1024));
  this._write.call(tap, obj);
  if (!tap.isValid()) {
    // We overflowed the buffer, need to resize.
    tap.buf = new Buffer(tap.pos);
    tap.pos = 0;
    this._write.call(tap, obj);
  }

  return tap.buf.slice(0, tap.pos);

};

/**
 * Check that the object can be encoded by this type.
 *
 * @param obj {Object} The object to check.
 *
 */
Type.prototype.isValid = function (obj) {

  throw new Error('abstract'); // jshint unused: false

};

/**
 * Generate a random instance of this type.
 *
 */
Type.prototype.random = function () { return new Error('abstract'); };


// Implementations.

/**
 * Primitive Avro types.
 *
 * These are grouped together and all instances are typically shared across a
 * single schema (since the default registry defines their name).
 *
 */
function PrimitiveType(name) {

  this.type = name;
  this._read = Tap.prototype['read' + utils.capitalize(name)];
  this._write = Tap.prototype['write' + utils.capitalize(name)];

  switch (name) {
    case 'null':
      this.isValid = function (o) { return o === null; };
      this.random = function () { return null; };
      break;
    case 'boolean':
      this.isValid = function (o) { /* jshint -W018 */ return o !== !o; };
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

PrimitiveType.prototype.toJSON = function () { return this.type; };


/**
 * Abstract base Avro union type.
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

  this.type = 'union';
  this.types = schema.map(function (o) { return parse(o, opts); });

}
util.inherits(UnionType, Type);

UnionType.prototype.toJSON = function () { return this.types; };


/**
 * Wrapped Avro union type.
 *
 * This is the type required by the spec.
 *
 */
function WrappedUnionType(schema, opts) {

  UnionType.call(this, schema, opts);

  var longReader = Tap.prototype.readLong;
  var longWriter = Tap.prototype.writeLong;
  var readers = this.types.map(function (type) { return type._read; });
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
    } else {
      return null;
    }
  };

  this._write = function (obj) {
    if (typeof obj != 'object') {
      throw new AvscError('invalid union: ' + obj);
    }
    var name = obj === null ? 'null' : Object.keys(obj)[0];
    var index = indices[name];
    if (index === undefined) {
      throw new AvscError('no such name in union: ' + name);
    }
    longWriter.call(this, index);
    if (obj !== null) {
      writers[index].call(this, obj[name]);
    }
  };

  this.isValid = function (obj) {
    if (typeof obj != 'object') {
      return false;
    }
    if (obj === null) {
      return indices['null'] !== undefined;
    }
    var name = Object.keys(obj)[0];
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
util.inherits(WrappedUnionType, UnionType);


/**
 * Unwrapped Avro union type.
 *
 * This version is slightly more performant and sometimes more convenient (not
 * the default since it doesn't actually adhere to the Avro spec). See
 * `unwrapUnions` option for more information.
 *
 */
function UnwrappedUnionType(schema, opts) {

  UnionType.call(this, schema, opts);

  var self = this;
  var longReader = Tap.prototype.readLong;
  var longWriter = Tap.prototype.writeLong;
  var readers = this.types.map(function (type) { return type._read; });
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

  this._read = function () {
    return readers[longReader.call(this)].call(this);
  };

  this._write = function (obj) {
    var i, l, type;
    for (i = 0, l = self.types.length; i < l; i++) {
      type = self.types[i];
      if (type.isValid(obj)) {
        longWriter.call(this, i);
        type._write.call(this, obj);
        return;
      }
    }
    throw new Error('invalid union value: ' + obj);
  };

}
util.inherits(UnwrappedUnionType, UnionType);

UnwrappedUnionType.prototype.isValid = function (obj) {

  return this.types.some(function (type) { return type.isValid(obj); });

};

UnwrappedUnionType.prototype.random = function () {

  return RANDOM.choice(this.types).random();

};


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
  var i, l;
  for (i = 0, l = schema.symbols.length; i < l; i++) {
    indices[schema.symbols[i]] = i;
  }

  this.type = 'enum';
  this.doc = schema.doc;
  this.symbols = schema.symbols;
  this.aliases = resolutions.aliases;
  this.name = resolutions.name.qualified;
  registerType(this, opts.registry);

  this._read = function () { return self.symbols[reader.call(this)]; };

  this._write = function (s) {
    var index = indices[s];
    if (index === undefined) {
      throw new Error('invalid ' + self.name + ' enum value: ' + s);
    }
    writer.call(this, index);
  };

  this.isValid = function (s) {
    return typeof s == 'string' && indices[s] !== undefined;
  };

}
util.inherits(EnumType, Type);

EnumType.prototype.random = function () {

  return RANDOM.choice(this.symbols);

};


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
  var writer = Tap.prototype.writeFixed;

  this.type = 'fixed';
  this.size = schema.size;
  this.aliases = resolutions.aliases;
  this.name = resolutions.name.qualified;
  registerType(this, opts.registry);

  this._read = function () { return reader.call(this, self.size); };

  this._write = function (buf) { writer.call(this, buf, self.size); };

  this.isValid = function (buf) {
    return Buffer.isBuffer(buf) && buf.length == self.size;
  };

}
util.inherits(FixedType, Type);

FixedType.prototype.random = function () {

  return RANDOM.nextBuffer(this.size);

};


/**
 * Avro map.
 *
 */
function MapType(schema, opts) {

  if (!schema.values) {
    throw new AvscError('missing map values: %j', schema);
  }

  opts = getOpts(schema, opts);

  this.type = 'map';
  this.values = parse(schema.values, opts);

  var reader = Tap.prototype.readMap;
  var writer = Tap.prototype.writeMap;
  var valuesReader = this.values._read;
  var valuesWriter = this.values._write;

  this._read = function () { return reader.call(this, valuesReader); };

  this._write = function (obj) { writer.call(this, obj, valuesWriter); };

}
util.inherits(MapType, Type);

MapType.prototype.isValid = function (obj) {

  if (typeof obj != 'object' || obj instanceof Array) {
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


/**
 * Avro array.
 *
 */
function ArrayType(schema, opts) {

  if (!schema.items) {
    throw new AvscError('missing array items: %j', schema);
  }

  opts = getOpts(schema, opts);

  this.type = 'array';
  this.items = parse(schema.items, opts);

  var reader = Tap.prototype.readArray;
  var writer = Tap.prototype.writeArray;
  var itemsReader = this.items._read;
  var itemsWriter = this.items._write;

  this._read = function () { return reader.call(this, itemsReader); };

  this._write = function (arr) { writer.call(this, arr, itemsWriter); };

}
util.inherits(ArrayType, Type);

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


/**
 * Avro record.
 *
 * A "specific record class" gets programmatically generated for each record.
 * This gives significant speedups over using generics objects (~3 times
 * faster) and provides a custom constructor.
 *
 */
function RecordType(schema, opts) {

  // jshint -W054

  opts = getOpts(schema, opts);

  var self = this;
  var resolutions = resolveNames(schema, opts.namespace);
  var unqualifiedName = resolutions.name.unqualified;

  this.type = 'record';
  this.doc = schema.doc;
  this.aliases = resolutions.aliases;
  this.name = resolutions.name.qualified;
  registerType(this, opts.registry);

  if (!(schema.fields instanceof Array)) {
    throw new AvscError('non-array %s fields', this.name);
  }
  if (utils.hasDuplicates(schema.fields, function (f) { return f.name; })) {
    throw new AvscError('duplicate %s field name', this.name);
  }
  this.fields = schema.fields.map(function (field) {
    if (typeof field != 'object' || typeof field.name != 'string') {
      throw new AvscError('invalid %s field: %j', self.name, field);
    }
    var type = parse(field.type, opts);
    var value = field['default'];
    var getDefault;
    if (value !== undefined) {
      if (~['fixed', 'bytes'].indexOf(type.type)) {
        if (Buffer.isBuffer(value)) {
          // In case the default was instantiated programmatically. We set it
          // back on the field so that JSON serialization works as expected.
          value = field['default'] = value.toString('binary');
        }
        // Buffers are mutable (and can't be frozen), so we must create a new
        // instance each time.
        getDefault = function () { return new Buffer(value, 'binary'); };
      } else {
        // These defaults are immutable, so we can safely just return them.
        getDefault = function () { return value; };
      }
      // Avro forces the default value to be of the first type in the
      // enum, so we must do a bit of extra logic here.
      if (
        !(type.type === 'union' ? type.types[0] : type).isValid(getDefault())
      ) {
        throw new AvscError(
          'invalid default for %s\'s %s field: %j',
          self.name, field.name, value
        );
      }
    }
    return {
      name: field.name,
      doc: field.doc,
      type: type,
      'default': field['default'], // For JSON serialization.
      _getDefault: getDefault // For JS objects.
    };
    // Having the second default wrapped in a function has the added benefit
    // that it won't be included in the JSON serialization.
  });

  this._create = generateConstructor();
  this._create.decode = function (obj) { return self.decode(obj); };
  this._create.random = function () { return self.random(); };
  this._create.prototype = {
    $type: this,
    $encode: function (opts) { return self.encode(this, opts); },
    $isValid: function () { return self.isValid(this); }
  };
  // The names of these properties added to the prototype are prefixed with `$`
  // because it is an invalid property name in Avro but not in JavaScript.
  // (This way we are guaranteed not to be stepped over!)

  this._read = generateReader();

  this._write = generateWriter();

  this.isValid = generateChecker();

  function generateConstructor() {

    var outerArgs = [];
    var innerArgs = [];
    var innerBody = '';
    var ds = []; // Defaults.
    var i, l, field, name, getDefault;
    for (i = 0, l = self.fields.length; i < l; i++) {
      field = self.fields[i];
      getDefault = field._getDefault;
      name = field.name;
      innerArgs.push(name);
      innerBody += '  ';
      if (getDefault === undefined) {
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
    var outerBody = 'return function ' + unqualifiedName + '(';
    outerBody += innerArgs.join(',') + ') {\n' + innerBody + '};';
    return new Function(outerArgs.join(','), outerBody).apply(undefined, ds);

  }

  function generateReader() {

    var names = [];
    var values = [self._create];
    var i, l;
    for (i = 0, l = self.fields.length; i < l; i++) {
      names.push('r' + i);
      values.push(self.fields[i].type._read);
    }
    var body = 'return function read' + unqualifiedName + '() {\n';
    body += '  return new Class(';
    body += names.map(function (r) { return r + '.call(this)'; }).join(',');
    body += ');\n};';
    names.unshift('Class');
    return new Function(names.join(','), body).apply(undefined, values);

  }

  function generateWriter() {

    // We still do default handling here, in case a normal JS object is passed.

    var args = [];
    var body = 'return function write' + unqualifiedName + '(obj) {\n';
    var values = [];
    var i, l, field, defaultValue;
    for (i = 0, l = self.fields.length; i < l; i++) {
      field = self.fields[i];
      args.push('w' + i);
      values.push(field.type._write);
      body += '  ';
      if (field._getDefault === undefined) {
        body += 'w' + i + '.call(this, obj.' + field.name + ');\n';
      } else {
        defaultValue = field._getDefault();
        // We use the default's value directly since it will be stored in the
        // writer's closure and won't be available to be modified.
        if (Buffer.isBuffer(defaultValue)) {
          // We don't want to hold on to whole buffer slabs because of this
          // though, so we copy them to standalone buffers.
          defaultValue = new buffer.SlowBuffer(defaultValue.length);
          field._getDefault().copy(defaultValue);
        }
        args.push('d' + i);
        values.push(defaultValue);
        body += 'var v' + i + ' = obj.' + field.name + '; ';
        body += 'if (v' + i + ' === undefined) { ';
        body += 'v' + i + ' = d' + i + ';';
        body += ' } w' + i + '.call(this, v' + i + ');\n';
      }
    }
    body += '}';
    return new Function(args.join(','), body).apply(undefined, values);

  }

  function generateChecker() {

    var names = [];
    var values = [];
    var body = 'return function check' + unqualifiedName + '(obj) {\n';
    body += '  if (typeof obj != \'object\') { return false; }\n';
    var i, l, field;
    for (i = 0, l = self.fields.length; i < l; i++) {
      field = self.fields[i];
      names.push('f' + i);
      values.push(field.type);
      body += '  ';
      if (field._getDefault === undefined) {
        body += 'if (!f' + i + '.isValid(obj.' + field.name + ')) { ';
        body += 'return false; }\n';
      } else {
        body += 'var v' + i + ' = obj.' + field.name + '; ';
        body += 'if (v' + i + ' !== undefined && ';
        body += '!f' + i + '.isValid(v' + i + ')) { ';
        body += 'return false; }\n';
      }
    }
    body += 'return true;\n};';
    return new Function(names.join(','), body).apply(undefined, values);

  }

}
util.inherits(RecordType, Type);

RecordType.prototype.random = function () {

  // jshint -W058

  var fields = [undefined];
  var i, l;
  for (i = 0, l = this.fields.length; i < l; i++) {
    fields.push(this.fields[i].type.random());
  }
  return new (this._create.bind.apply(this._create, fields));

};

/**
 * Get class record instances are made of.
 *
 * It can be instantiated and used directly!
 *
 */
RecordType.prototype.getRecordConstructor = function () {

  return this._create;

};

/**
 * Adapt a type to read data written by another, compatible, type.
 *
 * @param type Writer type.
 *
 */
RecordType.prototype.asReaderOf = function (type) {

  return new Error('not yet implemented'); // jshint unused: false

};


// Helpers.

/**
 * Create default parsing options.
 *
 * @param opts {Object} Base options.
 *
 */
function getOpts(schema, opts) {

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
    resolutions.aliases = schema.aliases.map(function (alias) {
      return resolve(alias).qualified;
    });
  }
  return resolutions;

  function resolve(name) {

    if (!~name.indexOf('.') && namespace) {
      name = namespace + '.' + name;
    }

    var parts = name.split('.');
    var tail = parts[parts.length - 1];
    if (~PRIMITIVES.indexOf(tail)) {
      throw new AvscError('cannot rename: %s', tail);
    }

    return {qualified: name, unqualified: tail};

  }

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


module.exports = {
  AvscError: AvscError,
  parse: parse,
  types : {
    ArrayType: ArrayType,
    EnumType: EnumType,
    FixedType: FixedType,
    MapType: MapType,
    PrimitiveType: PrimitiveType,
    RecordType: RecordType,
    Type: Type,
    UnionType: UnionType,
    UnwrappedUnionType: UnwrappedUnionType,
    WrappedUnionType: WrappedUnionType
  }
};
