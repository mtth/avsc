/* jshint node: true */

// TODO: Add `random` method to each type.
// TODO: Move type subclasses' methods to prototypes.
// TODO: Add aliases support.
// TODO: Add `equals` and `compare` method to each type.

'use strict';

var Tap = require('./tap'),
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


/**
 * Custom error, thrown when an invalid schema is encountered.
 *
 * @param message {String} Something useful.
 *
 */
function ParseError(message) {

  Error.call(this);
  this.message = message;

}
util.inherits(ParseError, Error);


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
    throw new Error('missing name: ' + schema);
  }

  if (schema instanceof Array) { // Union.
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
      throw new ParseError('unknown type: ' + type);
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

  // jshint unused: false

  throw new Error('abstract');

};

/**
 * Validity check.
 *
 * @param obj {Object} The object to check for validity.
 *
 */
Type.prototype._check = function (obj) {

  // jshint unused: false

  throw new Error('abstract');

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
    throw new Error('invalid bytes');
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
 *  + `size`, used to initialize the buffer. Defaults to 1024 bytes.
 *  + `unsafe`, bypass validity checks.
 *
 */
Type.prototype.encode = function (obj, opts) {

  if ((!opts || !opts.unsafe) && !this.validate(obj)) {
    throw new Error('incompatible object');
  }

  var size = (opts ? opts.size : 0) || 1024;
  var tap = new Tap(new Buffer(size));
  do {
    this._write.call(tap, obj);
  } while (!isValid());

  return tap.buf.slice(0, tap.offset);

  // Check that the tap is valid, else resize the underlying buffer.
  function isValid() {
    if (tap.isValid()) {
      return true;
    }
    size *= 2;
    tap.buf = new Buffer(size);
    tap.offset = 0;
    return false;
  }

};

/**
 * Check that the object can be encoded by this type.
 *
 * @param obj {Object} The object to check.
 *
 */
Type.prototype.validate = function (obj) { return this._check(obj); };


// Implementations.

function PrimitiveType(name) {

  this.name = name;
  this._check = getChecker();
  this._read = Tap.prototype['read' + capitalize(this.name)];
  this._write = Tap.prototype['write' + capitalize(this.name)];

  if (!this._check) {
    throw new ParseError('invalid primitive type: ' + name);
  }

  function getChecker() {

    switch (name) {
      case 'null':
        return function (o) { return o === null; };
      case 'boolean':
        return function (o) { return o === !!o; }; // jshint ignore: line
      case 'int':
        return function (o) { return o === (o | 0); };
      case 'long':
        return function (o) { return o % 1 === 0; };
      case 'float':
        return function (o) { return typeof o == 'number' && o < 3.42e38; };
      case 'double':
        return function (o) { return typeof o == 'number'; };
      case 'bytes':
        return Buffer.isBuffer;
      case 'string':
        return function (o) { return typeof o == 'string'; };
    }

  }

  function capitalize(name) {

    return name.charAt(0).toUpperCase() + name.slice(1);

  }

}
util.inherits(PrimitiveType, Type);

function UnionType(schema, opts) {

  if (!(schema instanceof Array)) {
    throw new ParseError('non-array union schema');
  }

  opts = getOpts(schema, opts);

  // TODO: Check that the alternatives satisfy Avro requirements.

  this.types = schema.map(function (o) {
    return parse(o, opts);
  });

  var self = this;
  var readers = this.types.map(function (obj) { return obj._read; });
  var longReader = Tap.prototype.readLong;
  var longWriter = Tap.prototype.writeLong;

  if (opts.unwrapUnions) {
    throw new Error('not implemented yet'); // TODO. (Actually the other.)
  }

  this._read = function () {
    return readers[longReader.call(this)].call(this);
  };

  this._write = function (obj) {
    var i, l, type;
    for (i = 0, l = self.types.length; i < l; i++) {
      type = self.types[i];
      if (type._check(obj)) {
        longWriter.call(this, i);
        type._write.call(this, obj);
        return;
      }
    }
    // (Fails silently.)
  };

  this._check = function (obj) {
    return this.types.some(function (type) { return type._check(obj); });
  };

}
util.inherits(UnionType, Type);

function EnumType(schema, opts) {

  if (!schema.name) {
    throw new ParseError('missing name');
  }
  if (!(schema.symbols instanceof Array)) {
    throw new ParseError('invalid symbols');
  }

  opts = getOpts(schema, opts);

  var self = this;
  var reader = Tap.prototype.readLong;
  var writer = Tap.prototype.writeLong;

  var indices = {}; // Compute reverse index.
  var i, l;
  for (i = 0, l = schema.symbols.length; i < l; i++) {
    indices[schema.symbols[i]] = i;
  }

  this.name = getQualifiedName(schema, opts.namespace);
  opts.registry[this.name] = this;
  this.doc = schema.doc;
  this.symbols = schema.symbols;

  this._read = function () { return self.symbols[reader.call(this)]; };

  this._write = function (s) {
    var index = indices[s];
    if (index === undefined) {
      return;
    }
    writer.call(this, index);
  };

  this._check = function (s) { return indices[s] !== undefined; };

}
util.inherits(EnumType, Type);

function FixedType(schema, opts) {

  if (!schema.name) {
    throw new ParseError('missing name');
  }
  if (schema.size !== (schema.size | 0)) {
    throw new ParseError('invalid size');
  }

  opts = getOpts(schema, opts);

  var self = this;
  var reader = Tap.prototype.readFixed;
  var writer = Tap.prototype.writeFixed;

  this.name = getQualifiedName(schema, opts);
  opts.registry[this.name] = this;
  this.size = schema.size;
  this._read = function () { return reader.call(this, self.size); };
  this._write = function (buf) { writer.call(this, buf, self.size); };
  this._check = function (buf) {
    return Buffer.isBuffer(buf) && buf.length == self.size;
  };

}
util.inherits(FixedType, Type);

function MapType(schema, opts) {

  if (!schema.values) {
    throw new ParseError('missing values');
  }

  opts = getOpts(schema, opts);

  var self = this;
  var reader = Tap.prototype.readMap;
  var writer = Tap.prototype.writeMap;

  this.valuesType = parse(schema.values, opts);

  this._read = function () {
    return reader.call(this, self.valuesType._read);
  };

  this._write = function (obj) {
    writer.call(this, obj, self.valuesType._write);
  };

  this._check = function (obj) {
    if (typeof obj != 'object') {
      return false;
    }
    var keys = Object.keys(obj);
    var i, l;
    for (i = 0, l = keys.length; i < l; i++) {
      if (!self.valuesType._check(obj[keys[i]])) {
        return false;
      }
    }
    return true;
  };

}
util.inherits(MapType, Type);

function ArrayType(schema, opts) {

  if (!schema.items) {
    throw new ParseError('missing array items');
  }

  opts = getOpts(schema, opts);

  var self = this;
  var reader = Tap.prototype.readArray;
  var writer = Tap.prototype.writeArray;

  this.itemsType = parse(schema.items, opts);

  this._read = function () {
    return reader.call(this, self.itemsType._read);
  };

  this._write = function (arr) {
    writer.call(this, arr, self.itemsType._write);
  };

  this._check = function (obj) {
    if (!(obj instanceof Array)) {
      return false;
    }
    return obj.every(function (elem) { return self.itemsType._check(elem); });
  };

}
util.inherits(ArrayType, Type);

function RecordType(schema, opts) {

  // jshint -W054

  if (!schema.name) {
    throw new ParseError('missing name');
  }
  if (!(schema.fields instanceof Array)) {
    throw new ParseError('invalid fields');
  }

  opts = getOpts(schema, opts);

  var self = this;

  this.name = getQualifiedName(schema, opts.namespace);
  this.doc = schema.doc;
  opts.registry[this.name] = this;

  this.fields = schema.fields.map(function (field) {
    var type = parse(field.type, opts);
    var value = field['default'];
    if (value !== undefined) {
      // Avro forces the default value to be of the first type in the
      // enum, so we must do a bit of extra logic here.
      if (
        type instanceof UnionType ?
          !type.types[0].validate(value) :
          !type.validate(value)
      ) {
        throw new ParseError('invalid default for field: ' + field.name);
      }
    }
    return {
      name: field.name,
      doc: field.doc,
      type: type,
      'default': value
    };
  });

  this._create = generateConstructor();
  this._create.decode = this.decode.bind(this);
  this._create.prototype = {
    $type: this,
    $encode: function (opts) { return self.encode(this, opts); },
    $validate: function () { return self.validate(this); }
  };
  // The names of these properties added to the prototype are prefixed with `$`
  // because it is an invalid property name in Avro but not in JavaScript.
  // (This way we are guaranteed not to be stepped over!)

  this._read = generateReader();
  this._write = generateWriter();
  this._check = function (obj) { // TODO: Code generate this.
    if (typeof obj != 'object') {
      return false;
    }
    return this.fields.every(function (field) {
      var value = obj[field.name];
      if (value === undefined) {
        value = field['default'];
      }
      return field.type._check(value);
    });
  };

  function generateConstructor() {

    var args = [];
    var statements = [];
    var i, l, name;
    for (i = 0, l = self.fields.length; i < l; i++) {
      name = self.fields[i].name;
      args.push(name);
      statements.push('this.' + name + ' = ' + name + ';');
    }
    var parts = self.name.split('.'); // To get the unqualified name.
    var body = 'return function ' + parts[parts.length - 1] + '(';
    body += args.join(',');
    body += ') {\n';
    body += statements.join('\n');
    body += '\n};';
    return new Function(body).call();

  }

  function generateReader() {

    var names = [];
    var values = [self._create];
    var i, l;
    for (i = 0, l = self.fields.length; i < l; i++) {
      names.push('r' + i);
      values.push(self.fields[i].type._read);
    }
    var body = 'return function() { return new Record(';
    body += names.map(function (r) { return r + '.call(this)'; }).join(',');
    body += '); };';
    names.unshift('Record');
    return new Function(names.join(','), body).apply(undefined, values);

  }

  function generateWriter() {

    var names = [];
    var values = [];
    var body = 'return function(obj) {\n';
    var i, l, field;
    for (i = 0, l = self.fields.length; i < l; i++) {
      field = self.fields[i];
      names.push('w' + i);
      values.push(field.type._write);
      if (field['default'] !== undefined) {
        body += 'var f' + i + ' = obj.' + field.name + '; ';
        body += 'if (f' + i + ' === undefined) { ';
        body += 'f' + i + ' = ' + JSON.stringify(field['default']) + ';';
        body += ' } w' + i + '.call(this, f' + i + ');\n';
      } else {
        body += 'w' + i + '.call(this, obj.' + field.name + ');\n';
      }
    }
    body += '}';
    return new Function(names.join(','), body).apply(undefined, values);

  }

}
util.inherits(RecordType, Type);

/**
 * Get class record instances are made of.
 *
 * It can be instantiated and used directly!
 *
 */
RecordType.prototype.getRecordConstructor = function () {

  return this._create;

};

// Helpers.

/**
 * Return a schema's qualified name.
 *
 * @param schema {Object} True schema (can't be a string).
 * @param namespace {String} Optional namespace.
 *
 */
function getQualifiedName(schema, namespace) {

  var name = schema.name;
  namespace = schema.namespace || namespace;
  if (name && !~name.indexOf('.') && namespace) {
    name = namespace + '.' + name;
  }
  return name;

}

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

module.exports = {
  ParseError: ParseError,
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
  }
};
