/* jshint node: true */

'use strict';

var Tap = require('./tap'),
    util = require('util');


/**
 * Custom error, thrown when an invalid schema is encountered.
 *
 * (Error currently has no constructor, so we don't call it.)
 *
 */
function ParseError(message) { this.message = message; }
util.inherits(ParseError, Error);


/**
 * Parse a schema.
 *
 * @param schema {Object|String} Schema.
 *
 * This is somewhat memory intensive, optimizing for decoding and encoding
 * speed. This should only be a problem is decoding many complex schemas.
 *
 */
function parse(schema, ns, reg) {

  reg = reg || {};

  if (typeof schema == 'string') {
    return parse({type: schema}, ns, reg);
  }

  if (schema instanceof Array) {
    return new AvroUnion(schema);
  }

  var typeName = schema.type;
  if (typeof typeName != 'string') {
    throw new ParseError('invalid type: ' + typeName);
  }

  // Primitive types.
  if (typeName in PRIMITIVE_CHECKERS) {
    return new AvroPrimitive(schema);
  }

  // Complex types.
  var Type;
  switch (typeName) {
    case 'array':
      Type = AvroArray;
      break;
    case 'fixed':
      Type = AvroFixed;
      break;
    case 'map':
      Type = AvroMap;
      break;
    case 'record':
      Type = AvroRecord;
      break;
    default:
      Type = undefined;
  }
  if (Type) {
    return new Type(schema, ns, reg);
  }

  // Type reference.
  ns = schema.namespace || ns;
  if (ns && !~typeName.indexOf('.')) {
    typeName = ns + '.' + typeName;
  }
  Type = reg[typeName];
  if (Type) {
    return Type;
  }

  throw new ParseError('missing type: ' + typeName);

}


/**
 * Base Avro type class.
 *
 * Not meant to be instantiated directly, but via the `fromSchema` static
 * method (which will return the correct subclass).
 *
 */
function AvroType() {}

/**
 * Base decoding method.
 *
 * This method should always be called with a tap as context. For example:
 * `type._read.call(tap)`. It is also important to remember to check that the
 * tap is valid after a read.
 *
 */
AvroType.prototype._read = function () { throw new Error('abstract'); };

/**
 * Base encoding method.
 *
 * @param obj {Object} The object to encode.
 *
 * Similarly to `_read` above, this method should be called with a tap as
 * context: `type._write.call(tap, obj)`. The tap should be checked for
 * validity afterwards as well.
 *
 */
AvroType.prototype._write = function (obj) {

  // jshint unused: false

  throw new Error('abstract');

};

/**
 * Validity check.
 *
 * @param obj {Object} The object to check for validity.
 *
 */
AvroType.prototype._check = function (obj) {

  // jshint unused: false

  throw new Error('abstract');

};

/**
 * Decode Avro bytes.
 *
 * @param buf {Buffer} Avro representation of an object.
 *
 */
AvroType.prototype.decode = function (buf) {

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
AvroType.prototype.encode = function (obj, opts) {

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
AvroType.prototype.validate = function (obj) { return this._check(obj); };

// Implementations.

// TODO: Move methods to prototypes.

function AvroPrimitive(schema) {

  if (!(schema.type in PRIMITIVE_CHECKERS)) {
    throw new ParseError('invalid primitive type: ' + schema.type);
  }

  this.name = schema.type;
  this._check = PRIMITIVE_CHECKERS[this.name];
  this._read = Tap.prototype['read' + capitalize(this.name)];
  this._write = Tap.prototype['write' + capitalize(this.name)];

  // self[attr] = function() { return self[attr] = fn.call(this); };

}
util.inherits(AvroPrimitive, AvroType);

// Unions.

function AvroUnion(schema, ns, reg) {

  if (!(schema instanceof Array)) {
    throw new ParseError('non-array union schema');
  }

  ns = schema.namespace || ns;
  reg = reg || {};

  // TODO: Check that the alternatives satisfy Avro requirements.

  this.types = schema.map(function (o) { return parse(o, ns, reg); });
  var readers = this.types.map(function (obj) { return obj._read; });
  var longReader = Tap.prototype.readLong;
  var longWriter = Tap.prototype.writeLong;

  this._read = function () {
    return readers[longReader.call(this)].call(this);
  };

  this._write = function (obj) {
    // TODO: Optimize this (perhaps by redesigning the `_check` function.
    var i, l, type;
    for (i = 0, l = this.types.length; i < l; i++) {
      type = this.types[i];
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
util.inherits(AvroUnion, AvroType);

// Fixed.

function AvroFixed(schema, ns, reg) {

  if (!schema.name) {
    throw new ParseError('missing name');
  }
  if (schema.size !== (schema.size | 0)) {
    throw new ParseError('invalid size');
  }

  var self = this;
  var reader = Tap.prototype.readFixed;
  var writer = Tap.prototype.writeFixed;

  this.name = resolveName(schema, ns);
  reg[this.name] = this;
  this.size = schema.size;
  this._read = function () { return reader.call(this, self.size); };
  this._write = function (buf) { writer.call(this, buf, self.size); };
  this._check = function (buf) {
    return Buffer.isBuffer(buf) && buf.length == self.size;
  };

}
util.inherits(AvroFixed, AvroType);

// Maps.

function AvroMap(schema, ns, reg) {

  if (!schema.values) {
    throw new ParseError('missing values');
  }

  ns = schema.namespace || ns;
  reg = reg || {};

  var self = this;
  var reader = Tap.prototype.readMap;
  var writer = Tap.prototype.writeMap;

  this.valuesType = parse(schema.values, ns, reg);
  this._read = function () { return reader.call(this, self.valuesType._read); };
  this._write = function (obj) { writer.call(this, obj, self.valuesType._write); };
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
util.inherits(AvroMap, AvroType);

// Arrays.

function AvroArray(schema, ns, reg) {

  if (!schema.items) {
    throw new ParseError('missing array items');
  }

  ns = schema.namespace || ns;
  reg = reg || {};

  var self = this;
  var reader = Tap.prototype.readArray;
  var writer = Tap.prototype.writeArray;

  this.itemsType = parse(schema.items, ns, reg);
  this._read = function () { return reader.call(this, self.itemsType._read); };
  this._write = function (arr) { writer.call(this, arr, self.itemsType._write); };
  this._check = function (obj) {
    if (!(obj instanceof Array)) {
      return false;
    }
    return obj.every(function (elem) { return self.itemsType._check(elem); });
  };

}
util.inherits(AvroArray, AvroType);

// Records, the fun part!

function AvroRecord(schema, ns, reg) {

  // jshint -W054

  if (!schema.name) {
    throw new ParseError('missing name');
  }
  if (!(schema.fields instanceof Array)) {
    throw new ParseError('invalid fields');
  }

  ns = schema.namespace || ns;
  reg = reg || {};

  var self = this;

  this.name = resolveName(schema, ns);
  this.doc = schema.doc;
  reg[this.name] = this;
  this.fields = schema.fields.map(function (field) {
    var type = parse(field.type, ns, reg);
    var value = field['default'];
    if (value !== undefined) {
      // Avro forces the default value to be of the first type in the
      // enum, so we must do a bit of extra logic here.
      if (
        type instanceof AvroUnion ?
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

  this._constructor = generateConstructor();
  this._constructor.decode = this.decode.bind(this);
  this._constructor.prototype = {
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
    var values = [self._constructor];
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
util.inherits(AvroRecord, AvroType);

/**
 * Get class record instances are made of.
 *
 * It can be instantiated and used directly!
 *
 */
AvroRecord.prototype.getRecordConstructor = function () {

  return this._constructor;

};

// Helpers.

// Primitive checks.
var PRIMITIVE_CHECKERS = {
  'null': function (obj) { return obj === null; },
  'boolean': function (obj) { return obj === !!obj; }, // jshint ignore: line
  'int': function (obj) { return obj === (obj | 0); },
  'long': function (obj) { return obj % 1 === 0; },
  'float': function (obj) { return typeof obj == 'number' && obj < 3.42e38; },
  'double': function (obj) { return typeof obj == 'number'; },
  'bytes': Buffer.isBuffer,
  'string': function (obj) { return typeof obj == 'string'; }
};

function resolveName(schema, ns) {

  var name = schema.name;
  ns = schema.namespace || ns;
  if (name && !~name.indexOf('.') && ns) {
    name = ns + '.' + name;
  }
  return name;

}

function capitalize(name) {

  return name.charAt(0).toUpperCase() + name.slice(1);

}

module.exports = {
  parse: parse,
  ParseError: ParseError,
  types: {} // TODO
};
