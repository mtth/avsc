/* jshint node: true */

'use strict';

var Tap = require('./tap'),
    util = require('util');


/**
 * Custom error, thrown when an invalid schema is encountered.
 *
 */
function ParseError(message) { Error.call(this, message); }
util.inherits(ParseError, Error);


/**
 * Base Avro type class.
 *
 */
function AvroType() {}

/**
 * Parse a schema.
 *
 * @param obj {Object} Schema, can be partial.
 *
 * This is somewhat memory intensive, optimizing for decoding and encoding
 * speed. This should only be a problem is decoding many complex schemas.
 *
 */
AvroType.fromSchema = function (obj) {

  // TODO: Friendly error messages.

  var registry = {};
  var i, l, type;
  for (i = 0, l = PRIMITIVES.length; i < l; i++) {
    type = PRIMITIVES[i];
    registry[type.name] = type;
  }
  return parse(obj);

  // Resolve all types, recursively.
  function parse(obj, ns) {

    if (typeof obj == 'string') {
      return parse({type: obj}, ns);
    }

    if (obj instanceof Array) {
      // Union.
      return new AvroUnion(obj.map(function (o) { return parse(o, ns); }));
    }

    ns = obj.namespace || ns;

    var type = obj.type;
    if (registry[type] instanceof AvroPrimitive) {
      // Primitive.
      return registry[type];
    }

    // Name; for enum, fixed, and record.
    var name = obj.name;
    if (name && !~name.indexOf('.') && ns) {
      name = ns + '.' + name;
    }

    var record;
    switch (type) {
      case 'enum':
        throw new Error('not implemented'); // TODO: Add enum support.
      case 'fixed':
        if (!name) {
          throw new ParseError('missing fixed name');
        }
        if (!obj.size) {
          throw new ParseError('missing fixed size');
        }
        return registry[name] = new AvroFixed(name, obj.size);
      case 'map':
        if (!obj.values) {
          throw new ParseError('missing map values');
        }
        return new AvroMap(parse(obj.values, ns));
      case 'array':
        if (!obj.items) {
          throw new ParseError('missing array items');
        }
        return new AvroArray(parse(obj.items, ns));
      case 'record':
        // TODO: Aliases support.
        if (!name) {
          throw new ParseError('missing record name');
        }
        if (!(obj.fields instanceof Array)) {
          throw new ParseError('record fields must be an array');
        }
        record = registry[name] = new AvroRecord(name, obj.doc);
        // Insert early so that fields can find it.
        record._initialize(obj.fields.map(function (field) {
          return {
            name: field.name,
            type: parse(field.type, ns),
            'default': field['default']
          };
        }));
        return record;
      default:
        // Type reference.
        if (ns && !~type.indexOf('.')) {
          type = ns + '.' + type;
        }
        record = registry[type];
        if (!record) {
          throw new ParseError('missing type: ' + type);
        }
        return record;
    }

  }

};

AvroType.prototype._reader = AvroType.prototype._writer = function () {

  throw new Error('not implemented');

};

/**
 * Decode Avro bytes.
 *
 * @param buf {Buffer} Avro representation of an object.
 *
 */
AvroType.prototype.decode = function (buf) {

  var tap = new Tap(buf);
  var obj = this._reader.call(tap);
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
 * @param size {Number} Optional size, used to initialize the buffer. Defaults
 * to 64 bytes.
 *
 */
AvroType.prototype.encode = function (obj, size) {

  size = size || 64;
  var tap = new Tap(new Buffer(size));
  do {
    this._writer.call(tap, obj);
  } while (!isValid());

  return tap.buf.slice(0, tap.offset);

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

// Implementations.

// Primitives, these are only instantiated once.

function AvroPrimitive(name) {

  this.name = name.toLowerCase();
  this._reader = Tap.prototype['read' + name];
  this._writer = Tap.prototype['write' + name];

}
util.inherits(AvroPrimitive, AvroType);

var PRIMITIVES = (function () {
  var primitives = [
    'Null', 'Boolean', 'Int', 'Long', 'Float', 'Double', 'Bytes', 'String'
  ]; // Capitalized for convenience below.
  return primitives.map(function (name) { return new AvroPrimitive(name); });
})();

// Enum.

// TODO.

// Unions.

function AvroUnion(types) {

  var reader = Tap.prototype.readUnion;
  var writer = Tap.prototype.writeUnion;

  this.types = types;
  this._reader = function () { return reader.call(this, types); };
  this._writer = function (obj) { return writer.call(this, obj, types); };

}
util.inherits(AvroUnion, AvroType);

// Fixed.

function AvroFixed(name, size) {

  // TODO: Generate code with length set.

  var reader = Tap.prototype.readFixed;
  var writer = Tap.prototype.writeFixed;

  this.name = name;
  this.size = size;
  this._reader = function () { return reader.call(this, size); };
  this._writer = function (buf) { writer.call(this, buf, size); };

}
util.inherits(AvroFixed, AvroType);

// Maps.

function AvroMap(type) {

  var reader = Tap.prototype.readMap;
  var writer = Tap.prototype.writeMap;

  this.valueType = type;
  this._reader = function () { return reader.call(this, type._reader); };
  this._writer = function (obj) { writer.call(this, obj, type._writer); };

}
util.inherits(AvroMap, AvroType);

// Arrays.

function AvroArray(type) {

  var reader = Tap.prototype.readArray;
  var writer = Tap.prototype.writeArray;

  this.itemType = type;
  this._reader = function () { return reader.call(this, type._reader); };
  this._writer = function (arr) { writer.call(this, arr, type._writer); };

}
util.inherits(AvroArray, AvroType);

// Records, the fun part!

function AvroRecord(name, doc) {

  // TODO: Add support for default.

  this.name = name;
  this.doc = doc;
  this.fieldTypes = null;

  // These get populated in the following `_initialize` call.
  this._constructor = null;
  this._reader = null;
  this._writer = null;

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

/**
 * Private method, called when the schema is first parsed.
 *
 * @param types {Array} List of field types contained in the record. These
 * types must already have been parsed.
 *
 */
AvroRecord.prototype._initialize = function (types) {

  /*jshint -W054 */

  var self = this;
  var names = types.map(function (field) { return field.name; });
  var body = names.map(function (s) { return 'this.' + s + ' = ' + s + ';'; });
  this.fieldTypes = types;
  this._constructor = new Function(names.join(','), body.join('\n'));
  this._reader = generateReader();
  this._writer = generateWriter();

  // We add a few convenience methods to the constructor.
  // The names of properties added to the prototype are prefixed with `$`
  // because it is an invalid property name in Avro but not in JavaScript.
  // (This way we are guaranteed not to be stepped over!)
  this._constructor.fromAvro = self.decode.bind(self);
  this._constructor.prototype = {
    $name: self.name,
    $fields: names,
    $_size: 1000, // TODO: Learn this size.
    $toAvro: function () { return self.encode(this); }
  };

  function generateReader() {

    var names = [];
    var values = [self._constructor];
    var i, l;
    for (i = 0, l = types.length; i < l; i++) {
      names.push('r' + i);
      values.push(types[i].type._reader);
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
    for (i = 0, l = types.length; i < l; i++) {
      field = types[i];
      names.push('w' + i);
      values.push(field.type._writer);
      body += 'w' + i + '.call(this, obj.' + field.name + ');\n';
    }
    body += '}';
    return new Function(names.join(','), body).apply(undefined, values);

  }

};

module.exports = {
  parse: AvroType.fromSchema,
  ParseError: ParseError
};
