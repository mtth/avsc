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
 * Base Avro type class.
 *
 * Not meant to be instantiated directly, but via the `fromSchema` static
 * method (which will return the correct subclass).
 *
 */
function AvroType() {}

/**
 * Parse a schema.
 *
 * @param schema {Object|String} Schema.
 *
 * This is somewhat memory intensive, optimizing for decoding and encoding
 * speed. This should only be a problem is decoding many complex schemas.
 *
 */
AvroType.fromSchema = function (schema) {

  var registry = {};
  var i, l, type;
  for (i = 0, l = PRIMITIVES.length; i < l; i++) {
    type = PRIMITIVES[i];
    registry[type.name] = type;
  }
  return parse(schema);

  // Resolve all types, recursively.
  function parse(schema, ns) {

    if (typeof schema == 'string') {
      return parse({type: schema}, ns);
    }

    if (schema instanceof Array) {
      // Union.
      // TODO: Check that the alternatives satisfy Avro requirements.
      return new AvroUnion(schema.map(function (o) { return parse(o, ns); }));
    }

    ns = schema.namespace || ns;

    var type = schema.type;
    if (registry[type] instanceof AvroPrimitive) {
      // Primitive.
      return registry[type];
    }

    // Name; for enum, fixed, and record.
    var name = schema.name;
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
        if (!schema.size) {
          throw new ParseError('missing fixed size');
        }
        return registry[name] = new AvroFixed(name, schema.size);
      case 'map':
        if (!schema.values) {
          throw new ParseError('missing map values');
        }
        return new AvroMap(parse(schema.values, ns));
      case 'array':
        if (!schema.items) {
          throw new ParseError('missing array items');
        }
        return new AvroArray(parse(schema.items, ns));
      case 'record':
        // TODO: Aliases support.
        if (!name) {
          throw new ParseError('missing record name');
        }
        if (!(schema.fields instanceof Array)) {
          throw new ParseError('record fields must be an array');
        }
        record = registry[name] = new AvroRecord(name, schema.doc);
        // Insert early so that fields can find it.
        record._initialize(schema.fields.map(function (field) {
          var type = parse(field.type, ns);
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
          return {name: field.name, type: type, 'default': value};
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

// TODO: Move methods to the prototype.

// Primitives, these are only instantiated once.

function AvroPrimitive(name, check) {

  this.name = name.toLowerCase();
  this._check = check;
  this._read = Tap.prototype['read' + name];
  this._write = Tap.prototype['write' + name];

}
util.inherits(AvroPrimitive, AvroType);

var PRIMITIVES = (function () {

  // jshint -W018

  var primitives = [
    {
      name: 'null',
      check: function (obj) { return obj === null || obj === undefined; }
    },
    {
      name: 'boolean',
      check: function (obj) { return obj === !!obj; }
    },
    {
      name: 'int',
      check: function (obj) { return obj === (obj | 0); }
    },
    {
      name: 'long',
      check: function (obj) { return obj % 1 === 0; }
    },
    {
      name: 'float',
      check: function (obj) { return typeof obj == 'number' && obj < 3.42e38; }
    },
    {
      name: 'double',
      check: function (obj) { return typeof obj == 'number'; }
    },
    {
      name: 'bytes',
      check: Buffer.isBuffer
    },
    {
      name: 'string',
      check: function (obj) { return typeof obj == 'string'; }
    }
  ];

  return primitives.map(function (obj) {
    var name = obj.name.charAt(0).toUpperCase() + obj.name.slice(1);
    return new AvroPrimitive(name, obj.check);
  });

})();

// Unions.

function AvroUnion(types) {

  var longReader = Tap.prototype.readLong;
  var longWriter = Tap.prototype.writeLong;
  var readers = types.map(function (obj) { return obj._read; });

  this.types = types;
  this._read = function () {
    return readers[longReader.call(this)].call(this);
  };
  this._write = function (obj) {
    // TODO: Optimize this (perhaps by redesigning the `_check` function.
    var i, l, type;
    for (i = 0, l = types.length; i < l; i++) {
      type = types[i];
      if (type._check(obj)) {
        longWriter.call(this, i);
        type._write.call(this, obj);
        return;
      }
    }
    // (Fails silently.)
  };
  this._check = function (obj) {
    return types.some(function (type) { return type._check(obj); });
  };

}
util.inherits(AvroUnion, AvroType);

// Fixed.

function AvroFixed(name, size) {

  var reader = Tap.prototype.readFixed;
  var writer = Tap.prototype.writeFixed;

  this.name = name;
  this.size = size;
  this._read = function () { return reader.call(this, size); };
  this._write = function (buf) { writer.call(this, buf, size); };
  this._check = function (buf) {
    return Buffer.isBuffer(buf) && buf.length == size;
  };

}
util.inherits(AvroFixed, AvroType);

// Maps.

function AvroMap(type) {

  var reader = Tap.prototype.readMap;
  var writer = Tap.prototype.writeMap;

  this.valueType = type;
  this._read = function () { return reader.call(this, type._read); };
  this._write = function (obj) { writer.call(this, obj, type._write); };
  this._check = function (obj) {
    if (typeof obj != 'object') {
      return false;
    }
    var keys = Object.keys(obj);
    var i, l;
    for (i = 0, l = keys.length; i < l; i++) {
      if (!type._check(obj[keys[i]])) {
        return false;
      }
    }
    return true;
  };

}
util.inherits(AvroMap, AvroType);

// Arrays.

function AvroArray(type) {

  var reader = Tap.prototype.readArray;
  var writer = Tap.prototype.writeArray;

  this.itemType = type;
  this._read = function () { return reader.call(this, type._read); };
  this._write = function (arr) { writer.call(this, arr, type._write); };
  this._check = function (obj) {
    if (!(obj instanceof Array)) {
      return false;
    }
    return obj.every(function (elem) { return type._check(elem); });
  };

}
util.inherits(AvroArray, AvroType);

// Records, the fun part!

function AvroRecord(name, doc) {

  this.name = name;
  this.doc = doc;
  this.fields = null;

  // These get populated in the following `_initialize` call.
  this._constructor = null;
  this._read = null;
  this._write = null;
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
 * @param fields {Array} List of field contained in the record. The types
 * inside each of these fiels must already have been parsed.
 *
 */
AvroRecord.prototype._initialize = function (fields) {

  // jshint -W054

  var self = this;
  var names = fields.map(function (field) { return field.name; });
  var body = names.map(function (s) { return 'this.' + s + ' = ' + s + ';'; });
  this.fields = fields;
  this._constructor = new Function(names.join(','), body.join('\n'));
  this._read = generateReader();
  this._write = generateWriter();

  // We add a few convenience methods to the constructor.
  // The names of properties added to the prototype are prefixed with `$`
  // because it is an invalid property name in Avro but not in JavaScript.
  // (This way we are guaranteed not to be stepped over!)
  this._constructor.decode = this.decode.bind(this);
  this._constructor.prototype = {
    $typeName: this.name,
    $fieldNames: names,
    $encode: function (opts) { return self.encode(this, opts); },
    $validate: function () { return self.validate(this); }
  };

  function generateReader() {

    var names = [];
    var values = [self._constructor];
    var i, l;
    for (i = 0, l = fields.length; i < l; i++) {
      names.push('r' + i);
      values.push(fields[i].type._read);
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
    for (i = 0, l = fields.length; i < l; i++) {
      field = fields[i];
      names.push('w' + i);
      values.push(field.type._write);
      if (field['default'] !== undefined) {
        body += 'var f' + i + ' = obj.' + field.name + ';\n';
        body += 'if (f' + i + ' === undefined) {\n';
        body += 'f' + i + ' = ' + JSON.stringify(field['default']) + ';\n';
        body += '}\nw' + i + '.call(this, f' + i + ');\n';
      } else {
        body += 'w' + i + '.call(this, obj.' + field.name + ');\n';
      }
    }
    body += '}';
    return new Function(names.join(','), body).apply(undefined, values);

  }

};

module.exports = {
  parse: AvroType.fromSchema,
  ParseError: ParseError
};
