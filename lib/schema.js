/* jshint node: true */

'use strict';

var tap = require('./tap'),
    fs = require('fs');

// Generate all primitive types along with their (de)serializers.
var PRIMITIVE_TYPES = (function () {

  // Capitalized for convenience below.
  var primitives = [
    'Null', 'Boolean', 'Int', 'Long', 'Float', 'Double', 'Bytes', 'String'
  ];

  return primitives.map(function (name) {
    return {
      type: name.toLowerCase(),
      _reader: tap.Tap.prototype['read' + name],
      _writer: tap.Tap.prototype['write' + name],
      _primitive: true
    };
  });

})();

/**
 * Avro schema.
 *
 */
function Schema(obj) {

  // Copy over primitive types. This map will also contain any defined records.
  this._types = {};
  var i, l, type;
  for (i = 0, l = PRIMITIVE_TYPES.length; i < l; i++) {
    type = PRIMITIVE_TYPES[i];
    this._types[type.type] = type;
  }

  // Parse schema, fully resolving all type names.
  this._value = this._parse(obj);

}

/**
 * Parse a schema, resolving all types.
 *
 * @param obj {Object} Schema, can be partial.
 * @param namespace {String} Optional namespace.
 *
 * This is somewhat memory intensive, optimizing for decoding and encoding
 * speed. This should only be a problem is decoding many complex schemas.
 *
 */
Schema.prototype._parse = function (obj, namespace) {

  var self = this;

  if (typeof obj == 'string') {
    return self._parse({type: obj}, namespace);
  }

  var parsed, reader, writer;
  if (obj instanceof Array) {
    // Union.
    parsed = obj.map(function (obj) { return self._parse(obj, namespace); });
    reader = tap.Tap.prototype.readUnion;
    writer = tap.Tap.prototype.writeUnion;
    return {
      type: 'union',
      _reader: function () { return reader.call(this, parsed); },
      _writer: function (obj) { return writer.call(this, obj, parsed); }
    };
  }

  var type = obj.type;
  if (self._types.hasOwnProperty(type) && self._types[type]._primitive) {
    // Primitive type.
    return self._types[type];
  }

  var ns = obj.namespace || namespace;
  var len;
  switch (type) {
    case 'fixed':
      // TODO: Generate code with length set.
      reader = tap.Tap.prototype.readFixed;
      writer = tap.Tap.prototype.writeFixed;
      len = obj.size;
      return {
        type: 'fixed',
        _reader: function () { return reader.call(this, len); },
        _writer: function (buf) { writer.call(this, buf, len); }
      };
    case 'map':
      parsed = self._parse(obj.values, ns);
      reader = tap.Tap.prototype.readMap;
      writer = tap.Tap.prototype.writeMap;
      return {
        type: 'map',
        values: parsed,
        _reader: function () { return reader.call(this, parsed._reader); },
        _writer: function (obj) { writer.call(this, obj, parsed._writer); }
      };
    case 'array':
      parsed = self._parse(obj.items, ns);
      reader = tap.Tap.prototype.readArray;
      writer = tap.Tap.prototype.writeArray;
      return {
        type: 'array',
        items: parsed,
        _reader: function () { return reader.call(this, parsed._reader); },
        _writer: function (arr) { writer.call(this, arr, parsed._writer); }
      };
    case 'record':
      var name = obj.name;
      if (!~name.indexOf('.') && ns) {
        name = ns + '.' + name;
      }
      parsed = {type: 'record', name: name};
      self._types[name] = parsed; // Insert early so that fields can find it.
      var fields = obj.fields.map(function (field) {
        return {
          name: field.name,
          type: self._parse(field.type, ns),
          'default': field['default']
        };
      });
      len = fields.length;
      parsed.fields = fields;
      // TODO: Generate custom code for each record reader and writer.
      // TODO: Add support for default.
      parsed._reader = function () {
        var obj = {};
        var i, field;
        for (i = 0; i < len; i++) {
          field = fields[i];
          obj[field.name] = field.type._reader.call(this);
        }
        return obj;
      };
      parsed._writer = function (obj) {
        var i, field;
        for (i = 0; i < len; i++) {
          field = fields[i];
          field.type._writer.call(this, obj[field.name]);
        }
      };
      return parsed;
    default:
      // Type reference.
      if (ns && !~type.indexOf('.')) {
        type = ns + '.' + type;
      }
      parsed = self._types[type];
      if (!parsed) {
        throw new Error('missing type: ' + type);
      }
      return parsed;
  }

};

/**
 * Convenience method to encode a record.
 *
 */
Schema.prototype.encode = function (record) {

  var buf = new Buffer(1000); // TODO: Learn this size.
  var fl = new tap.Tap(buf);
  this._value._writer.call(fl, record);
  return buf.slice(0, fl.offset);

};

/**
 * Decode a record from bytes.
 *
 */
Schema.prototype.decode = function (buf) {

  return this._value._reader.call(new tap.Tap(buf));

};

/**
 * Load a schema from a file.
 *
 */
Schema.fromFile = function (path) {

  return new Schema(JSON.parse(fs.readFileSync(path)));

};

module.exports = {
  Schema: Schema
};
