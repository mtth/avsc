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
  this._value = this._resolve(obj);

}

/**
 * Convenience method to check whether a type is primitive or not.
 *
 * @param name {String} Type name.
 *
 */
Schema.prototype._isPrimitive = function (name) {

  return this._types.hasOwnProperty(name) && this._types[name]._primitive;

};

/**
 * Parse a schema, resolving all types.
 *
 * @param obj {Object} Schema, can be partial.
 * @param namespace {String} Optional namespace.
 *
 */
Schema.prototype._resolve = function (obj, namespace) {

  var self = this;

  if (typeof obj == 'string') {
    return self._resolve({type: obj}, namespace);
  }

  if (obj instanceof Array) {
    // Enum or record fields.
    return obj.map(function (elem) {
      return self._resolve(elem, namespace);
    });
  }

  var ns = obj.namespace || namespace;
  var type = obj.type;
  var expanded;
  switch (type) {
    case 'map':
      expanded = self._resolve(obj.values, ns);
      return {
        type: 'map',
        values: expanded,
        _reader: function () {
          return tap.Tap.prototype.readMap.call(this, expanded._reader);
        },
        _writer: function (obj) {
          tap.Tap.prototype.writeMap.call(this, obj, expanded._writer);
        }
      };
    case 'array':
      expanded = self._resolve(obj.items, ns);
      return {
        type: 'array',
        items: expanded,
        _reader: function () {
          return tap.Tap.prototype.readArray.call(this, expanded._reader);
        },
        _writer: function (arr) {
          tap.Tap.prototype.writeArray.call(this, arr, expanded._writer);
        }
      };
    case 'record':
      var name = obj.name;
      if (!~name.indexOf('.') && ns) {
        name = ns + '.' + name;
      }
      expanded = {type: 'record', name: name};
      self._types[name] = expanded; // Insert early so that fields can find it.
      var fields = obj.fields.map(function (field) {
        return {
          name: field.name,
          type: self._resolve(field, ns),
          'default': field['default']
        };
      });
      var len = fields.length;
      expanded.fields = fields;
      // TODO: Generate custom code for each record reader and writer.
      expanded._reader = function () {
        var obj = {};
        var i, field;
        for (i = 0; i < len; i++) {
          field = fields[i];
          obj[field.name] = field.type._reader.call(this);
        }
        return obj;
      };
      expanded._writer = function (obj) {
        var i, field;
        for (i = 0; i < len; i++) {
          field = fields[i];
          field.type._writer.call(this, obj[field.name]);
        }
      };
      return expanded;
    default:
      if (ns && !~type.indexOf('.') && !self._isPrimitive(type)) {
        type = ns + '.' + type;
      }
      expanded = self._types[type];
      if (!expanded) {
        throw new Error('missing type: ' + type);
      }
      return expanded;
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
