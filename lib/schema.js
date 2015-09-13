/* jshint node: true */

'use strict';

var tap = require('./tap');

// Generate all primitive types along with their (de)serializers.
var PRIMITIVE_TYPES = (function () {

  var p = tap.Tap.prototype;
  return [
    {name: 'null', _reader: p.readNull, _writer: p.writeNull},
    {name: 'boolean', _reader: p.readBoolean, _writer: p.writeBoolean},
    {name: 'int', _reader: p.readInt, _writer: p.writeInt},
    {name: 'long', _reader: p.readLong, _writer: p.writeLong},
    {name: 'float', _reader: p.readFloat, _writer: p.writeFloat},
    {name: 'double', _reader: p.readDouble, _writer: p.writeDouble},
    {name: 'bytes', _reader: p.readBytes, _writer: p.writeBytes},
    {name: 'string', _reader: p.readString, _writer: p.writeString}
  ];

})();

/**
 * Avro schema.
 *
 */
function Schema(obj) {

  // Load primitive types.
  this._types = {};
  var i, l, type;
  for (i = 0, l = PRIMITIVE_TYPES.length; i < l; i++) {
    type = PRIMITIVE_TYPES[i];
    this._types[type.name] = type;
  }

  // Parse schema, fully resolving all type names.
  this._value = resolve(this.types, obj);

  function resolve(types, obj, namespace) {

    if (typeof obj == 'string') {
      return resolve(types, {type: obj}, namespace);
    }

    if (obj instanceof Array) {
      // Enum or record fields.
      return obj.map(function (elem) {
        return resolve(types, elem, namespace);
      });
    }

    var ns = obj.namespace || namespace;
    var type = obj.type;
    var expanded;
    switch (type) {
      case 'map':
        expanded = resolve(types, obj.values, ns);
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
        expanded = resolve(types, obj.items, ns);
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
        types[name] = expanded; // Insert early so that fields can find it.
        var fields = obj.fields.map(function (field) {
          return {
            name: field.name,
            type: resolve(types, field, ns),
            'default': field['default']
          };
        });
        var len = fields.length;
        expanded.fields = fields;
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
        if (!(type in PRIMITIVE_TYPES) && !~type.indexOf('.') && ns) {
          type = ns + '.' + type;
        }
        expanded = types[type];
        if (!expanded) {
          throw new Error('missing type: ' + type);
        }
        return expanded;
    }

  }

}

Schema.prototype.encode = function (record) {

  var buf = new Buffer(1000); // TODO: Learn this size.
  var fl = new tap.Tap(buf);
  this._value._writer.call(fl, record);
  return buf.slice(0, fl.offset);

};

Schema.prototype.decode = function (buf) {

  return this._value._reader.call(new tap.Tap(buf));

};

module.exports = {
  Schema: Schema
};
