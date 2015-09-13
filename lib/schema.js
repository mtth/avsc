/* jshint node: true */

'use strict';

var flow = require('./flow');

var prototype = flow.Flow.prototype;

var PRIMITIVE_TYPES = {
  'null': [prototype.readNull, prototype.writeNull],
  'boolean': [prototype.readBoolean, prototype.writeBoolean],
  'int': [prototype.readInt, prototype.writeInt],
  'long': [prototype.readLong, prototype.writeLong],
  'float': [prototype.readFloat, prototype.writeFloat],
  'double': [prototype.readDouble, prototype.writeDouble],
  'bytes': [prototype.readBytes, prototype.writeBytes],
  'string': [prototype.readString, prototype.writeString]
};

function Schema(obj) {

  this.types = primeTypeNames();
  this.value = expand(this.types, obj);

  function primeTypeNames() {

    var types = {};
    var name, fns;
    for (name in PRIMITIVE_TYPES) {
      fns = PRIMITIVE_TYPES[name];
      types[name] = {type: name, _reader: fns[0], _writer: fns[1]};
    }
    return types;

  }

  function expand(types, obj, namespace) {

    if (typeof obj == 'string') {
      return expand(types, {type: obj}, namespace);
    }

    if (obj instanceof Array) {
      // Enum or record fields.
      return obj.map(function (elem) {
        return expand(types, elem, namespace);
      });
    }

    var ns = obj.namespace || namespace;
    var type = obj.type;
    var expanded;
    switch (type) {
      case 'map':
        expanded = expand(types, obj.values, ns);
        return {
          type: 'map',
          values: expanded,
          _reader: function () {
            return prototype.readMap.call(this, expanded._reader);
          },
          _writer: function (obj) {
            prototype.writeMap.call(this, obj, expanded._writer);
          }
        };
      case 'array':
        expanded = expand(types, obj.items, ns);
        return {
          type: 'array',
          items: expanded,
          _reader: function () {
            return prototype.readArray.call(this, expanded._reader);
          },
          _writer: function (arr) {
            prototype.writeArray.call(this, arr, expanded._writer);
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
            type: expand(types, field, ns),
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
  var fl = new flow.Flow(buf);
  this.value._writer.call(fl, record);
  return buf.slice(0, fl.offset);

};

Schema.prototype.decode = function (buf) {

  return this.value._reader.call(new flow.Flow(buf));

};

module.exports = {
  Schema: Schema
};
