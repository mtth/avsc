/* jshint node: true */

'use strict';

var tap = require('./tap'),
    types = require('./types'),
    fs = require('fs');

/**
 * Avro schema.
 *
 * We choose `$` prefixed names for special keys because it is a valid
 * identifier for object properties in JavaScript, but not in Avro, so we are
 * safe.
 *
 */
function Schema(obj) {

  // Copy over primitive types. This map will also contain any defined records.
  this._types = {};
  var i, l, type;
  for (i = 0, l = types.PRIMITIVES.length; i < l; i++) {
    type = types.PRIMITIVES[i];
    this._types[type.name] = type;
  }

  // Parse schema, fully resolving all type names.
  this._value = this._parse(obj);

}

/**
 * Parse a schema, resolving all types.
 *
 * @param obj {Object} Schema, can be partial.
 * @param ns {String} Optional namespace.
 *
 * This is somewhat memory intensive, optimizing for decoding and encoding
 * speed. This should only be a problem is decoding many complex schemas.
 *
 */
Schema.prototype._parse = function (obj, ns) {

  var self = this;

  if (typeof obj == 'string') {
    return self._parse({type: obj}, ns);
  }

  if (obj instanceof Array) {
    // Union.
    return new types.$Union(obj.map(function (o) { return self._parse(o, ns); }));
  }

  ns = obj.namespace || ns;

  var type = obj.type;
  if (self._types[type] instanceof types.$Primitive) {
    // Primitive.
    return self._types[type];
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
      return self._types[name] = new types.$Fixed(name, obj.size);
    case 'map':
      return new types.$Map(self._parse(obj.values, ns));
    case 'array':
      return new types.$Array(self._parse(obj.items, ns));
    case 'record':
      record = self._types[name] = new types.$Record(name);
      // Insert early so that fields can find it.
      record.setFields(obj.fields.map(function (field) {
        return {
          name: field.name,
          type: self._parse(field.type, ns),
          'default': field['default']
        };
      }));
      return record;
    default:
      // Type reference.
      if (ns && !~type.indexOf('.')) {
        type = ns + '.' + type;
      }
      record = self._types[type];
      if (!record) {
        throw new Error('missing type: ' + type);
      }
      return record;
  }

};

Schema.prototype.decoder = null; // TODO: Transform stream (block -> record).

Schema.prototype.encoder = null; // TODO: Transform stream (record -> block).

/**
 * Decode a record from bytes.
 *
 */
Schema.prototype.decode = function (buf) {

  return this._value._reader.call(new tap.Tap(buf));

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
 * Load a schema from a file.
 *
 */
Schema.fromFile = function (path) {

  return new Schema(JSON.parse(fs.readFileSync(path)));

};

module.exports = {
  Schema: Schema
};
