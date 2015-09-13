/* jshint node: true */

'use strict';

var tap = require('./tap'),
    util = require('util');

/**
 * Base Avro type class.
 *
 */
function $Type() {

  this._reader = this._writer = function () {
    throw new Error('not implemented');
  };

}

/**
 * Parse schema.
 *
 */
$Type.fromSchema = function (obj) {

  // TODO: Friendly error messages.

  var registry = {};
  var i, l, type;
  for (i = 0, l = PRIMITIVES.length; i < l; i++) {
    type = PRIMITIVES[i];
    registry[type.name] = type;
  }
  return (function parse(obj, ns) {

    if (typeof obj == 'string') {
      return parse({type: obj}, ns);
    }

    if (obj instanceof Array) {
      // Union.
      return new $Union(obj.map(function (o) { return parse(o, ns); }));
    }

    ns = obj.namespace || ns;

    var type = obj.type;
    if (registry[type] instanceof $Primitive) {
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
        return registry[name] = new $Fixed(name, obj.size);
      case 'map':
        return new $Map(parse(obj.values, ns));
      case 'array':
        return new $Array(parse(obj.items, ns));
      case 'record':
        record = registry[name] = new $Record(name);
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
          throw new Error('missing type: ' + type);
        }
        return record;
    }

  })(obj);

};

/**
 * Not super efficient.
 *
 */
$Type.prototype.decode = function (buf) {

  return this._reader.call(new tap.Tap(buf));

};

/**
 * Even less efficient.
 *
 */
$Type.prototype.encode = function (obj) {

  var buf = new tap.Tap(new Buffer(obj.$size || 1000));
  this._writer.call(buf, obj);
  return buf.buf.slice(0, buf.offset);

};

// Implementations.

function $Primitive(name) {

  this.name = name.toLowerCase();
  this._reader = tap.Tap.prototype['read' + name];
  this._writer = tap.Tap.prototype['write' + name];

}
util.inherits($Primitive, $Type);

function $Union(types) {

  var reader = tap.Tap.prototype.readUnion;
  var writer = tap.Tap.prototype.writeUnion;

  this._reader = function () { return reader.call(this, types); };
  this._writer = function (obj) { return writer.call(this, obj, types); };

}
util.inherits($Union, $Type);

function $Fixed(name, size) {

  // TODO: Generate code with length set.

  var reader = tap.Tap.prototype.readFixed;
  var writer = tap.Tap.prototype.writeFixed;

  this.name = name;
  this._reader = function () { return reader.call(this, size); };
  this._writer = function (buf) { writer.call(this, buf, size); };

}
util.inherits($Fixed, $Type);

function $Map(type) {

  var reader = tap.Tap.prototype.readMap;
  var writer = tap.Tap.prototype.writeMap;

  this._type = type;
  this._reader = function () { return reader.call(this, type._reader); };
  this._writer = function (obj) { writer.call(this, obj, type._writer); };

}
util.inherits($Map, $Type);

function $Array(type) {

  var reader = tap.Tap.prototype.readArray;
  var writer = tap.Tap.prototype.writeArray;

  this._type = type;
  this._reader = function () { return reader.call(this, type._reader); };
  this._writer = function (arr) { writer.call(this, arr, type._writer); };

}
util.inherits($Array, $Type);

function $Record(name) {

  // TODO: Add support for default.

  this.name = name;

  // These get populated in the following `_initialize` call.
  this._fields = null;
  this._constructor = null;
  this._reader = null;
  this._writer = null;

}
util.inherits($Record, $Type);

$Record.prototype.getRecordConstructor = function () {

  return this._constructor;

};

$Record.prototype._initialize = function (fields) {

  /*jshint -W054 */

  var self = this;
  var names = fields.map(function (field) { return field.name; });
  var body = names.map(function (s) { return 'this.' + s + ' = ' + s + ';'; });
  self._fields = fields;
  self._constructor = new Function(names.join(','), body.join('\n'));
  self._reader = self._generateReader(fields);
  self._writer = self._generateWriter(fields);

  self._constructor.fromAvro = self.decode.bind(self); // Convenience.
  self._constructor.prototype = {
    $name: self.name,
    $fields: names,
    $size: 1000, // TODO: Learn self.
    $toAvro: function () { self.encode(this); }
  };

};

$Record.prototype._generateReader = function (fields) {

  /*jshint -W054 */

  var names = [];
  var values = [this._constructor];
  var i, l;
  for (i = 0, l = fields.length; i < l; i++) {
    names.push('r' + i);
    values.push(fields[i].type._reader);
  }
  var body = 'return function() { return new Record(';
  body += names.map(function (r) { return r + '.call(this)'; }).join(',');
  body += '); };';
  names.unshift('Record');
  return new Function(names.join(','), body).apply(undefined, values);

};

$Record.prototype._generateWriter = function (fields) {

  /*jshint -W054 */

  var names = [];
  var values = [];
  var body = 'return function(obj) {\n';
  var i, l, field;
  for (i = 0, l = fields.length; i < l; i++) {
    field = fields[i];
    names.push('w' + i);
    values.push(field.type._writer);
    body += 'w' + i + '.call(this, obj.' + field.name + ');\n';
  }
  body += '}';
  return new Function(names.join(','), body).apply(undefined, values);

};

// Generate all primitive types along with their (de)serializers.
var PRIMITIVES = (function () {
  var primitives = [
    'Null', 'Boolean', 'Int', 'Long', 'Float', 'Double', 'Bytes', 'String'
  ]; // Capitalized for convenience below.
  return primitives.map(function (name) { return new $Primitive(name); });
})();

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

module.exports = {
  $Type: $Type,
  $Primitive: $Primitive,
  $Union: $Union,
  $Fixed: $Fixed,
  $Map: $Map,
  $Array: $Array,
  $Record: $Record,
  PRIMITIVES: PRIMITIVES
};
