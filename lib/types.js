/* jshint node: true */

'use strict';

var tap = require('./tap');

function $Primitive(name) {

  this.name = name.toLowerCase();
  this._reader = tap.Tap.prototype['read' + name];
  this._writer = tap.Tap.prototype['write' + name];

}

function $Union(types) {

  var reader = tap.Tap.prototype.readUnion;
  var writer = tap.Tap.prototype.writeUnion;

  this._reader = function () { return reader.call(this, types); };
  this._writer = function (obj) { return writer.call(this, obj, types); };

}

function $Fixed(name, size) {

  // TODO: Generate code with length set.

  var reader = tap.Tap.prototype.readFixed;
  var writer = tap.Tap.prototype.writeFixed;

  this.name = name;
  this._reader = function () { return reader.call(this, size); };
  this._writer = function (buf) { writer.call(this, buf, size); };

}

function $Map(type) {

  var reader = tap.Tap.prototype.readMap;
  var writer = tap.Tap.prototype.writeMap;

  this._type = type;
  this._reader = function () { return reader.call(this, type._reader); };
  this._writer = function (obj) { writer.call(this, obj, type._writer); };

}

function $Array(type) {

  var reader = tap.Tap.prototype.readArray;
  var writer = tap.Tap.prototype.writeArray;

  this._type = type;
  this._reader = function () { return reader.call(this, type._reader); };
  this._writer = function (arr) { writer.call(this, arr, type._writer); };

}

function $Record(name) {

  // TODO: Add support for default.

  this.name = name;

  // These get populated in the following `setFields` call.
  this._fields = null;
  this._constructor = null;
  this._reader = null;
  this._writer = null;

}

$Record.prototype.setFields = function (fields) {

  this._fields = fields;

  var names = fields.map(function (field) { return field.name; });
  var body = names.map(function (s) { return 'this.' + s + ' = ' + s + ';'; });
  this._constructor = new Function(names.join(','), body.join('\n'));
  this._constructor.prototype.$name = this.name;
  this._reader = this._generateReader(fields);
  this._writer = this._generateWriter(fields);

};

$Record.prototype._generateReader = function (fields) {

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

module.exports = {
  $Primitive: $Primitive,
  $Union: $Union,
  $Fixed: $Fixed,
  $Map: $Map,
  $Array: $Array,
  $Record: $Record,
  PRIMITIVES: PRIMITIVES
};
