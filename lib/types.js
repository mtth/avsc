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
  this._fields = [];
  this._constructor = null;
  // TODO: Code generate this, to be used by reader and writer. Possible using
  // just the field names to first generate just the constructor (so that it
  // can be inserted in the map. Then code generate the reader and writer.
  this._reader = null;
  this._writer = null;

}

$Record.prototype.setFields = function (fields) {

  var len = fields.length;
  this._fields = fields;
  this._reader = function () {
    var obj = {};
    var i, field;
    for (i = 0; i < len; i++) {
      field = fields[i];
      obj[field.name] = field.type._reader.call(this);
    }
    return obj;
  };
  this._writer = function (obj) {
    var i, field;
    for (i = 0; i < len; i++) {
      field = fields[i];
      field.type._writer.call(this, obj[field.name]);
    }
  };

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
