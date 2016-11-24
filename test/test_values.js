/* jshint node: true, mocha: true */

'use strict';

var types = require('../lib/types'),
    values = require('../lib/values'),
    assert = require('assert'),
    util = require('util');


var LogicalType = types.builtins.LogicalType;
var Type = types.Type;

suite('values', function () {

  suite('combine', function () {

    var combine = values.combine;

    test('empty', function () {
      assert.throws(function () { combine([]); });
    });

    test('numbers', function () {
      var t1 = Type.create('int');
      var t2 = Type.create('long');
      var t3 = Type.create('float');
      var t4 = Type.create('double');
      assert.strictEqual(combine([t1, t2]), t2);
      assert.strictEqual(combine([t1, t2, t3, t4]), t4);
      assert.strictEqual(combine([t3, t2]), t3);
      assert.strictEqual(combine([t2]), t2);
    });

    test('string & int', function () {
      var t1 = Type.create('int');
      var t2 = Type.create('string');
      assertUnionsEqual(combine([t1, t2]), Type.create(['int', 'string']));
    });

    test('records & maps', function () {
      var t1 = Type.create({
        type: 'record',
        fields: [{name: 'foo', type: 'int', 'default': 2}]
      });
      var t2 = Type.create({type: 'map', values: 'string'});
      var t3;
      t3 = combine([t1, t2]);
      assertUnionsEqual(t3.getValuesType(), Type.create(['int', 'string']));
      t3 = combine([t2, t1]);
      assertUnionsEqual(t3.getValuesType(), Type.create(['int', 'string']));
    });

    test('arrays', function () {
      var t1 = Type.create({type: 'array', items: 'null'});
      var t2 = Type.create({type: 'array', items: 'int'});
      var t3 = combine([t1, t2]);
      assertUnionsEqual(t3.getItemsType(), Type.create(['null', 'int']));
    });

    test('field single default', function () {
      var t1 = Type.create({
        type: 'record',
        fields: [{name: 'foo', type: 'int', 'default': 2}]
      });
      var t2 = Type.create({
        type: 'record',
        fields: []
      });
      var t3 = combine([t1, t2], {strictDefaults: true});
      assert.deepEqual(
        JSON.parse(t3.getSchema({exportAttrs: true})),
        {
          type: 'record',
          fields: [
            {name: 'foo', type: 'int', 'default': 2}
          ]
        }
      );
    });

    test('field multiple types default', function () {
      var t1 = Type.create({
        type: 'record',
        fields: [{name: 'foo', type: 'string'}]
      });
      var t2 = Type.create({
        type: 'record',
        fields: [{name: 'foo', type: 'int', 'default': 2}]
      });
      var t3 = combine([t1, t2], {strictDefaults: true});
      assert.deepEqual(
        JSON.parse(t3.getSchema({exportAttrs: true})),
        {
          type: 'record',
          fields: [
            // Int should be first in the union.
            {name: 'foo', type: ['int', 'string'], 'default': 2}
          ]
        }
      );
    });

    test('missing fields no null default', function () {
      var t1 = Type.create({
        type: 'record',
        fields: [{name: 'foo', type: 'int'}, {name: 'bar', type: 'string'}]
      });
      var t2 = Type.create({
        type: 'record',
        fields: [{name: 'bar', type: 'string'}]
      });
      var t3;
      t3 = combine([t1, t2]);
      assert.deepEqual(
        JSON.parse(t3.getSchema({exportAttrs: true})),
        {
          type: 'record',
          fields: [
            // The null branch should always be first here.
            {name: 'foo', type: ['null', 'int'], 'default': null},
            {name: 'bar', type: 'string'}
          ]
        }
      );
      t3 = combine([t1, t2], {strictDefaults: true});
      assertUnionsEqual(t3.getValuesType(), Type.create(['int', 'string']));
    });

    test('logical types', function () {
      var opts = {logicalTypes: {even: EvenType, odd: OddType}};

      function EvenType(attrs, opts) { LogicalType.call(this, attrs, opts); }
      util.inherits(EvenType, LogicalType);
      EvenType.prototype._fromValue = function (val) { return 2 * val; };
      EvenType.prototype._toValue = function (any) {
        if (any === (any | 0) && any % 2 === 0) {
          return any / 2;
        }
      };

      function OddType(attrs, opts) { LogicalType.call(this, attrs, opts); }
      util.inherits(OddType, LogicalType);
      OddType.prototype._fromValue = function (val) { return 2 * val + 1; };
      OddType.prototype._toValue = function (any) {
        if (any === (any | 0) && any % 2 === 1) {
          return any / 2;
        }
      };

      var t1 = Type.create({type: 'int', logicalType: 'even'}, opts);
      var t2 = Type.create({type: 'long', logicalType: 'odd'}, opts);
      assertUnionsEqual(combine([t1, t2]), Type.create([t1, t2]));
      assert.throws(function () { combine([t1, t1]); });
    });

    test('invalid wrapped union', function () {
      var t1 = Type.create(['int'], {wrapUnions: true});
      var t2 = Type.create('string');
      assert.throws(function () { combine([t1, t2]); }, /cannot combine/);
    });

    test('error while creating wrapped union', function () {
      var opts = {typeHook: hook, wrapUnions: false};
      var t1 = Type.create(['int'], {wrapUnions: true});
      var t2 = Type.create(['string'], {wrapUnions: true});
      assert.throws(function () { combine([t1, t2], opts); }, /foo/);
      assert(!opts.wrapUnions);

      function hook() { throw new Error('foo'); }
    });

    test('inconsistent wrapped union', function () {
      var t1 = Type.create(
        [{type: 'fixed', name: 'Id', size: 2}],
        {wrapUnions: true}
      );
      var t2 = Type.create(
        [{type: 'fixed', name: 'Id', size: 3}],
        {wrapUnions: true}
      );
      assert.throws(function () { combine([t1, t2]); }, /inconsistent/);
    });

    test('valid wrapped unions', function () {
      var opts = {wrapUnions: true};
      var t1 = Type.create(['int', 'string', 'null'], opts);
      var t2 = Type.create(['null', 'long'], opts);
      assertUnionsEqual(
        combine([t1, t2]),
        Type.create(['int', 'long', 'string', 'null'], opts)
      );
    });

    test('valid unwrapped unions', function () {
      var t1 = Type.create(['int', 'string', 'null']);
      var t2 = Type.create(['null', 'long']);
      assertUnionsEqual(
        combine([t1, t2]),
        Type.create(['long', 'string', 'null'])
      );
    });

    test('buffers', function () {
      var t1 = Type.create({type: 'fixed', size: 2});
      var t2 = Type.create({type: 'fixed', size: 4});
      var t3 = Type.create('bytes');
      assert.strictEqual(combine([t1, t1]), t1);
      assert.strictEqual(combine([t1, t3]), t3);
      assert(combine([t1, t2]).equals(t3));
    });

    test('strings', function () {
      var t1 = Type.create({type: 'enum', symbols: ['A', 'b']});
      var t2 = Type.create({type: 'enum', symbols: ['A', 'B']});
      var t3 = Type.create('string');
      var symbols;
      symbols = combine([t1, t1]).getSymbols();
      assert.deepEqual(symbols.sort(), ['A', 'b']);
      assert.strictEqual(combine([t1, t3]), t3);
      assert.strictEqual(combine([t1, t2, t3]), t3);
      symbols = combine([t1, t2]).getSymbols();
      assert.deepEqual(symbols.sort(), ['A', 'B', 'b']);
    });

    test('strings', function () {
      var opts = {wrapUnions: true};
      var t1 = Type.create(['null', 'int'], opts);
      var t2 = Type.create(['null', 'long', 'string'], opts);
      var t3 = Type.create(['string'], opts);
      var t4 = combine([t1, t2, t3]);
      assert.deepEqual(
        JSON.parse(t4.getSchema()),
        ['null', 'int', 'long', 'string']
      );
    });

  });

  suite('infer', function () {

    var infer = values.infer;

    test('numbers', function () {
      assert.equal(infer(1).getTypeName(), 'int');
      assert.equal(infer(1.2).getTypeName(), 'float');
      assert.equal(infer(9007199254740991).getTypeName(), 'double');
    });

    test('function', function () {
      assert.throws(function () { infer(function () {}); });
    });

    test('record', function () {
      var t = infer({b: true, n: null, s: '', f: new Buffer(0)});
      assert.deepEqual(
        JSON.parse(t.getSchema()),
        {
          type: 'record',
          fields: [
            {name: 'b', type: 'boolean'},
            {name: 'n', type: 'null'},
            {name: 's', type: 'string'},
            {name: 'f', type: 'bytes'}
          ]
        }
      );
    });

    test('empty array', function () {
      // Mostly check that the sentinel behaves correctly.
      var t1 = infer({0: [], 1: [true]});
      assert.equal(t1.getValuesType().getItemsType().getTypeName(), 'boolean');
      var t2 = infer({0: [], 1: [true], 2: [null]});
      assertUnionsEqual(
        t2.getValuesType().getItemsType(),
        Type.create(['boolean', 'null'])
      );
      var t3 = infer({0: [], 1: []});
      assert.equal(t3.getValuesType().getItemsType().getTypeName(), 'null');
    });

    test('value hook', function () {
      var t = infer({foo: 23, bar: 'hi'}, {valueHook: hook});
      assert.equal(t.getField('foo').getType().getTypeName(), 'long');
      assert.equal(t.getField('bar').getType().getTypeName(), 'string');
      assert.throws(function () {
        infer({foo: function () {}}, {valueHook: hook});
      });

      function hook(val, opts) {
        if (typeof val == 'number') {
          return Type.create('long', opts);
        }
        if (typeof val == 'function') {
          // This will throw an error.
          return null;
        }
      }
    });

  });

});

function assertUnionsEqual(t1, t2) {
  // The order of branches in combined unions is undefined, this function
  // allows a safe equality check.
  var b1 = {};
  var b2 = {};
  t1.getTypes().forEach(function (t) { b1[t.getName(true)] = t; });
  t2.getTypes().forEach(function (t) { b2[t.getName(true)] = t; });
  assert.deepEqual(b1, b2);
}
