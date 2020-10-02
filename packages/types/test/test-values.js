/* jshint node: true, mocha: true */

'use strict';

var types = require('../lib/types'),
    values = require('../lib/values'),
    assert = require('assert'),
    util = require('util');

var Type = types.constructors.Type;

suite('values', function () {
  suite('fromJSON', function () {
    var fromJSON = values.fromJSON;

    test('int', function () {
      var t = Type.forSchema('int');
      assert.equal(fromJSON(123, t), 123);
    });

    test('record', function () {
      var t = Type.forSchema({
        type: 'record',
        name: 'Foo',
        fields: [
          {name: 'name', type: 'string'},
          {name: 'age', type: 'int'}
        ]
      });
      var foo = {name: 'bar', age: 1234};
      assert.deepEqual(fromJSON(foo, t), foo);
    });

    test('record with default', function () {
      var t = Type.forSchema({
        type: 'record',
        name: 'Foo',
        fields: [
          {name: 'age', type: 'int', default: 20}
        ]
      });
      assert.deepEqual(fromJSON({}, t), {age: 20});
    });

    test('record missing fields', function () {
      var t = Type.forSchema({
        type: 'record',
        name: 'Foo',
        fields: [
          {name: 'age', type: 'int', default: 20}
        ]
      });
      assert.deepEqual(fromJSON({}, t), {age: 20});
    });

    test('record with multiple fields and default', function () {
      var t = Type.forSchema({
        type: 'record',
        name: 'Foo',
        fields: [
          {name: 'f2', type: 'string'},
          {name: 'f3', type: 'int', default: 3}
        ]
      });
      assert.deepEqual(fromJSON({f2: 'a'}, t), {f2: 'a', f3: 3});
    });

    test('record missing fields', function () {
      var t = Type.forSchema({
        type: 'record',
        name: 'Foo',
        fields: [{name: 'age', type: 'int'}]
      });
      assert.throws(function () { fromJSON({}, t); }, /missing 1 field/);
    });

    test('abstract long', function () {
      var t = types.constructors.LongType.__with({
        fromBuffer: function () { throw new Error('boom'); },
        toBuffer: function () { throw new Error('boom'); },
        compare: function () { throw new Error('boom'); },
        isValid: function (n) { return typeof n == 'string'; },
        fromJSON: function (n) { return '' + n; },
        toJSON: function (n) { return +n; },
      });
      assert.equal(fromJSON(123, t), '123');
    });

  });

  suite('fromDefaultJSON', function () {
    var fromDefaultJSON = values.fromDefaultJSON;

    test('int', function () {
      var t = Type.forSchema('int');
      assert.equal(fromDefaultJSON(123, t), 123);
    });

    test('record', function () {
      var t = Type.forSchema({
        type: 'record',
        name: 'Foo',
        fields: [
          {name: 'name', type: 'string'},
          {name: 'age', type: 'int'}
        ]
      });
      var foo = {name: 'bar', age: 1234};
      assert.deepEqual(fromDefaultJSON(foo, t), foo);
    });

    test('unwrapped union', function () {
      var t = Type.forSchema(['int', 'null']);
      assert.equal(fromDefaultJSON(2, t), 2);
    });

    test('wrapped union non null', function () {
      var t = Type.forSchema(['int', 'null', 'double']);
      assert.deepEqual(fromDefaultJSON(3, t), {int: 3});
    });

    test('wrapped union null', function () {
      var t = Type.forSchema(['null', 'double', 'int']);
      assert.deepEqual(fromDefaultJSON(null, t), null);
    });
  });

  suite('toJSON', function () {
    var toJSON = values.toJSON;

    test('int', function () {
      var t = Type.forSchema('int');
      assert.equal(toJSON(123, t), 123);
    });

    test('record', function () {
      var t = Type.forSchema({
        type: 'record',
        name: 'Foo',
        fields: [
          {name: 'name', type: 'string'},
          {name: 'age', type: ['null', 'int'], default: null}
        ]
      });
      var ok = {name: 'bar', age: 1234};
      var json = {name: 'bar', age: {int: 1234}};
      assert.deepEqual(toJSON(ok, t), json);
      assert.deepEqual(toJSON({name: 'b'}, t), {name: 'b', age: null});
      assert.throws(function () { toJSON('a', t); }, /not a valid object/);
      var extra = {name: 'bar', age: 1234, address: 'here'};
      assert.deepEqual(toJSON(extra, t, {allowUndeclaredFields: true}), json);
      assert.throws(function () { toJSON(extra, t); }, /undeclared field/);
    });

    test('record omitting default', function () {
      var t = Type.forSchema({
        type: 'record',
        name: 'Foo',
        fields: [
          {name: 'name', type: 'string'},
          {name: 'age', type: ['null', 'int'], default: null}
        ]
      });
      assert.deepEqual(
        toJSON({name: 'bar', age: null}, t, {omitDefaultValues: true}),
        {name: 'bar'}
      );
    });

    test('map', function () {
      var t = Type.forSchema({
        type: 'map',
        values: ['null', 'string'],
      });
      assert.deepEqual(
        toJSON({name: 'bar'}, t),
        {name: {string: 'bar'}}
      );
      assert.deepEqual(
        toJSON({name: '', age: null}, t),
        {name: {string: ''}, age: null}
      );
    });

    test('array', function () {
      var t = Type.forSchema({type: 'array', items: 'int'});
      assert.deepEqual(toJSON([], t), []);
      assert.deepEqual(toJSON([1, 3, 2], t), [1, 3, 2]);

      assert.throws(function () { toJSON([1, null], t); }, /\[1\]/);
      assert.throws(function () { toJSON({one: 1}, t); }, /not an array/);
    });

    test('logical type', function () {
      var t = Type.forSchema({
        type: 'long',
        logicalType: 'date',
        items: 'int'
      }, {logicalTypes: {date: DateType}});
      var d = new Date(1234);
      assert.equal(toJSON(d, t), 1234);
      assert.throws(function () { toJSON(123, t); }, /encoding failed/);
    });

    test('abstract long', function () {
      var t = types.constructors.LongType.__with({
        fromBuffer: function () { throw new Error('boom'); },
        toBuffer: function () { throw new Error('boom'); },
        compare: function () { throw new Error('boom'); },
        isValid: function (n) { return typeof n == 'string'; },
        fromJSON: function (n) { return '' + n; },
        toJSON: function (n) { return +n; },
      });
      assert.equal(toJSON('1234', t), 1234);
    });
  });
});

function DateType(schema, opts) {
  types.constructors.LogicalType.call(this, schema, opts);
}
util.inherits(DateType, types.constructors.LogicalType);

DateType.prototype._fromValue = function (val) { return new Date(val); };

DateType.prototype._toValue = function (date) {
  if (!(date instanceof Date)) {
    return undefined;
  }
  if (this.underlyingType.typeName === 'long') {
    return +date;
  } else {
    // String.
    return '' + date;
  }
};
