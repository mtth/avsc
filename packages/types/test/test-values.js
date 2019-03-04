/* jshint node: true, mocha: true */

'use strict';

var types = require('../lib/types'),
    values = require('../lib/values'),
    assert = require('assert');

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
          {name: 'age', type: ['null', 'int']}
        ]
      });
      var foo = {name: 'bar', age: 1234};
      assert.deepStrictEqual(toJSON(foo, t), {name: 'bar', age: {int: 1234}});
    });
  });
});
