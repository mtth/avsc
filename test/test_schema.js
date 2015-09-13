/* jshint node: true, mocha: true */

'use strict';

var schema = require('../lib/schema'),
    tap = require('../lib/tap'),
    assert = require('assert');

suite('schema', function () {

  suite('primitive', function () {

    var sm = new schema.Schema('int');

    test('expand', function () {

      assert.equal(sm.value.type, 'int');

    });

    test('encode decode', function () {

      var buf = sm.encode(10);
      assert.equal(sm.decode(buf), 10);

    });

  });

  suite('invalid', function () {

    test('missing', function () {

      assert.throws(function () { new schema.Schema('Foo'); });

    });

  });

  suite('map', function () {

    test('read write', function () {

      var buf = new Buffer(20);
      buf.fill(0);
      var fl = new tap.Tap(buf);
      var sm = new schema.Schema({type: 'map', values: 'string'});
      var obj = {one: 'un', two: 'deux'};
      sm.value.writer.call(fl, obj);
      fl.offset = 0;
      assert.deepEqual(sm.value.reader.call(fl), obj);

    });

  });

  suite('flat record', function () {

    var sm = new schema.Schema({
      type: 'record',
      name: 'Person',
      namespace: 'world',
      fields: [
        {name: 'name', type: 'string'},
        {name: 'age', type: 'int'}
      ]
    });

    test('expand', function () {

      assert('world.Person' in sm.types);
      assert.equal(sm.value.fields[0].type.type, 'string');

    });

    test('encode decode', function () {

      var record = {name: 'bob', age: 23};
      var buf = sm.encode(record);
      assert.deepEqual(sm.decode(buf), record);

    });

  });

  suite('referenced record', function () {

    test('expand', function () {

      var obj = {
        type: 'record',
        name: 'Person',
        namespace: 'world',
        fields: [
          {name: 'name', type: 'string'},
          {name: 'age', type: 'int'},
          {name: 'friends', type: 'array', items: 'Person'}
        ]
      };
      var sm = new schema.Schema(obj);
      assert.equal(sm.value, sm.value.fields[2].type.items);

    });

  });

});
