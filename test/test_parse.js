/* jshint node: true, mocha: true */

'use strict';

var parse = require('../lib/parse'),
    assert = require('assert');

suite('parse', function () {

  suite('primitive schemas', function () {

    var intType = parse.parse('int');
    var stringType = parse.parse({type: 'string'});

    test('from string', function () {

      assert.equal(intType.decode(new Buffer([0x80, 0x01])), 64);
      assert(new Buffer([0]).equals(intType.encode(0)));

    });

    test('from object', function () {

      var buf = new Buffer([0x06, 0x68, 0x69, 0x21]);
      var s = 'hi!';
      assert.equal(stringType.decode(buf), s);
      assert(buf.equals(stringType.encode(s)));

    });

    test('isValid', function () {

      assert(intType.isValid(123));
      assert(!intType.isValid('hi'));
      assert(stringType.isValid('hi'));

    });

    test('encode', function () {

      var buf = new Buffer([0x06, 0x68, 0x69, 0x21]);
      assert(buf.equals(stringType.encode('hi!', 1)));

    });

  });

  suite('built-in complex schemas', function () {

    testElems({
      ok: [
        {
          name: 'map of ints',
          schema: {type: 'map', values: 'int'},
          obj: {one: 1, two: 2}
        },
        {
          name: 'map of arrays of strings',
          schema: {type: 'map', values: {type: 'array', items: 'string'}},
          obj: {foo: [], bar: ['hello', 'world!', '']}
        },
        {
          name: 'array of longs',
          schema: {type: 'array', items: 'long'},
          obj: [0, -123123, 958723, 123]
        },
        {
          name: 'array of maps of int',
          schema: {type: 'array', items: {type: 'map', values: 'int'}},
          obj: [{one: 1, two: 2}]
        }
      ],
      err: [
        {
          name: 'missing map values',
          schema: {type: 'map'}
        },
        {
          name: 'missing array items',
          schema: {type: 'array', values: 'int'}
        },
        {
          name: 'missing fixed size',
          schema: {type: 'fixed', name: 'foo'}
        },
        {
          name: 'missing fixed name',
          schema: {type: 'fixed', size: 5}
        }
      ]
    });

  });

  suite('record schemas', function () {

    testElems({
      ok: [
        {
          name: 'flat single int field',
          schema: {
            type: 'record', name: 'Foo', fields: [{name: 'bar', type: 'int'}]
          },
          obj: {bar: 3}
        }
      ],
      err: [
        {
          name: 'missing fields',
          schema: {type: 'record', name: 'Foo'},
        },
        {
          name: 'missing name',
          schema: {type: 'record', fields: [{name: 'bar', type: 'int'}]}
        },
        {
          name: 'invalid fields',
          schema: {type: 'record', name: 'Foo', fields: {name: 'bar'}}
        },
        {
          name: 'invalid default',
          schema: {
            type: 'record',
            name: 'Foo',
            fields: [{name: 'bar', type: 'int', 'default': null}]
          }
        }
      ]
    });

    test('writer default', function () {

      var type = parse.parse({
        type: 'record',
        name: 'Person',
        fields: [
          {name: 'name', type: 'string', 'default': 'unknown'},
          {name: 'age', type: 'int'}
        ]
      });

      var x = {age: 23};
      var buf = type.encode(x);
      assert.deepEqual(type.decode(buf), {name: 'unknown', age: 23});

    });

  });

  function testElems(elems) {

    elems.ok.forEach(function (elem) {
      test(elem.name, function () {
        var type = parse.parse(elem.schema);
        assert.deepEqual(type.decode(type.encode(elem.obj)), elem.obj);
      });
    });

    if (elems.err) {
      elems.err.forEach(function (elem) {
        test(elem.name, function () {
          assert.throws(
            function () { parse.parse(elem.schema); },
            parse.ParseError
          );
        });
      });
    }

  }

});
