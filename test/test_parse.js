/* jshint node: true, mocha: true */

'use strict';

var parse = require('../lib/parse'),
    assert = require('assert');

suite('parse', function () {

  suite('PrimitiveType', function () {

    var data = [
      {
        name: 'int',
        valid: [1, -3, 12314, 0, 1e9],
        invalid: [null, 'hi', undefined, 1.5, 1e28, 123124123123213]
      },
      {
        name: 'long',
        valid: [1, -3, 12314, 9007199254740991],
        invalid: [null, 'hi', undefined, 9007199254740992, 1.3, 1e67]
      },
      {
        name: 'string',
        valid: ['', 'hi'],
        invalid: [null, undefined, 1, 0]
      },
      {
        name: 'null',
        valid: [null],
        invalid: [0, 1, 'hi', undefined]
      },
      // { TODO: Uncomment when implemented.
      //   name: 'float',
      //   valid: [1, -3.4, 12314e31],
      //   invalid: [null, 'hi', undefined, 5e38]
      // },
      // {
      //   name: 'double',
      //   valid: [1, -3.4, 12314e31, 5e38],
      //   invalid: [null, 'hi', undefined, 5e89]
      // },
      {
        name: 'bytes',
        valid: [new Buffer(1), new Buffer('abc')],
        invalid: [null, 'hi', undefined, 1, 0, -3.5],
        check: function (a, b) { assert(a.equals(b)); }
      }
    ];

    data.forEach(function (elem) {
      test(elem.name, function () {
        var type = new parse.types.PrimitiveType(elem.name);
        elem.valid.forEach(function (v) {
          assert(type.isValid(v), '' + v);
          var fn = elem.check || assert.equal;
          fn(type.decode(type.encode(v)), v);
        });
        elem.invalid.forEach(function (v) {
          assert(!type.isValid(v), '' + v);
        });
        assert(type.isValid(type.random()));
      });
    });

    test('invalid', function () {
      assert.throws(
        function () { new parse.types.PrimitiveType('foo'); },
        parse.ParseError
      );
    });

  });

  suite('EnumType', function () {

    test('empty', function () {
      assert.throws(
        function () { new parse.types.EnumType({name: 'Foo', symbols: []}); },
        parse.ParseError
      );
    });

    test('no symbols', function () {
      assert.throws(
        function () { new parse.types.EnumType({name: 'Foo'}); },
        parse.ParseError
      );
    });

    test('no name', function () {
      assert.throws(
        function () { new parse.types.EnumType({symbols: ['HI']}); },
        parse.ParseError
      );
    });

    test('single symbol', function () {
      var symbols = ['HI'];
      var t = new parse.types.EnumType({name: 'Foo', symbols: symbols});
      assert.equal(t.getTypeName(), 'enum');
      assert.equal(t.random(), 'HI');
      assert.deepEqual(t.symbols, symbols);
      assert(t.isValid('HI'));
      assert(!t.isValid('HEY'));
      assert(!t.isValid(null));
    });

    test('multiple symbols', function () {
      var symbols = ['HI', 'HEY'];
      var t = new parse.types.EnumType({name: 'Foo', symbols: symbols});
      assert.deepEqual(t.symbols, symbols);
      assert(t.isValid('HI'));
      assert(t.isValid('HEY'));
      assert(!t.isValid('HELLO'));
    });

  });

  suite('FixedType', function () {

    test('empty', function () {
      assert.throws(
        function () { new parse.types.FixedType({name: 'Foo', size: 0}); },
        parse.ParseError
      );
    });

    test('no size', function () {
      assert.throws(
        function () { new parse.types.FixedType({name: 'Foo'}); },
        parse.ParseError
      );
    });

    test('no name', function () {
      assert.throws(
        function () { new parse.types.FixedType({symbols: ['HI']}); },
        parse.ParseError
      );
    });

    test('size 1', function () {
      var t = new parse.types.FixedType({name: 'F', namespace: 'h', size: 1});
      assert.equal(t.name, 'h.F');
      assert.equal(t.getTypeName(), 'fixed');
      var buf = t.random();
      assert(Buffer.isBuffer(buf) && buf.length === 1);
      assert(t.isValid(new Buffer(1)));
      assert(!t.isValid(new Buffer(2)));
      assert(!t.isValid(null));
    });

  });

  suite('MapType', function () {

    test('no values', function () {
      assert.throws(
        function () { new parse.types.MapType({}); },
        parse.ParseError
      );
    });

    test('int', function () {
      var t = new parse.types.MapType({values: 'int'});
      // TODO.
    });

  });

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
