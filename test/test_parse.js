/* jshint node: true, mocha: true */

'use strict';

var parse = require('../lib/parse'),
    assert = require('assert');

suite('parse', function () {

  test('schema instance', function () {
    var type = parse.parse({
      type: 'record',
      name: 'Person',
      fields: [{name: 'so', type: 'Person'}]
    });
    assert.strictEqual(parse.parse(type), type);
  });

  test('wrapped primitive', function () {
    var type = parse.parse({
      type: 'record',
      name: 'Person',
      fields: [
        {name: 'firstName', type: 'int'},
        {name: 'lastName', type: {type: 'int'}}
      ]
    });
    assert.strictEqual(type.fields[0].type, type.fields[1].type);
  });

  suite('PrimitiveType', function () {

    var data = [
      {
        schema: 'int',
        valid: [1, -3, 12314, 0, 1e9],
        invalid: [null, 'hi', undefined, 1.5, 1e28, 123124123123213]
      },
      {
        schema: 'long',
        valid: [1, -3, 12314, 9007199254740991],
        invalid: [null, 'hi', undefined, 9007199254740992, 1.3, 1e67]
      },
      {
        schema: 'string',
        valid: ['', 'hi'],
        invalid: [null, undefined, 1, 0]
      },
      {
        schema: 'null',
        valid: [null],
        invalid: [0, 1, 'hi', undefined]
      },
      {
        schema: 'float',
        valid: [1, -3.4, 12314e31],
        invalid: [null, 'hi', undefined, 5e38],
        check: function (a, b) { assert(floatEquals(a, b)); }
      },
      {
        schema: 'double',
        valid: [1, -3.4, 12314e31, 5e37],
        invalid: [null, 'hi', undefined],
        check: function (a, b) { assert(floatEquals(a, b), '' + [a, b]); }
      },
      {
        schema: 'bytes',
        valid: [new Buffer(1), new Buffer('abc')],
        invalid: [null, 'hi', undefined, 1, 0, -3.5]
      }
    ];

    var schemas = ['foo', ''];

    testType(parse.types.PrimitiveType, data, schemas);

    test('encode int', function () {

      var type = new parse.types.PrimitiveType('int');
      assert.equal(type.decode(new Buffer([0x80, 0x01])), 64);
      assert(new Buffer([0]).equals(type.encode(0)));

    });

    test('decode string', function () {

      var type = new parse.types.PrimitiveType('string');
      var buf = new Buffer([0x06, 0x68, 0x69, 0x21]);
      var s = 'hi!';
      assert.equal(type.decode(buf), s);
      assert(buf.equals(type.encode(s)));

    });

    test('encode string', function () {

      var type = new parse.types.PrimitiveType('string');
      var buf = new Buffer([0x06, 0x68, 0x69, 0x21]);
      assert(buf.equals(type.encode('hi!', 1)));

    });

  });

  suite('EnumType', function () {

    var data = [
      {
        name: 'single symbol',
        schema: {name: 'Foo', symbols: ['HI']},
        valid: ['HI'],
        invalid: ['HEY', null, undefined, 0]
      },
      {
        name: 'number-ish as symbol',
        schema: {name: 'Foo', symbols: ['HI', '0']},
        valid: ['HI', '0'],
        invalid: ['HEY', null, undefined, 0]
      }
    ];

    var schemas = [
      {name: 'Foo', symbols: []},
      {name: 'Foo'},
      {symbols: ['hi']}
    ];

    testType(parse.types.EnumType, data, schemas);

  });

  suite('FixedType', function () {

    var data = [
      {
        name: 'size 1',
        schema: {name: 'Foo', size: 1},
        valid: [new Buffer(1)],
        invalid: ['HEY', null, undefined, 0, new Buffer(2)],
        check: function (a, b) { assert(a.equals(b)); }
      }
    ];

    var schemas = [
      {name: 'Foo', size: 0},
      {name: 'Foo', size: -2},
      {size: 2},
      {name: 'Foo'},
      {}
    ];

    testType(parse.types.FixedType, data, schemas);

  });

  suite('MapType', function () {

    var data = [
      {
        name: 'int',
        schema: {values: 'int'},
        valid: [{one: 1}, {two: 2, o: 0}],
        invalid: [1, {o: null}, [], undefined, {o: 'hi'}, {1: '', 2: 3}, ''],
        check: assert.deepEqual
      },
      {
        name: 'enum',
        schema: {values: {type: 'enum', name: 'a', symbols: ['A', 'B']}},
        valid: [{a: 'A'}, {a: 'A', b: 'B'}, {}],
        invalid: [{o: 'a'}, {1: 'A', 2: 'b'}, {a: 3}],
        check: assert.deepEqual
      },
      {
        name: 'array of string',
        schema: {values: {type: 'array', items: 'string'}},
        valid: [{a: []}, {a: ['A'], b: ['B', '']}, {}],
        invalid: [{o: 'a', b: []}, {a: [1, 2]}, {a: {b: ''}}],
        check: assert.deepEqual
      }
    ];

    var schemas = [
      {},
      {values: ''},
      {values: {type: 'array'}}
    ];

    testType(parse.types.MapType, data, schemas);

  });

  suite('WrappedUnionType', function () {

    var data = [
      {
        name: 'null and string',
        schema: ['null', 'string'],
        valid: [null, {string: 'hi'}],
        invalid: ['null', undefined, {string: 1}],
        check: assert.deepEqual
      },
    ];

    var schemas = [
      {},
      [],
      ['null', 'null'],
      ['null', {type: 'map', values: 'int'}, {type: 'map', values: 'long'}]
    ];

    testType(parse.types.WrappedUnionType, data, schemas);

    test('instanceof Union', function () {
      var type = new parse.types.WrappedUnionType(['null', 'int']);
      assert(type instanceof parse.types.UnionType);
    });

  });

  suite('UnwrappedUnionType', function () {

    var data = [
      {
        name: 'null and string',
        schema: ['null', 'string'],
        valid: [null, 'hi'],
        invalid: [undefined, 2, {string: 1}],
        check: assert.deepEqual
      },
    ];

    var schemas = [];

    testType(parse.types.UnwrappedUnionType, data, schemas);

    test('instanceof Union', function () {
      var type = new parse.types.UnwrappedUnionType(['null', 'int']);
      assert(type instanceof parse.types.UnionType);
    });

  });

  suite('RecordType', function () {

    var data = [
      {
        name: 'union field null and string with default',
        schema: {
          type: 'record',
          name: 'a',
          fields: [{name: 'b', type: ['null', 'string'], 'default': null}]
        },
        valid: [],
        invalid: [],
        check: assert.deepEqual
      }
    ];

    var schemas = [
      {type: 'record', name: 'a', fields: ['null', 'string']},
      {type: 'record', name: 'a', fields: [{type: ['null', 'string']}]},
      {
        type: 'record',
        name: 'a',
        fields: [{name: 'b', type: ['null', 'string'], 'default': 'a'}]
      },
    ];

    testType(parse.types.RecordType, data, schemas);

    test('duplicate field names', function () {
      assert.throws(function () {
        parse.parse({
          type: 'record',
          name: 'Person',
          fields: [{name: 'age', type: 'int'}, {name: 'age', type: 'float'}]
        });
      }, parse.AvscError);
    });

    test('default constructor', function () {
      var type = parse.parse({
        type: 'record',
        name: 'Person',
        fields: [{name: 'age', type: 'int', 'default': 25}]
      });
      var Person = type.getRecordConstructor();
      var p = new Person();
      assert.equal(p.age, 25);
    });

    test('default check & write', function () {
      var type = parse.parse({
        type: 'record',
        name: 'Person',
        fields: [{name: 'age', type: 'int', 'default': 25}]
      });
      assert.deepEqual(type.encode({}), new Buffer([50]));
    });

    test('fixed string default', function () {
      var s = '\x01\x04';
      var b = new Buffer(s);
      var type = parse.parse({
        type: 'record',
        name: 'Object',
        fields: [
          {
            name: 'id',
            type: {type: 'fixed', size: 2, name: 'Id'},
            'default': s
          }
        ]
      });

      assert.deepEqual(type.fields[0]['default'], s);

      var obj = new (type.getRecordConstructor())();
      assert.deepEqual(obj.id, new Buffer([1, 4]));
      assert.deepEqual(type.encode({}), b);
    });

    test('fixed buffer default', function () {
      var s = '\x01\x04';
      var b = new Buffer(s);
      var type = parse.parse({
        type: 'record',
        name: 'Object',
        fields: [
          {
            name: 'id',
            type: {type: 'fixed', size: 2, name: 'Id'},
            'default': b
          }
        ]
      });
      assert.deepEqual(type.fields[0]['default'], s);
    });

    test('fixed buffer invalid default', function () {
      assert.throws(function () {
        parse.parse({
          type: 'record',
          name: 'Object',
          fields: [
            {
              name: 'id',
              type: {type: 'fixed', size: 2, name: 'Id'},
              'default': new Buffer([0])
            }
          ]
        });
      }, parse.AvscError);
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

  suite('type names', function () {

    test('existing', function () {
      var type = parse.parse({
        type: 'record',
        name: 'Person',
        fields: [{name: 'so', type: 'Person'}]
      });

      assert.strictEqual(type, type.fields[0].type);
    });

    test('namespaced', function () {
      var type = parse.parse({
        type: 'record',
        name: 'Person',
        fields: [
          {
            name: 'so',
            type: {
              type: 'record',
              name: 'Person',
              fields: [{name: 'age', type: 'int'}],
              namespace: 'a'
            }
          }
        ]
      });

      assert.equal(type.name, 'Person');
      assert.equal(type.fields[0].type.name, 'a.Person');
    });

    test('redefining', function () {
      assert.throws(function () {
        parse.parse({
          type: 'record',
          name: 'Person',
          fields: [
            {
              name: 'so',
              type: {
                type: 'record',
                name: 'Person',
                fields: [{name: 'age', type: 'int'}]
              }
            }
          ]
        });
      }, parse.AvscError);
    });

    test('missing', function () {
      assert.throws(function () {
        parse.parse({
          type: 'record',
          name: 'Person',
          fields: [{name: 'so', type: 'Friend'}]
        });
      }, parse.AvscError);
    });

    test('redefining primitive', function () {
      assert.throws( // Unqualified.
        function () { parse.parse({type: 'fixed', name: 'int', size: 2}); },
        parse.AvscError
      );
      assert.throws( // Qualified.
        function () {
          parse.parse({type: 'fixed', name: 'int', size: 2, namespace: 'a'});
        },
        parse.AvscError
      );
    });

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
          parse.AvscError
        );
      });
    });
  }

}

function testType(Type, data, invalidSchemas) {

  data.forEach(function (elem) {
    test(elem.name || elem.schema, function () {
      var type = new Type(elem.schema);
      elem.valid.forEach(function (v) {
        assert(type.isValid(v), '' + v);
        var fn = elem.check || assert.deepEqual;
        fn(type.decode(type.encode(v)), v);
      });
      elem.invalid.forEach(function (v) {
        assert(!type.isValid(v), '' + v);
      });
      assert(type.isValid(type.random()));
    });
  });

  test('invalid', function () {
    invalidSchemas.forEach(function (schema) {
      assert.throws(function () { new Type(schema); }, parse.AvscError);
    });
  });

}

function floatEquals(a, b) {

  return Math.abs((a - b) / Math.min(a, b)) < 1e-7;

}
