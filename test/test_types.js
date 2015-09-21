/* jshint node: true, mocha: true */

'use strict';

var types = require('../lib/types'),
    utils = require('../lib/utils'),
    assert = require('assert');


var AvscError = utils.AvscError;
var fromSchema = types.Type.fromSchema;

suite('types', function () {

  suite('from schema', function  () {

    test('unknown types', function () {
      assert.throws(function () { fromSchema('a'); }, AvscError);
      assert.throws(function () { fromSchema({type: 'b'}); }, AvscError);
    });

    test('namespaced type', function () {
      var type = fromSchema({
        type: 'record',
        name: 'Human',
        namespace: 'earth',
        fields: [
          {
            name: 'id',
            type: {type: 'fixed', name: 'Id', size: 2, namespace: 'all'}
          },
          {
            name: 'alien',
            type: {
              type: 'record',
              name: 'Alien',
              namespace: 'all',
              fields: [
                {name: 'friend', type: 'earth.Human'},
                {name: 'id', type: 'Id'},
              ]
            }
          }
        ]
      });
      assert.equal(type.name, 'earth.Human');
      assert.equal(type.fields[0].type.name, 'all.Id');
      assert.equal(type.fields[1].type.name, 'all.Alien');
    });

    test('wrapped primitive', function () {
      var type = fromSchema({
        type: 'record',
        name: 'Person',
        fields: [
          {name: 'firstName', type: 'string'},
          {name: 'lastName', type: {type: 'string'}},
          {name: 'nothing', type: {type: 'null'}}
        ]
      });
      assert.strictEqual(type.fields[0].type, type.fields[1].type);
      assert.equal(type.fields[2].type.type, 'null');
    });

    test('decode truncated', function () {
      var type = fromSchema('int');
      assert.throws(function () {
        type.decode(new Buffer([128]));
      }, AvscError);
    });

    test('encode safe & unsafe', function () {
      var type = fromSchema('int');
      assert.throws(function () { type.encode('abc'); }, AvscError);
      type.encode('abc', {unsafe: true});
    });

    test('encode into buffer', function () {
      var type = fromSchema('string');
      var b1 = new Buffer(4);
      var b2;
      // Fits.
      b1.fill(0);
      b2 = type.encode('hi', {buffer: b1});
      assert.deepEqual(b1, new Buffer('\x04hi\x00'));
      assert.deepEqual(b2, new Buffer('\x04hi'));
      b2[0] = 0;
      assert.equal(b1[0], 0);
      // Doesn't fit.
      b1.fill(0);
      b2 = type.encode('hello', {buffer: b1});
      b2[0] = 0;
      assert.equal(b1[0], 10);
    });

    test('wrap & unwrap unions', function () {
      // Default is to wrap.
      var type;
      type = fromSchema(['null', 'int']);
      assert(type instanceof types.WrappedUnionType);
      type = fromSchema(['null', 'int'], {unwrapUnions: true});
      assert(type instanceof types.UnwrappedUnionType);
    });

  });

  suite('PrimitiveType', function () {

    var data = [
      {
        schema: 'boolean',
        valid: [true, false],
        invalid: [null, 'hi', undefined, 1.5, 1e28, 123124123123213]
      },
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

    testType(types.PrimitiveType, data, schemas);

    test('encode int', function () {

      var type = new types.PrimitiveType('int');
      assert.equal(type.decode(new Buffer([0x80, 0x01])), 64);
      assert(new Buffer([0]).equals(type.encode(0)));

    });

    test('decode string', function () {

      var type = new types.PrimitiveType('string');
      var buf = new Buffer([0x06, 0x68, 0x69, 0x21]);
      var s = 'hi!';
      assert.equal(type.decode(buf), s);
      assert(buf.equals(type.encode(s)));

    });

    test('encode string', function () {

      var type = new types.PrimitiveType('string');
      var buf = new Buffer([0x06, 0x68, 0x69, 0x21]);
      assert(buf.equals(type.encode('hi!', 1)));

    });

    test('as reader of', function () {
      assert.equal(getReader('long', 'int').type, 'long');
      assert.equal(getReader('double', 'long').type, 'double');
      assert.equal(getReader('bytes', 'string').type, 'bytes');
      assert.throws(function () { getReader('int', 'long'); }, AvscError);
      assert.throws(function () { getReader('long', 'double'); }, AvscError);
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

    testType(types.EnumType, data, schemas);

    test('write invalid', function () {
      var type = fromSchema({type: 'enum', symbols: ['A'], name: 'a'});
      assert.throws(function () {
        type.encode('B', {unsafe: true});
      }, AvscError);
    });

    test('as reader of', function () {
      var t1, t2;
      t1 = t('Foo', ['bar', 'baz']);
      t2 = t('Foo', ['bar', 'baz']);
      assertTypeEquals(getReader(t1, t2), t1);
      t1 = t('Foo2', ['bar', 'baz'], ['Foo']);
      assertTypeEquals(getReader(t1, t2), t1);
      t2 = t('Foo', ['bar']);
      assertTypeEquals(getReader(t1, t2), t1);
      t2 = t('Foo', ['bar', 'bax']);
      assert.throws(function () { getReader(t1, t2); }, AvscError);
      assert.throws(function () { getReader(t1, 'int'); }, AvscError);
      function t(name, symbols, aliases, namespace) {
        var obj = {type: 'enum', name: name, symbols: symbols};
        if (aliases !== undefined) {
          obj.aliases = aliases;
        }
        if (namespace !== undefined) {
          obj.namespace = namespace;
        }
        return obj;
      }
    });

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

    testType(types.FixedType, data, schemas);

    test('as reader of', function () {
      var t1, t2;
      t1 = fromSchema({type: 'fixed', size: 2, name: 'Id'});
      t2 = fromSchema({type: 'fixed', size: 2, name: 'Id'});
      assertTypeEquals(getReader(t1, t2), t1);
      t1 = fromSchema({type: 'fixed', size: 2, name: 'ID', aliases: ['Id']});
      assertTypeEquals(getReader(t1, t2), t1);
      t1 = fromSchema({type: 'fixed', size: 2, name: 'ID', aliases: ['Id'], namespace: 'a'});
      assert.throws(function () { getReader(t1, t2); }, AvscError);
      t2 = fromSchema({type: 'fixed', size: 2, name: 'Id', namespace: 'a'});
      assertTypeEquals(getReader(t1, t2), t1);
      t2 = fromSchema({type: 'fixed', size: 2, name: 'ID', namespace: 'a'});
      assertTypeEquals(getReader(t1, t2), t1);
      t2 = fromSchema({type: 'fixed', size: 3, name: 'ID', namespace: 'a'});
      assert.throws(function () { getReader(t1, t2); }, AvscError);
      assert.throws(function () { getReader(t1, 'int'); }, AvscError);
    });

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

    testType(types.MapType, data, schemas);

    test('as reader of', function () {
      var t1, t2;
      t1 = fromSchema({type: 'map', values: 'int'});
      t2 = fromSchema({type: 'map', values: 'int'});
      assertTypeEquals(getReader(t1, t2), t1);
      t1 = fromSchema({type: 'map', values: 'long'});
      assertTypeEquals(getReader(t1, t2), t1);
      t2 = fromSchema({type: 'map', values: 'string'});
      assert.throws(function () { getReader(t1, t2); }, AvscError);
      assert.throws(function () { getReader(t1, 'int'); }, AvscError);
    });

  });

  suite('ArrayType', function () {

    var data = [
      {
        name: 'int',
        schema: {items: 'int'},
        valid: [[1,3,4], []],
        invalid: [1, {o: null}, undefined, ['a'], [true]],
        check: assert.deepEqual
      }
    ];

    var schemas = [
      {},
      {items: ''},
    ];

    testType(types.ArrayType, data, schemas);

    test('as reader of', function () {
      var t1, t2;
      t1 = fromSchema({type: 'array', items: 'int'});
      t2 = fromSchema({type: 'array', items: 'int'});
      assertTypeEquals(getReader(t1, t2), t1);
      t1 = fromSchema({type: 'array', items: 'long'});
      assertTypeEquals(getReader(t1, t2), t1);
      t2 = fromSchema({type: 'array', items: 'string'});
      assert.throws(function () { getReader(t1, t2); }, AvscError);
      assert.throws(function () { getReader(t1, 'int'); }, AvscError);
    });

  });

  suite('WrappedUnionType', function () {

    var data = [
      {
        name: 'null & string',
        schema: ['null', 'string'],
        valid: [null, {string: 'hi'}],
        invalid: ['null', undefined, {string: 1}],
        check: assert.deepEqual
      },
      {
        name: 'qualified name',
        schema: ['null', {type: 'fixed', name: 'a.B', size: 2}],
        valid: [null, {'a.B': new Buffer(2)}],
        invalid: [new Buffer(2)],
        check: assert.deepEqual
      },
      {
        name: 'array int',
        schema: ['null', {type: 'array', items: 'int'}],
        valid: [null, {array: [1,3]}],
        invalid: [{array: ['a']}, [4]],
        check: assert.deepEqual
      },
      {
        name: 'null',
        schema: ['null'],
        valid: [null],
        invalid: [{array: ['a']}, [4], 'null'],
        check: assert.deepEqual
      }
    ];

    var schemas = [
      {},
      [],
      ['null', 'null'],
      ['null', {type: 'map', values: 'int'}, {type: 'map', values: 'long'}]
    ];

    testType(types.WrappedUnionType, data, schemas);

    test('instanceof Union', function () {
      var type = new types.WrappedUnionType(['null', 'int']);
      assert(type instanceof types.UnionType);
    });

    test('missing name write', function () {
      var type = new types.WrappedUnionType(['null', 'int']);
      assert.throws(function () {
        type.encode({b: 'a'}, {unsafe: true});
      }, AvscError);
    });

    test('non wrapped write', function () {
      var type = new types.WrappedUnionType(['null', 'int']);
      assert.throws(function () {
        type.encode(1, {unsafe: true});
      }, AvscError);
    });

    test('to JSON', function () {
      var type = new types.WrappedUnionType(['null', 'int']);
      assert.equal(JSON.stringify(type), '["null","int"]');
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

    var schemas = [
      [{type: 'array', items: 'int'}, {type: 'array', items: 'string'}]
    ];

    testType(types.UnwrappedUnionType, data, schemas);

    test('invalid write', function () {
      var type = new types.UnwrappedUnionType(['null', 'int']);
      assert.throws(function () {
        type.encode('a', {unsafe: true});
      }, AvscError);
    });

    test('instanceof Union', function () {
      var type = new types.UnwrappedUnionType(['null', 'int']);
      assert(type instanceof types.UnionType);
    });

    test('to JSON', function () {
      var type = new types.UnwrappedUnionType(['null', 'int']);
      assert.equal(JSON.stringify(type), '["null","int"]');
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
      {type: 'record', name: 'a', fields: {type: 'int', name: 'age'}}
    ];

    testType(types.RecordType, data, schemas);

    test('duplicate field names', function () {
      assert.throws(function () {
        fromSchema({
          type: 'record',
          name: 'Person',
          fields: [{name: 'age', type: 'int'}, {name: 'age', type: 'float'}]
        });
      }, AvscError);
    });

    test('default constructor', function () {
      var type = fromSchema({
        type: 'record',
        name: 'Person',
        fields: [{name: 'age', type: 'int', 'default': 25}]
      });
      var Person = type.getRecordConstructor();
      var p = new Person();
      assert.equal(p.age, 25);
    });

    test('default check & write', function () {
      var type = fromSchema({
        type: 'record',
        name: 'Person',
        fields: [{name: 'age', type: 'int', 'default': 25}]
      });
      assert.deepEqual(type.encode({}), new Buffer([50]));
    });

    test('fixed string default', function () {
      var s = '\x01\x04';
      var b = new Buffer(s);
      var type = fromSchema({
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
      var type = fromSchema({
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
        fromSchema({
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
      }, AvscError);
    });

    test('record isValid', function () {
      var type = fromSchema({
        type: 'record',
        name: 'Person',
        fields: [{name: 'age', type: 'int'}]
      });
      var Person = type.getRecordConstructor();
      assert((new Person(20)).$isValid());
      assert(!(new Person()).$isValid());
      assert(!(new Person('a')).$isValid());
    });

    test('record encode', function () {
      var type = fromSchema({
        type: 'record',
        name: 'Person',
        fields: [{name: 'age', type: 'int'}]
      });
      var Person = type.getRecordConstructor();
      assert.deepEqual((new Person(48)).$encode(), new Buffer([96]));
      assert.throws(function () { (new Person()).$encode(); });
      assert.doesNotThrow(function () {
        (new Person()).$encode({unsafe: true});
      });
    });

    test('Record decode', function () {
      var type = fromSchema({
        type: 'record',
        name: 'Person',
        fields: [{name: 'age', type: 'int'}]
      });
      var Person = type.getRecordConstructor();
      assert.deepEqual(Person.decode(new Buffer([40])), {age: 20});
    });

    test('Record random', function () {
      var type = fromSchema({
        type: 'record',
        name: 'Person',
        fields: [{name: 'age', type: 'int'}]
      });
      var Person = type.getRecordConstructor();
      assert(type.isValid(Person.random()));
    });

  });

  suite('type names', function () {

    test('existing', function () {
      var type = fromSchema({
        type: 'record',
        name: 'Person',
        fields: [{name: 'so', type: 'Person'}]
      });
      assert.strictEqual(type, type.fields[0].type);
    });

    test('namespaced', function () {
      var type = fromSchema({
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
        fromSchema({
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
      }, AvscError);
    });

    test('missing', function () {
      assert.throws(function () {
        fromSchema({
          type: 'record',
          name: 'Person',
          fields: [{name: 'so', type: 'Friend'}]
        });
      }, AvscError);
    });

    test('redefining primitive', function () {
      assert.throws( // Unqualified.
        function () { fromSchema({type: 'fixed', name: 'int', size: 2}); },
        AvscError
      );
      assert.throws( // Qualified.
        function () {
          fromSchema({type: 'fixed', name: 'int', size: 2, namespace: 'a'});
        },
        AvscError
      );
    });

    test('aliases', function () {
      var type = fromSchema({
        type: 'record',
        name: 'Person',
        namespace: 'a',
        aliases: ['Human', 'b.Being'],
        fields: [{name: 'age', type: 'int'}]
      });
      assert.deepEqual(type.aliases, ['a.Human', 'b.Being']);
    });

  });

});

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
      assert.throws(function () { new Type(schema); }, AvscError);
    });
  });

}

function getReader(reader, writer) {

  return fromSchema(reader).asReaderOf(fromSchema(writer));

}

function assertTypeEquals(a, b) {

  // Really inefficient...
  assert.deepEqual(
    JSON.parse(JSON.stringify(a)),
    JSON.parse(JSON.stringify(b))
  );

}

function floatEquals(a, b) {

  return Math.abs((a - b) / Math.min(a, b)) < 1e-7;

}
