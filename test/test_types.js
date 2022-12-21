'use strict';

let types = require('../lib/types'),
    utils = require('../lib/utils'),
    assert = require('assert'),
    buffer = require('buffer'),
    util = require('util');


let Buffer = buffer.Buffer;

let LogicalType = types.builtins.LogicalType;
let Tap = utils.Tap;
let Type = types.Type;
let builtins = types.builtins;


suite('types', function () {

  suite('BooleanType', function () {

    let data = [
      {
        valid: [true, false],
        invalid: [null, 'hi', undefined, 1.5, 1e28, 123124123123213]
      }
    ];

    testType(builtins.BooleanType, data);

    test('to JSON', function () {
      let t = new builtins.BooleanType();
      assert.equal(t.toJSON(), 'boolean');
    });

    test('compare buffers', function () {
      let t = new builtins.BooleanType();
      let bt = t.toBuffer(true);
      let bf = t.toBuffer(false);
      assert.equal(t.compareBuffers(bt, bf), 1);
      assert.equal(t.compareBuffers(bf, bt), -1);
      assert.equal(t.compareBuffers(bt, bt), 0);
    });

    test('get name', function () {
      let t = new builtins.BooleanType();
      assert.strictEqual(t.getName(), undefined);
      assert.equal(t.getName(true), 'boolean');
    });

  });

  suite('IntType', function () {

    let data = [
      {
        valid: [1, -3, 12314, 0, 1e9],
        invalid: [null, 'hi', undefined, 1.5, 1e28, 123124123123213]
      }
    ];

    testType(builtins.IntType, data);

    test('toBuffer int', function () {

      let type = Type.forSchema('int');
      assert.equal(type.fromBuffer(utils.bufferFrom([0x80, 0x01])), 64);
      assert(utils.bufferFrom([0]).equals(type.toBuffer(0)));

    });

    test('resolve int > long', function () {
      let intType = Type.forSchema('int');
      let longType = Type.forSchema('long');
      let buf = intType.toBuffer(123);
      assert.equal(
        longType.fromBuffer(buf, longType.createResolver(intType)),
        123
      );
    });

    test('resolve int > U[null, int]', function () {
      let wt = Type.forSchema('int');
      let rt = Type.forSchema(['null', 'int']);
      let buf = wt.toBuffer(123);
      assert.deepEqual(rt.fromBuffer(buf, rt.createResolver(wt)), 123);
    });

    test('resolve int > W[null, int]', function () {
      let wt = Type.forSchema('int');
      let rt = Type.forSchema(['null', 'int'], {wrapUnions: true});
      let buf = wt.toBuffer(123);
      assert.deepEqual(
        rt.fromBuffer(buf, rt.createResolver(wt)),
        {'int': 123}
      );
    });

    test('resolve int > float', function () {
      let wt = Type.forSchema('int');
      let rt = Type.forSchema('float');
      let buf = wt.toBuffer(123);
      assert.deepEqual(rt.fromBuffer(buf, rt.createResolver(wt)), 123);
    });

    test('resolve int > double', function () {
      let wt = Type.forSchema('int');
      let rt = Type.forSchema('double');
      let n = Math.pow(2, 30) + 1;
      let buf = wt.toBuffer(n);
      assert.deepEqual(rt.fromBuffer(buf, rt.createResolver(wt)), n);
    });

    test('toString', function () {
      assert.equal(Type.forSchema('int').toString(), '"int"');
    });

    test('clone', function () {
      let t = Type.forSchema('int');
      assert.equal(t.clone(123), 123);
      assert.equal(t.clone(123, {}), 123);
      assert.throws(function () { t.clone(''); });
    });

    test('resolve invalid', function () {
      assert.throws(function () { getResolver('int', 'long'); });
    });

  });

  suite('LongType', function () {

    let data = [
      {
        valid: [1, -3, 12314, 9007199254740990, 900719925474090],
        invalid: [null, 'hi', undefined, 9007199254740991, 1.3, 1e67]
      }
    ];

    testType(builtins.LongType, data);

    test('resolve invalid', function () {
      assert.throws(function () { getResolver('long', 'double'); });
    });

    test('resolve long > float', function () {
      let t1 = Type.forSchema('long');
      let t2 = Type.forSchema('float');
      let n = 9007199254740990; // Number.MAX_SAFE_INTEGER - 1
      let buf = t1.toBuffer(n);
      let f = t2.fromBuffer(buf, t2.createResolver(t1));
      assert(Math.abs(f - n) / n < 1e-7);
      assert(t2.isValid(f));
    });

    test('precision loss', function () {
      let type = Type.forSchema('long');
      let buf = utils.bufferFrom(
        [0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x20]
      );
      assert.throws(function () { type.fromBuffer(buf); });
    });

    test('using missing methods', function () {
      assert.throws(function () { builtins.LongType.__with(); });
    });

  });

  suite('StringType', function () {

    let data = [
      {
        valid: [
          '',
          'hi',
          'ᚠᛇᚻ᛫ᛒᛦᚦ᛫ᚠᚱᚩᚠᚢᚱ᛫ᚠᛁᚱᚪ᛫ᚷᛖᚻᚹᛦᛚᚳᚢᛗ',
          'Sîne klâwen durh die wolken sint geslagen',
          ' ვეპხის ტყაოსანი შოთა რუსთაველი ',
          '私はガラスを食べられます。それは私を傷つけません。',
          'ฉันกินกระจกได้ แต่มันไม่ทำให้ฉันเจ็บ',
          '\ud800\udfff'
        ],
        invalid: [null, undefined, 1, 0]
      }
    ];

    testType(builtins.StringType, data);

    test('fromBuffer string', function () {
      let type = Type.forSchema('string');
      let buf = utils.bufferFrom([0x06, 0x68, 0x69, 0x21]);
      let s = 'hi!';
      assert.equal(type.fromBuffer(buf), s);
      assert(buf.equals(type.toBuffer(s)));
    });

    test('toBuffer string', function () {
      let type = Type.forSchema('string');
      let buf = utils.bufferFrom([0x06, 0x68, 0x69, 0x21]);
      assert(buf.equals(type.toBuffer('hi!', 1)));
    });

    test('resolve string > bytes', function () {
      let stringT = Type.forSchema('string');
      let bytesT = Type.forSchema('bytes');
      let buf = stringT.toBuffer('\x00\x01');
      assert.deepEqual(
        bytesT.fromBuffer(buf, bytesT.createResolver(stringT)),
        utils.bufferFrom([0, 1])
      );
    });

    test('encode resize', function () {
      let t = Type.forSchema('string');
      let s = 'hello';
      let b, pos;
      b = utils.newBuffer(2);
      pos = t.encode(s, b);
      assert(pos < 0);
      b = utils.newBuffer(b.length - pos);
      pos = t.encode(s, b);
      assert(pos >= 0);
      assert.equal(s, t.fromBuffer(b)); // Also checks exact length match.
    });

  });

  suite('NullType', function () {

    let data = [
      {
        schema: 'null',
        valid: [null],
        invalid: [0, 1, 'hi', undefined]
      }
    ];

    testType(builtins.NullType, data);

    test('wrap', function () {
      let t = Type.forSchema('null');
      assert.strictEqual(t.wrap(null), null);
    });

  });

  suite('FloatType', function () {

    let data = [
      {
        valid: [1, -3, 123e7],
        invalid: [null, 'hi', undefined],
        check: function (a, b) { assert(floatEquals(a, b)); }
      }
    ];

    testType(builtins.FloatType, data);

    test('compare buffer', function () {
      let t = Type.forSchema('float');
      let b1 = t.toBuffer(0.5);
      assert.equal(t.compareBuffers(b1, b1), 0);
      let b2 = t.toBuffer(-0.75);
      assert.equal(t.compareBuffers(b1, b2), 1);
      let b3 = t.toBuffer(175);
      assert.equal(t.compareBuffers(b1, b3), -1);
    });

    test('resolver float > float', function () {
      assert.doesNotThrow(function () { getResolver('float', 'float'); });
    });

    test('resolver double > float', function () {
      assert.throws(function () { getResolver('float', 'double'); });
    });

    test('fromString', function () {
      let t = Type.forSchema('float');
      let f = t.fromString('3.1');
      assert(t.isValid(f));
    });

    test('clone from double', function () {
      let t = Type.forSchema('float');
      let d = 3.1;
      let f;
      f = t.clone(d);
      assert(t.isValid(f));
    });

  });

  suite('DoubleType', function () {

    let data = [
      {
        valid: [1, -3.4, 12314e31, 5e37],
        invalid: [null, 'hi', undefined],
        check: function (a, b) { assert(floatEquals(a, b), '' + [a, b]); }
      }
    ];

    testType(builtins.DoubleType, data);

    test('resolver string > double', function () {
      assert.throws(function () { getResolver('double', 'string'); });
    });

    test('compare buffer', function () {
      let t = Type.forSchema('double');
      let b1 = t.toBuffer(0.5);
      assert.equal(t.compareBuffers(b1, b1), 0);
      let b2 = t.toBuffer(-0.75);
      assert.equal(t.compareBuffers(b1, b2), 1);
      let b3 = t.toBuffer(175);
      assert.equal(t.compareBuffers(b1, b3), -1);
    });

  });

  suite('BytesType', function () {

    let data = [
      {
        valid: [utils.newBuffer(1), utils.bufferFrom('abc')],
        invalid: [null, 'hi', undefined, 1, 0, -3.5]
      }
    ];

    testType(builtins.BytesType, data);

    test('resolve string > bytes', function () {
      let bytesT = Type.forSchema('bytes');
      let stringT = Type.forSchema('string');
      let buf = utils.bufferFrom([4, 0, 1]);
      assert.deepEqual(
        stringT.fromBuffer(buf, stringT.createResolver(bytesT)),
        '\x00\x01'
      );
    });

    test('clone', function () {
      let t = Type.forSchema('bytes');
      let s = '\x01\x02';
      let buf = utils.bufferFrom(s);
      let clone;
      clone = t.clone(buf);
      assert.deepEqual(clone, buf);
      clone = t.clone(buf, {});
      assert.deepEqual(clone, buf);
      clone[0] = 0;
      assert.equal(buf[0], 1);
      assert.throws(function () { t.clone(s); });
      clone = t.clone(buf.toJSON(), {coerceBuffers: true});
      assert.deepEqual(clone, buf);
      assert.throws(function () { t.clone(1, {coerceBuffers: true}); });
    });

    test('fromString', function () {
      let t = Type.forSchema('bytes');
      let s = '\x01\x02';
      let buf = utils.bufferFrom(s);
      let clone;
      clone = t.fromString(JSON.stringify(s));
      assert.deepEqual(clone, buf);
      clone = t.fromString(JSON.stringify(s), {});
      assert.deepEqual(clone, buf);
    });

    test('compare', function () {
      let t = Type.forSchema('bytes');
      let b1 = t.toBuffer(utils.bufferFrom([0, 2]));
      assert.equal(t.compareBuffers(b1, b1), 0);
      let b2 = t.toBuffer(utils.bufferFrom([0, 2, 3]));
      assert.equal(t.compareBuffers(b1, b2), -1);
      let b3 = t.toBuffer(utils.bufferFrom([1]));
      assert.equal(t.compareBuffers(b3, b1), 1);
    });

  });

  suite('UnwrappedUnionType', function () {

    let data = [
      {
        name: 'null & string',
        schema: ['null', 'string'],
        valid: [null, 'hi'],
        invalid: [undefined, {string: 'hi'}],
        check: assert.deepEqual
      },
      {
        name: 'qualified name',
        schema: ['null', {type: 'fixed', name: 'a.B', size: 2}],
        valid: [null, utils.newBuffer(2)],
        invalid: [{'a.B': utils.newBuffer(2)}],
        check: assert.deepEqual
      },
      {
        name: 'array int',
        schema: ['int', {type: 'array', items: 'int'}],
        valid: [1, [1,3]],
        invalid: [null, 'hi', {array: [2]}],
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

    let schemas = [
      {},
      [],
      ['null', 'null'],
      ['int', 'long'],
      ['fixed', 'bytes'],
      [{name: 'Letter', type: 'enum', symbols: ['A', 'B']}, 'string'],
      ['null', {type: 'map', values: 'int'}, {type: 'map', values: 'long'}],
      ['null', ['int', 'string']]
    ];

    testType(builtins.UnwrappedUnionType, data, schemas);

    test('getTypes', function () {
      let t = new builtins.UnwrappedUnionType(['null', 'int']);
      let ts = t.getTypes();
      assert(ts[0].equals(Type.forSchema('null')));
      assert(ts[1].equals(Type.forSchema('int')));
    });

    test('getTypeName', function () {
      let t = new builtins.UnwrappedUnionType(['null', 'int']);
      assert.strictEqual(t.getName(), undefined);
      assert.strictEqual(t.getName(true), undefined);
      assert.equal(t.typeName, 'union:unwrapped');
    });

    test('invalid read', function () {
      let type = new builtins.UnwrappedUnionType(['null', 'int']);
      assert.throws(function () { type.fromBuffer(utils.bufferFrom([4])); });
    });

    test('missing bucket write', function () {
      let type = new builtins.UnwrappedUnionType(['null', 'int']);
      assert.throws(function () { type.toBuffer('hi'); });
    });

    test('invalid bucket write', function () {
      let type = new builtins.UnwrappedUnionType(['null', 'int']);
      assert.throws(function () { type.toBuffer(2.5); });
    });

    test('fromString', function () {
      let type = new builtins.UnwrappedUnionType(['null', 'int']);
      assert.strictEqual(type.fromString('null'), null);
      assert.deepEqual(type.fromString('{"int": 48}'), 48);
      assert.throws(function () { type.fromString('48'); });
      assert.throws(function () { type.fromString('{"long": 48}'); });
    });

    test('toString', function () {
      let type = new builtins.UnwrappedUnionType(['null', 'int']);
      assert.strictEqual(type.toString(null), 'null');
      assert.deepEqual(type.toString(48), '{"int":48}');
      assert.throws(function () { type.toString(2.5); });
    });

    test('non wrapped write', function () {
      let type = new builtins.UnwrappedUnionType(['null', 'int']);
      assert.deepEqual(type.toBuffer(23), utils.bufferFrom([2, 46]));
      assert.deepEqual(type.toBuffer(null), utils.bufferFrom([0]));
    });

    test('coerce buffers', function () {
      let type = new builtins.UnwrappedUnionType(['null', 'bytes']);
      let obj = {type: 'Buffer', data: [1, 2]};
      assert.throws(function () { type.clone(obj); });
      assert.deepEqual(
        type.clone(obj, {coerceBuffers: true}),
        utils.bufferFrom([1, 2])
      );
      assert.deepEqual(type.clone(null, {coerceBuffers: true}), null);
    });

    test('wrapped write', function () {
      let type = new builtins.UnwrappedUnionType(['null', 'int']);
      assert.throws(function () { type.toBuffer({'int': 1}); });
    });

    test('to JSON', function () {
      let type = new builtins.UnwrappedUnionType(['null', 'int']);
      assert.equal(JSON.stringify(type), '["null","int"]');
      assert.equal(type.inspect(), '<UnwrappedUnionType ["null","int"]>');
    });

    test('resolve int to [string, long]', function () {
      let t1 = Type.forSchema('int');
      let t2 = new builtins.UnwrappedUnionType(['string', 'long']);
      let a = t2.createResolver(t1);
      let buf = t1.toBuffer(23);
      assert.deepEqual(t2.fromBuffer(buf, a), 23);
    });

    test('resolve null to [null, int]', function () {
      let t1 = Type.forSchema('null');
      let t2 = new builtins.UnwrappedUnionType(['null', 'int']);
      let a = t2.createResolver(t1);
      assert.deepEqual(t2.fromBuffer(utils.newBuffer(0), a), null);
    });

    test('resolve [string, int] to unwrapped [float, bytes]', function () {
      let t1 = new builtins.WrappedUnionType(['string', 'int']);
      let t2 = new builtins.UnwrappedUnionType(['float', 'bytes']);
      let a = t2.createResolver(t1);
      let buf;
      buf = t1.toBuffer({string: 'hi'});
      assert.deepEqual(t2.fromBuffer(buf, a), utils.bufferFrom('hi'));
      buf = t1.toBuffer({'int': 1});
      assert.deepEqual(t2.fromBuffer(buf, a), 1);
    });

    test('clone', function () {
      let t = new builtins.UnwrappedUnionType(
        ['null', {type: 'map', values: 'int'}]
      );
      let o = {'int': 1};
      assert.strictEqual(t.clone(null), null);
      let c;
      c = t.clone(o);
      assert.deepEqual(c, o);
      c = t.clone(o, {});
      assert.deepEqual(c, o);
      c.int = 2;
      assert.equal(o.int, 1);
      assert.throws(function () { t.clone([]); });
      assert.throws(function () { t.clone([], {}); });
      assert.throws(function () { t.clone(undefined); });
    });

    test('invalid null', function () {
      let t = new builtins.UnwrappedUnionType(['string', 'int']);
      assert.throws(function () { t.fromString(null); }, /invalid/);
    });

    test('invalid multiple keys', function () {
      let t = new builtins.UnwrappedUnionType(['null', 'int']);
      let o = {'int': 2};
      assert.equal(t.fromString(JSON.stringify(o)), 2);
      o.foo = 3;
      assert.throws(function () { t.fromString(JSON.stringify(o)); });
    });

    test('clone named type', function () {
      let t = Type.forSchema({
        name: 'Person',
        type: 'record',
        fields: [
          {name: 'id1', type: {name: 'an.Id', type: 'fixed', size: 1}},
          {name: 'id2', type: ['null', 'an.Id']}
        ]
      }, {wrapUnions: false});
      let b = utils.bufferFrom([0]);
      let o = {id1: b, id2: b};
      assert.deepEqual(t.clone(o), o);
    });

    test('compare buffers', function () {
      let t = new builtins.UnwrappedUnionType(['null', 'double']);
      let b1 = t.toBuffer(null);
      assert.equal(t.compareBuffers(b1, b1), 0);
      let b2 = t.toBuffer(4);
      assert.equal(t.compareBuffers(b2, b1), 1);
      assert.equal(t.compareBuffers(b1, b2), -1);
      let b3 = t.toBuffer(6);
      assert.equal(t.compareBuffers(b3, b2), 1);
    });

    test('compare', function () {
      let t;
      t = new builtins.UnwrappedUnionType(['null', 'int']);
      assert.equal(t.compare(null, 3), -1);
      assert.equal(t.compare(null, null), 0);
      assert.throws(function () { t.compare('hi', 2); });
      assert.throws(function () { t.compare(null, 'hey'); });
    });

    test('wrap', function () {
      let t = new builtins.UnwrappedUnionType(['null', 'double']);
      assert.throws(function () { t.wrap(1.0); }, /directly/);
    });

  });

  suite('WrappedUnionType', function () {

    let data = [
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
        valid: [null, {'a.B': utils.newBuffer(2)}],
        invalid: [utils.newBuffer(2)],
        check: assert.deepEqual
      },
      {
        name: 'array int',
        schema: ['int', {type: 'array', items: 'int'}],
        valid: [{'int': 1}, {array: [1,3]}],
        invalid: [null, 2, {array: ['a']}, [4], 2],
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

    let schemas = [
      {},
      [],
      ['null', 'null'],
      ['null', {type: 'map', values: 'int'}, {type: 'map', values: 'long'}],
      ['null', ['int', 'string']]
    ];

    testType(builtins.WrappedUnionType, data, schemas);

    test('getTypes', function () {
      let t = Type.forSchema(['null', 'int']);
      let ts = t.types;
      assert(ts[0].equals(Type.forSchema('null')));
      assert(ts[1].equals(Type.forSchema('int')));
    });

    test('get branch type', function () {
      let type = new builtins.WrappedUnionType(['null', 'int']);
      let buf = type.toBuffer({'int': 48});
      let branchType = type.fromBuffer(buf).constructor.type;
      assert(branchType instanceof builtins.IntType);
    });

    test('missing name write', function () {
      let type = new builtins.WrappedUnionType(['null', 'int']);
      assert.throws(function () {
        type.toBuffer({b: 'a'});
      });
    });

    test('read invalid index', function () {
      let type = new builtins.WrappedUnionType(['null', 'int']);
      let buf = utils.bufferFrom([1, 0]);
      assert.throws(function () { type.fromBuffer(buf); });
    });

    test('non wrapped write', function () {
      let type = new builtins.WrappedUnionType(['null', 'int']);
      assert.throws(function () {
        type.toBuffer(1, true);
      }, Error);
    });

    test('to JSON', function () {
      let type = new builtins.WrappedUnionType(['null', 'int']);
      assert.equal(JSON.stringify(type), '["null","int"]');
    });

    test('resolve int to [long, int]', function () {
      let t1 = Type.forSchema('int');
      let t2 = new builtins.WrappedUnionType(['long', 'int']);
      let a = t2.createResolver(t1);
      let buf = t1.toBuffer(23);
      assert.deepEqual(t2.fromBuffer(buf, a), {'long': 23});
    });

    test('resolve null to [null, int]', function () {
      let t1 = Type.forSchema('null');
      let t2 = new builtins.WrappedUnionType(['null', 'int']);
      let a = t2.createResolver(t1);
      assert.deepEqual(t2.fromBuffer(utils.newBuffer(0), a), null);
    });

    test('resolve [string, int] to [long, bytes]', function () {
      let t1 = new builtins.WrappedUnionType(['string', 'int']);
      let t2 = new builtins.WrappedUnionType(['long', 'bytes']);
      let a = t2.createResolver(t1);
      let buf;
      buf = t1.toBuffer({string: 'hi'});
      assert.deepEqual(
        t2.fromBuffer(buf, a),
        {'bytes': utils.bufferFrom('hi')}
      );
      buf = t1.toBuffer({'int': 1});
      assert.deepEqual(t2.fromBuffer(buf, a), {'long': 1});
    });

    test('resolve unwrapped [string, int] to [long, bytes]', function () {
      let t1 = new builtins.UnwrappedUnionType(['string', 'int']);
      let t2 = new builtins.WrappedUnionType(['long', 'bytes']);
      let a = t2.createResolver(t1);
      let buf;
      buf = t1.toBuffer('hi');
      assert.deepEqual(
        t2.fromBuffer(buf, a),
        {'bytes': utils.bufferFrom('hi')}
      );
      buf = t1.toBuffer(1);
      assert.deepEqual(t2.fromBuffer(buf, a), {'long': 1});
    });

    test('clone', function () {
      let t = new builtins.WrappedUnionType(['null', 'int']);
      let o = {'int': 1};
      assert.strictEqual(t.clone(null), null);
      let c;
      c = t.clone(o);
      assert.deepEqual(c, o);
      c = t.clone(o, {});
      assert.deepEqual(c, o);
      c.int = 2;
      assert.equal(o.int, 1);
      assert.throws(function () { t.clone([]); });
      assert.throws(function () { t.clone([], {}); });
      assert.throws(function () { t.clone(undefined); });
    });

    test('clone and wrap', function () {
      let t = new builtins.WrappedUnionType(['string', 'int']);
      let o;
      o = t.clone('hi', {wrapUnions: true});
      assert.deepEqual(o, {'string': 'hi'});
      o = t.clone(3, {wrapUnions: true});
      assert.deepEqual(o, {'int': 3});
      assert.throws(function () { t.clone(null, {wrapUnions: 2}); });
    });

    test('unwrap', function () {
      let t = new builtins.WrappedUnionType(['string', 'int']);
      let v = t.clone({string: 'hi'});
      assert.equal(v.unwrap(), 'hi');
    });

    test('invalid multiple keys', function () {
      let t = new builtins.WrappedUnionType(['null', 'int']);
      let o = {'int': 2};
      assert(t.isValid(o));
      o.foo = 3;
      assert(!t.isValid(o));
    });

    test('clone multiple keys', function () {
      let t = new builtins.WrappedUnionType(['null', 'int']);
      let o = {'int': 2, foo: 3};
      assert.throws(function () { t.clone(o); });
      assert.throws(function () { t.clone(o, {}); });
    });

    test('clone qualify names', function () {
      let t = Type.forSchema({
        name: 'Person',
        type: 'record',
        fields: [
          {name: 'id1', type: {name: 'an.Id', type: 'fixed', size: 1}},
          {name: 'id2', type: ['null', 'an.Id']}
        ]
      }, {wrapUnions: true});
      let b = utils.bufferFrom([0]);
      let o = {id1: b, id2: {Id: b}};
      let c = {id1: b, id2: {'an.Id': b}};
      assert.throws(function () { t.clone(o, {}); });
      assert.deepEqual(t.clone(o, {qualifyNames: true}), c);
    });

    test('clone invalid qualified names', function () {
      let t = Type.forSchema({
        name: 'Person',
        type: 'record',
        fields: [
          {name: 'id1', type: {name: 'Id', type: 'fixed', size: 1}},
          {name: 'id2', type: ['null', 'Id']}
        ]
      }, {wrapUnions: true});
      let b = utils.bufferFrom([0]);
      let o = {id1: b, id2: {'an.Id': b}};
      assert.throws(function () { t.clone(o); });
      assert.throws(function () { t.clone(o, {}); });
    });

    test('compare buffers', function () {
      let t = new builtins.WrappedUnionType(['null', 'double']);
      let b1 = t.toBuffer(null);
      assert.equal(t.compareBuffers(b1, b1), 0);
      let b2 = t.toBuffer({'double': 4});
      assert.equal(t.compareBuffers(b2, b1), 1);
      assert.equal(t.compareBuffers(b1, b2), -1);
      let b3 = t.toBuffer({'double': 6});
      assert.equal(t.compareBuffers(b3, b2), 1);
    });

    test('compare', function () {
      let t;
      t = new builtins.WrappedUnionType(['null', 'int']);
      assert.equal(t.compare(null, {'int': 3}), -1);
      assert.equal(t.compare(null, null), 0);
      t = new builtins.WrappedUnionType(['int', 'float']);
      assert.equal(t.compare({'int': 2}, {'float': 0.5}), -1);
      assert.equal(t.compare({'int': 20}, {'int': 5}), 1);
    });

    test('isValid hook', function () {
      let t = new builtins.WrappedUnionType(['null', 'int']);
      let paths = [];
      assert(t.isValid(null, {errorHook: hook}));
      assert(t.isValid({'int': 1}, {errorHook: hook}));
      assert(!paths.length);
      assert(!t.isValid({'int': 'hi'}, {errorHook: hook}));
      assert.deepEqual(paths, [['int']]);

      function hook(path) { paths.push(path); }
    });

  });

  suite('EnumType', function () {

    let data = [
      {
        name: 'single symbol',
        schema: {name: 'Foo', symbols: ['HI']},
        valid: ['HI'],
        invalid: ['HEY', null, undefined, 0]
      },
      {
        name: 'number-ish as symbol',
        schema: {name: 'Foo', symbols: ['HI', 'A0']},
        valid: ['HI', 'A0'],
        invalid: ['HEY', null, undefined, 0, 'a0']
      }
    ];

    let schemas = [
      {name: 'Foo', symbols: []},
      {name: 'Foo'},
      {name: 'G', symbols: ['0']}
    ];

    testType(builtins.EnumType, data, schemas);

    test('get full name', function () {
      let t = Type.forSchema({
        type: 'enum',
        symbols: ['A', 'B'],
        name: 'Letter',
        namespace: 'latin'
      });
      assert.equal(t.name, 'latin.Letter');
      assert.equal(t.branchName, 'latin.Letter');
    });

    test('get aliases', function () {
      let t = Type.forSchema({
        type: 'enum',
        symbols: ['A', 'B'],
        name: 'Letter',
        namespace: 'latin',
        aliases: ['Character', 'alphabet.Letter']
      });
      let aliases = t.getAliases();
      assert.deepEqual(aliases, ['latin.Character', 'alphabet.Letter']);
      aliases.push('Char');
      assert.equal(t.getAliases().length, 3);
    });

    test('get symbols', function () {
      let t = Type.forSchema({
        type: 'enum',
        symbols: ['A', 'B'],
        name: 'Letter'
      });
      let symbols = t.getSymbols();
      assert.deepEqual(symbols, ['A', 'B']);
    });

    test('duplicate symbol', function () {
      assert.throws(function () {
        Type.forSchema({type: 'enum', symbols: ['A', 'B', 'A'], name: 'B'});
      });
    });

    test('missing name', function () {
      let schema = {type: 'enum', symbols: ['A', 'B']};
      let t = Type.forSchema(schema);
      assert.strictEqual(t.getName(), undefined);
      assert.strictEqual(t.getName(true), 'enum');
      assert.throws(function () {
        Type.forSchema(schema, {noAnonymousTypes: true});
      });
    });

    test('write invalid', function () {
      let type = Type.forSchema({type: 'enum', symbols: ['A'], name: 'a'});
      assert.throws(function () {
        type.toBuffer('B');
      });
    });

    test('read invalid index', function () {
      let type = new builtins.EnumType({
        type: 'enum', symbols: ['A'], name: 'a'
      });
      let buf = utils.bufferFrom([2]);
      assert.throws(function () { type.fromBuffer(buf); });
    });

    test('resolve', function () {
      let t1, t2, buf, resolver;
      t1 = newEnum('Foo', ['bar', 'baz']);
      t2 = newEnum('Foo', ['bar', 'baz']);
      resolver = t1.createResolver(t2);
      buf = t2.toBuffer('bar');
      assert.equal(t1.fromBuffer(buf, resolver), 'bar');
      t2 = newEnum('Foo', ['baz', 'bar']);
      buf = t2.toBuffer('bar');
      resolver = t1.createResolver(t2);
      assert.notEqual(t1.fromBuffer(buf), 'bar');
      assert.equal(t1.fromBuffer(buf, resolver), 'bar');
      t1 = newEnum('Foo2', ['foo', 'baz', 'bar'], ['Foo']);
      resolver = t1.createResolver(t2);
      assert.equal(t1.fromBuffer(buf, resolver), 'bar');
      assert.throws(function () {
        t1.createResolver(newEnum('Foo2', ['bar', 'baz', 'bax']));
      });
      assert.throws(function () {
        t1.createResolver(newEnum('Foo3', ['foo', 'bar']));
      });
      assert.throws(function () {
        t1.createResolver(Type.forSchema('int'));
      });
      function newEnum(name, symbols, aliases, namespace) {
        let obj = {type: 'enum', name: name, symbols: symbols};
        if (aliases !== undefined) {
          obj.aliases = aliases;
        }
        if (namespace !== undefined) {
          obj.namespace = namespace;
        }
        return new builtins.EnumType(obj);
      }
    });

    test('resolve with default', function () {
      let wt = new builtins.EnumType({name: 'W', symbols: ['A', 'B']});
      let rt = new builtins.EnumType({
        name: 'W',
        symbols: ['D', 'A', 'C'],
        default: 'D',
      });
      let resolver = rt.createResolver(wt);
      assert.equal(rt.fromBuffer(wt.toBuffer('A'), resolver), 'A');
      assert.equal(rt.fromBuffer(wt.toBuffer('B'), resolver), 'D');
    });

    test('invalid default', function () {
      assert.throws(function () {
        new builtins.EnumType({
          name: 'W',
          symbols: ['A', 'B'],
          default: 'D',
        });
      });
    });

    test('clone', function () {
      let t = Type.forSchema({
        type: 'enum',
        name: 'Foo',
        symbols: ['bar', 'baz']
      });
      assert.equal(t.clone('bar'), 'bar');
      assert.equal(t.clone('bar', {}), 'bar');
      assert.throws(function () { t.clone('BAR'); });
      assert.throws(function () { t.clone(null); });
    });

    test('compare buffers', function () {
      let t = Type.forSchema({
        type: 'enum',
        name: 'Foo',
        symbols: ['bar', 'baz']
      });
      let b1 = t.toBuffer('bar');
      let b2 = t.toBuffer('baz');
      assert.equal(t.compareBuffers(b1, b1), 0);
      assert.equal(t.compareBuffers(b2, b1), 1);
    });

    test('compare', function () {
      let t = Type.forSchema({type: 'enum', name: 'Foo', symbols: ['b', 'a']});
      assert.equal(t.compare('b', 'a'), -1);
      assert.equal(t.compare('a', 'a'), 0);
    });

  });

  suite('FixedType', function () {

    let data = [
      {
        name: 'size 1',
        schema: {name: 'Foo', size: 2},
        valid: [utils.bufferFrom([1, 2]), utils.bufferFrom([2, 3])],
        invalid: [
          'HEY',
          null,
          undefined,
          0,
          utils.newBuffer(1),
          utils.newBuffer(3)
        ],
        check: function (a, b) { assert(a.equals(b)); }
      }
    ];

    let invalidSchemas = [
      {name: 'Foo', size: NaN},
      {name: 'Foo', size: -2},
      {name: 'Foo'},
      {}
    ];

    testType(builtins.FixedType, data, invalidSchemas);

    test('get full name', function () {
      let t = Type.forSchema({
        type: 'fixed',
        size: 2,
        name: 'Id',
        namespace: 'id'
      });
      assert.equal(t.getName(), 'id.Id');
      assert.equal(t.getName(true), 'id.Id');
    });

    test('get aliases', function () {
      let t = Type.forSchema({
        type: 'fixed',
        size: 3,
        name: 'Id'
      });
      let aliases = t.getAliases();
      assert.deepEqual(aliases, []);
      aliases.push('ID');
      assert.equal(t.getAliases().length, 1);
    });

    test('get size', function () {
      let t = Type.forSchema({type: 'fixed', size: 5, name: 'Id'});
      assert.equal(t.getSize(), 5);
    });

    test('get zero size', function () {
      let t = Type.forSchema({type: 'fixed', size: 0, name: 'Id'});
      assert.equal(t.getSize(), 0);
    });

    test('resolve', function () {
      let t1 = new builtins.FixedType({name: 'Id', size: 4});
      let t2 = new builtins.FixedType({name: 'Id', size: 4});
      assert.doesNotThrow(function () { t2.createResolver(t1); });
      t2 = new builtins.FixedType({name: 'Id2', size: 4});
      assert.throws(function () { t2.createResolver(t1); });
      t2 = new builtins.FixedType({name: 'Id2', size: 4, aliases: ['Id']});
      assert.doesNotThrow(function () { t2.createResolver(t1); });
      t2 = new builtins.FixedType({name: 'Id2', size: 5, aliases: ['Id']});
      assert.throws(function () { t2.createResolver(t1); });
    });

    test('clone', function () {
      let t = new builtins.FixedType({name: 'Id', size: 2});
      let s = '\x01\x02';
      let buf = utils.bufferFrom(s);
      let clone;
      clone = t.clone(buf);
      assert.deepEqual(clone, buf);
      clone = t.clone(buf, {});
      assert.deepEqual(clone, buf);
      clone[0] = 0;
      assert.equal(buf[0], 1);
      assert.throws(function () { t.clone(s); });
      assert.throws(function () { t.clone(s, {}); });
      clone = t.clone(buf.toJSON(), {coerceBuffers: true});
      assert.deepEqual(clone, buf);
      assert.throws(function () { t.clone(1, {coerceBuffers: true}); });
      assert.throws(function () { t.clone(utils.bufferFrom([2])); });
    });

    test('fromString', function () {
      let t = new builtins.FixedType({name: 'Id', size: 2});
      let s = '\x01\x02';
      let buf = utils.bufferFrom(s);
      let clone = t.fromString(JSON.stringify(s));
      assert.deepEqual(clone, buf);
    });

    test('compare buffers', function () {
      let t = Type.forSchema({type: 'fixed', name: 'Id', size: 2});
      let b1 = utils.bufferFrom([1, 2]);
      assert.equal(t.compareBuffers(b1, b1), 0);
      let b2 = utils.bufferFrom([2, 2]);
      assert.equal(t.compareBuffers(b1, b2), -1);
    });

  });

  suite('MapType', function () {

    let data = [
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

    let schemas = [
      {},
      {values: ''},
      {values: {type: 'array'}}
    ];

    testType(builtins.MapType, data, schemas);

    test('get values type', function () {
      let t = new builtins.MapType({type: 'map', values: 'int'});
      assert(t.getValuesType().equals(Type.forSchema('int')));
    });

    test('write int', function () {
      let t = new builtins.MapType({type: 'map', values: 'int'});
      let buf = t.toBuffer({'\x01': 3, '\x02': 4});
      assert.deepEqual(buf, utils.bufferFrom([4, 2, 1, 6, 2, 2, 8, 0]));
    });

    test('read long', function () {
      let t = new builtins.MapType({type: 'map', values: 'long'});
      let buf = utils.bufferFrom([4, 2, 1, 6, 2, 2, 8, 0]);
      assert.deepEqual(t.fromBuffer(buf), {'\x01': 3, '\x02': 4});
    });

    test('read with sizes', function () {
      let t = new builtins.MapType({type: 'map', values: 'int'});
      let buf = utils.bufferFrom([1,6,2,97,2,0]);
      assert.deepEqual(t.fromBuffer(buf), {a: 1});
    });

    test('skip', function () {
      let v1 = Type.forSchema({
        name: 'Foo',
        type: 'record',
        fields: [
          {name: 'map', type: {type: 'map', values: 'int'}},
          {name: 'val', type: 'int'}
        ]
      });
      let v2 = Type.forSchema({
        name: 'Foo',
        type: 'record',
        fields: [{name: 'val', type: 'int'}]
      });
      let b1 = utils.bufferFrom([2,2,97,2,0,6]); // Without sizes.
      let b2 = utils.bufferFrom([1,6,2,97,2,0,6]); // With sizes.
      let resolver = v2.createResolver(v1);
      assert.deepEqual(v2.fromBuffer(b1, resolver), {val: 3});
      assert.deepEqual(v2.fromBuffer(b2, resolver), {val: 3});
    });

    test('resolve int > long', function () {
      let t1 = new builtins.MapType({type: 'map', values: 'int'});
      let t2 = new builtins.MapType({type: 'map', values: 'long'});
      let resolver = t2.createResolver(t1);
      let obj = {one: 1, two: 2};
      let buf = t1.toBuffer(obj);
      assert.deepEqual(t2.fromBuffer(buf, resolver), obj);
    });

    test('resolve double > double', function () {
      let t = new builtins.MapType({type: 'map', values: 'double'});
      let resolver = t.createResolver(t);
      let obj = {one: 1, two: 2};
      let buf = t.toBuffer(obj);
      assert.deepEqual(t.fromBuffer(buf, resolver), obj);
    });

    test('resolve invalid', function () {
      let t1 = new builtins.MapType({type: 'map', values: 'int'});
      let t2 = new builtins.MapType({type: 'map', values: 'string'});
      assert.throws(function () { t2.createResolver(t1); });
      t2 = new builtins.ArrayType({type: 'array', items: 'string'});
      assert.throws(function () { t2.createResolver(t1); });
    });

    test('resolve fixed', function () {
      let t1 = Type.forSchema({
        type: 'map', values: {name: 'Id', type: 'fixed', size: 2}
      });
      let t2 = Type.forSchema({
        type: 'map', values: {
          name: 'Id2', aliases: ['Id'], type: 'fixed', size: 2
        }
      });
      let resolver = t2.createResolver(t1);
      let obj = {one: utils.bufferFrom([1, 2])};
      let buf = t1.toBuffer(obj);
      assert.deepEqual(t2.fromBuffer(buf, resolver), obj);
    });

    test('clone', function () {
      let t = new builtins.MapType({type: 'map', values: 'int'});
      let o = {one: 1, two: 2};
      let c;
      c = t.clone(o);
      assert.deepEqual(c, o);
      c = t.clone(o, {});
      assert.deepEqual(c, o);
      c.one = 3;
      assert.equal(o.one, 1);
      assert.throws(function () { t.clone(undefined); });
      assert.throws(function () { t.clone(undefined, {}); });
    });

    test('clone coerce buffers', function () {
      let t = new builtins.MapType({type: 'map', values: 'bytes'});
      let o = {one: {type: 'Buffer', data: [1]}};
      assert.throws(function () { t.clone(o, {}); });
      assert.throws(function () { t.clone(o); });
      let c = t.clone(o, {coerceBuffers: true});
      assert.deepEqual(c, {one: utils.bufferFrom([1])});
    });

    test('compare buffers', function () {
      let t = new builtins.MapType({type: 'map', values: 'bytes'});
      let b1 = t.toBuffer({});
      assert.throws(function () { t.compareBuffers(b1, b1); });
    });

    test('isValid hook', function () {
      let t = new builtins.MapType({type: 'map', values: 'int'});
      let o = {one: 1, two: 'deux', three: null, four: 4};
      let errs = {};
      assert(!t.isValid(o, {errorHook: hook}));
      assert.deepEqual(errs, {two: 'deux', three: null});

      function hook(path, obj, type) {
        assert.strictEqual(type, t.getValuesType());
        assert.equal(path.length, 1);
        errs[path[0]] = obj;
      }
    });

    test('getName', function () {
      let t = new builtins.MapType({type: 'map', values: 'int'});
      assert.strictEqual(t.getName(), undefined);
      assert.strictEqual(t.getName(true), 'map');
    });

  });

  suite('ArrayType', function () {

    let data = [
      {
        name: 'int',
        schema: {items: 'int'},
        valid: [[1,3,4], []],
        invalid: [1, {o: null}, undefined, ['a'], [true]],
        check: assert.deepEqual
      }
    ];

    let schemas = [
      {},
      {items: ''},
    ];

    testType(builtins.ArrayType, data, schemas);

    test('get items type', function () {
      let t = new builtins.ArrayType({type: 'array', items: 'int'});
      assert(t.getItemsType().equals(Type.forSchema('int')));
    });

    test('read with sizes', function () {
      let t = new builtins.ArrayType({type: 'array', items: 'int'});
      let buf = utils.bufferFrom([1,2,2,0]);
      assert.deepEqual(t.fromBuffer(buf), [1]);
    });

    test('skip', function () {
      let v1 = Type.forSchema({
        name: 'Foo',
        type: 'record',
        fields: [
          {name: 'array', type: {type: 'array', items: 'int'}},
          {name: 'val', type: 'int'}
        ]
      });
      let v2 = Type.forSchema({
        name: 'Foo',
        type: 'record',
        fields: [{name: 'val', type: 'int'}]
      });
      let b1 = utils.bufferFrom([2,2,0,6]); // Without sizes.
      let b2 = utils.bufferFrom([1,2,2,0,6]); // With sizes.
      let resolver = v2.createResolver(v1);
      assert.deepEqual(v2.fromBuffer(b1, resolver), {val: 3});
      assert.deepEqual(v2.fromBuffer(b2, resolver), {val: 3});
    });

    test('resolve string items to bytes items', function () {
      let t1 = new builtins.ArrayType({type: 'array', items: 'string'});
      let t2 = new builtins.ArrayType({type: 'array', items: 'bytes'});
      let resolver = t2.createResolver(t1);
      let obj = ['\x01\x02'];
      let buf = t1.toBuffer(obj);
      assert.deepEqual(
        t2.fromBuffer(buf, resolver),
        [utils.bufferFrom([1, 2])]
      );
    });

    test('resolve invalid', function () {
      let t1 = new builtins.ArrayType({type: 'array', items: 'string'});
      let t2 = new builtins.ArrayType({type: 'array', items: 'long'});
      assert.throws(function () { t2.createResolver(t1); });
      t2 = new builtins.MapType({type: 'map', values: 'string'});
      assert.throws(function () { t2.createResolver(t1); });
    });

    test('clone', function () {
      let t = new builtins.ArrayType({type: 'array', items: 'int'});
      let o = [1, 2];
      let c;
      c = t.clone(o);
      assert.deepEqual(c, o);
      c = t.clone(o, {});
      assert.deepEqual(c, o);
      c.one = 3;
      assert.equal(o[0], 1);
      assert.throws(function () { t.clone({}); });
      assert.throws(function () { t.clone({}, {}); });
    });

    test('clone coerce buffers', function () {
      let t = Type.forSchema({
        type: 'array',
        items: {type: 'fixed', name: 'Id', size: 2}
      });
      let o = [{type: 'Buffer', data: [1, 2]}];
      assert.throws(function () { t.clone(o); });
      assert.throws(function () { t.clone(o, {}); });
      let c = t.clone(o, {coerceBuffers: true});
      assert.deepEqual(c, [utils.bufferFrom([1, 2])]);
    });

    test('compare buffers', function () {
      let t = Type.forSchema({type: 'array', items: 'int'});
      assert.equal(t.compareBuffers(t.toBuffer([]), t.toBuffer([])), 0);
      assert.equal(t.compareBuffers(t.toBuffer([1, 2]), t.toBuffer([])), 1);
      assert.equal(t.compareBuffers(t.toBuffer([1]), t.toBuffer([1, -1])), -1);
      assert.equal(t.compareBuffers(t.toBuffer([1]), t.toBuffer([2])), -1);
      assert.equal(t.compareBuffers(t.toBuffer([1, 2]), t.toBuffer([1])), 1);
    });

    test('compare', function () {
      let t = Type.forSchema({type: 'array', items: 'int'});
      assert.equal(t.compare([], []), 0);
      assert.equal(t.compare([], [-1]), -1);
      assert.equal(t.compare([1], [1]), 0);
      assert.equal(t.compare([2], [1, 2]), 1);
    });

    test('isValid hook invalid array', function () {
      let t = Type.forSchema({type: 'array', items: 'int'});
      let hookCalled = false;
      assert(!t.isValid({}, {errorHook: hook}));
      assert(hookCalled);

      function hook(path, obj, type) {
        assert.strictEqual(type, t);
        assert.deepEqual(path, []);
        hookCalled = true;
      }
    });

    test('isValid hook invalid elems', function () {
      let t = Type.forSchema({type: 'array', items: 'int'});
      let paths = [];
      assert(!t.isValid([0, 3, 'hi', 5, 'hey'], {errorHook: hook}));
      assert.deepEqual(paths, [['2'], ['4']]);

      function hook(path, obj, type) {
        assert.strictEqual(type, t.getItemsType());
        assert.equal(typeof obj, 'string');
        paths.push(path);
      }
    });

    test('isValid hook reentrant', function () {
      let t = new builtins.ArrayType({
        items: new builtins.ArrayType({items: 'int'})
      });
      let a1 = [[1, 3], ['a', 2, 'c'], [3, 'b']];
      let a2 = [[1, 3]];
      let paths = [];
      assert(!t.isValid(a1, {errorHook: hook}));
      assert.deepEqual(paths, [['1', '0'], ['1', '2'], ['2', '1']]);

      function hook(path, any, type, val) {
        paths.push(path);
        assert.strictEqual(val, a1);
        assert(t.isValid(a2, {errorHook: hook}));
      }
    });

    test('round-trip multi-block array', function () {
      let tap = new Tap(utils.newBuffer(64));
      tap.writeInt(2);
      tap.writeString('hi');
      tap.writeString('hey');
      tap.writeInt(1);
      tap.writeString('hello');
      tap.writeInt(0);
      let t = new builtins.ArrayType({items: 'string'});
      assert.deepEqual(
        t.fromBuffer(tap.buf.slice(0, tap.pos)),
        ['hi', 'hey', 'hello']
      );
    });

  });

  suite('RecordType', function () {

    let data = [
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

    let schemas = [
      {type: 'record', name: 'a', fields: ['null', 'string']},
      {type: 'record', name: 'a', fields: [{type: ['null', 'string']}]},
      {
        type: 'record',
        name: 'a',
        fields: [{name: 'b', type: ['null', 'string'], 'default': 'a'}]
      },
      {type: 'record', name: 'a', fields: {type: 'int', name: 'age'}}
    ];

    testType(builtins.RecordType, data, schemas);

    test('duplicate field names', function () {
      assert.throws(function () {
        Type.forSchema({
          type: 'record',
          name: 'Person',
          fields: [{name: 'age', type: 'int'}, {name: 'age', type: 'float'}]
        });
      });
    });

    test('invalid name', function () {
      let schema = {
        name: 'foo-bar.Bar',
        type: 'record',
        fields: [{name: 'id', type: 'int'}]
      };
      assert.throws(function () {
        Type.forSchema(schema);
      }, /invalid name/);
    });

    test('reserved name', function () {
      let schema = {
        name: 'case',
        type: 'record',
        fields: [{name: 'id', type: 'int'}]
      };
      let Case = Type.forSchema(schema).recordConstructor;
      let c = new Case(123);
      assert.equal(c.id, 123);
    });

    test('default constructor', function () {
      let type = Type.forSchema({
        type: 'record',
        name: 'Person',
        fields: [{name: 'age', type: 'int', 'default': 25}]
      });
      let Person = type.getRecordConstructor();
      let p = new Person();
      assert.equal(p.age, 25);
      assert.strictEqual(p.constructor, Person);
    });

    test('wrap values', function () {
      let type = Type.forSchema({
        namespace: 'id',
        type: 'record',
        name: 'Id',
        fields: [{name: 'n', type: 'int'}]
      });
      let Id = type.recordConstructor;
      let id = new Id(12);
      let wrappedId = {'id.Id': id};
      assert.deepEqual(type.wrap(id), wrappedId);
      assert.deepEqual(id.wrap(), wrappedId);
    });

    test('default check & write', function () {
      let type = Type.forSchema({
        type: 'record',
        name: 'Person',
        fields: [
          {name: 'age', type: 'int', 'default': 25},
          {name: 'name', type: 'string', 'default': '\x01'}
        ]
      });
      assert.deepEqual(type.toBuffer({}), utils.bufferFrom([50, 2, 1]));
    });

    test('fixed string default', function () {
      let s = '\x01\x04';
      let b = utils.bufferFrom(s);
      let type = Type.forSchema({
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
      let obj = new (type.getRecordConstructor())();
      assert.deepEqual(obj.id, utils.bufferFrom([1, 4]));
      assert.deepEqual(type.toBuffer({}), b);
    });

    test('fixed buffer invalid default', function () {
      assert.throws(function () {
        Type.forSchema({
          type: 'record',
          name: 'Object',
          fields: [
            {
              name: 'id',
              type: {type: 'fixed', size: 2, name: 'Id'},
              'default': utils.bufferFrom([0])
            }
          ]
        });
      });
    });

    test('union invalid default', function () {
      assert.throws(function () {
        Type.forSchema({
          type: 'record',
          name: 'Person',
          fields: [{name: 'name', type: ['null', 'string'], 'default': ''}]
        });
      }, /incompatible.*first branch/);
    });

    test('record default', function () {
      let d = {street: null, zip: 123};
      let schema = {
        name: 'Person',
        type: 'record',
        fields: [
          {
            name: 'address',
            type: {
              name: 'Address',
              type: 'record',
              fields: [
                {name: 'street', type: ['null', 'string']},
                {name: 'zip', type: ['int', 'string']}
              ]
            },
            'default': d
          }
        ]
      };
      let Person, person;
      // Wrapped
      Person = Type.forSchema(schema, {wrapUnions: true})
        .getRecordConstructor();
      person = new Person();
      assert.deepEqual(person.address, {street: null, zip: {'int': 123}});
      // Unwrapped.
      Person = Type.forSchema(schema).getRecordConstructor();
      person = new Person();
      assert.deepEqual(person.address, {street: null, zip: 123});
    });

    test('record keyword field name', function () {
      let type = Type.forSchema({
        type: 'record',
        name: 'Person',
        fields: [{name: 'null', type: 'int'}]
      });
      let Person = type.getRecordConstructor();
      assert.deepEqual(new Person(2), {'null': 2});
    });

    test('record isValid', function () {
      let type = Type.forSchema({
        type: 'record',
        name: 'Person',
        fields: [{name: 'age', type: 'int'}]
      });
      let Person = type.getRecordConstructor();
      assert((new Person(20)).isValid());
      assert(!(new Person()).isValid());
      assert(!(new Person('a')).isValid());
    });

    test('record toBuffer', function () {
      let type = Type.forSchema({
        type: 'record',
        name: 'Person',
        fields: [{name: 'age', type: 'int'}]
      });
      let Person = type.getRecordConstructor();
      assert.deepEqual((new Person(48)).toBuffer(), utils.bufferFrom([96]));
      assert.throws(function () { (new Person()).toBuffer(); });
    });

    test('record compare', function () {
      let P = Type.forSchema({
        type: 'record',
        name: 'Person',
        fields: [
          {name: 'data', type: {type: 'map', values: 'int'}, order:'ignore'},
          {name: 'age', type: 'int'}
        ]
      }).getRecordConstructor();
      let p1 = new P({}, 1);
      let p2 = new P({}, 2);
      assert.equal(p1.compare(p2), -1);
      assert.equal(p2.compare(p2), 0);
      assert.equal(p2.compare(p1), 1);
    });

    test('Record type', function () {
      let type = Type.forSchema({
        type: 'record',
        name: 'Person',
        fields: [{name: 'age', type: 'int'}]
      });
      let Person = type.getRecordConstructor();
      assert.strictEqual(Person.getType(), type);
    });

    test('mutable defaults', function () {
      let Person = Type.forSchema({
        type: 'record',
        name: 'Person',
        fields: [
          {
            name: 'friends',
            type: {type: 'array', items: 'string'},
            'default': []
          }
        ]
      }).getRecordConstructor();
      let p1 = new Person(undefined);
      assert.deepEqual(p1.friends, []);
      p1.friends.push('ann');
      let p2 = new Person(undefined);
      assert.deepEqual(p2.friends, []);
    });

    test('resolve alias', function () {
      let v1 = Type.forSchema({
        type: 'record',
        name: 'Person',
        fields: [{name: 'name', type: 'string'}]
      });
      let p = v1.random();
      let buf = v1.toBuffer(p);
      let v2 = Type.forSchema({
        type: 'record',
        name: 'Human',
        aliases: ['Person'],
        fields: [{name: 'name', type: 'string'}]
      });
      let resolver = v2.createResolver(v1);
      assert.deepEqual(v2.fromBuffer(buf, resolver), p);
      let v3 = Type.forSchema({
        type: 'record',
        name: 'Human',
        fields: [{name: 'name', type: 'string'}]
      });
      assert.throws(function () { v3.createResolver(v1); });
    });

    test('resolve alias with namespace', function () {
      let v1 = Type.forSchema({
        type: 'record',
        name: 'Person',
        namespace: 'earth',
        fields: [{name: 'name', type: 'string'}]
      });
      let v2 = Type.forSchema({
        type: 'record',
        name: 'Human',
        aliases: ['Person'],
        fields: [{name: 'name', type: 'string'}]
      });
      assert.throws(function () { v2.createResolver(v1); });
      let v3 = Type.forSchema({
        type: 'record',
        name: 'Human',
        aliases: ['earth.Person'],
        fields: [{name: 'name', type: 'string'}]
      });
      assert.doesNotThrow(function () { v3.createResolver(v1); });
    });

    test('resolve skip field', function () {
      let v1 = Type.forSchema({
        type: 'record',
        name: 'Person',
        fields: [
          {name: 'age', type: 'int'},
          {name: 'name', type: 'string'}
        ]
      });
      let p = {age: 25, name: 'Ann'};
      let buf = v1.toBuffer(p);
      let v2 = Type.forSchema({
        type: 'record',
        name: 'Person',
        fields: [{name: 'name', type: 'string'}]
      });
      let resolver = v2.createResolver(v1);
      assert.deepEqual(v2.fromBuffer(buf, resolver), {name: 'Ann'});
    });

    test('resolve new field', function () {
      let v1 = Type.forSchema({
        type: 'record',
        name: 'Person',
        fields: [{name: 'name', type: 'string'}]
      });
      let p = {name: 'Ann'};
      let buf = v1.toBuffer(p);
      let v2 = Type.forSchema({
        type: 'record',
        name: 'Person',
        fields: [
          {name: 'age', type: 'int', 'default': 25},
          {name: 'name', type: 'string'}
        ]
      });
      let resolver = v2.createResolver(v1);
      assert.deepEqual(v2.fromBuffer(buf, resolver), {name: 'Ann', age: 25});
    });

    test('resolve field with javascript keyword as name', function () {
      let v1 = Type.forSchema({
        type: 'record',
        name: 'Person',
        fields: [{name: 'void', type: 'string'}]
      });
      let p = {void: 'Ann'};
      let buf = v1.toBuffer(p);
      let v2 = Type.forSchema({
        type: 'record',
        name: 'Person',
        fields: [
          {name: 'void', type: 'string'}
        ]
      });
      let resolver = v2.createResolver(v1);
      assert.deepEqual(v2.fromBuffer(buf, resolver), {void: 'Ann'});
    });

    test('resolve new field no default', function () {
      let v1 = Type.forSchema({
        type: 'record',
        name: 'Person',
        fields: [{name: 'name', type: 'string'}]
      });
      let v2 = Type.forSchema({
        type: 'record',
        name: 'Person',
        fields: [
          {name: 'age', type: 'int'},
          {name: 'name', type: 'string'}
        ]
      });
      assert.throws(function () { v2.createResolver(v1); });
    });

    test('resolve from recursive schema', function () {
      let v1 = Type.forSchema({
        type: 'record',
        name: 'Person',
        fields: [{name: 'friends', type: {type: 'array', items: 'Person'}}]
      });
      let v2 = Type.forSchema({
        type: 'record',
        name: 'Person',
        fields: [{name: 'age', type: 'int', 'default': -1}]
      });
      let resolver = v2.createResolver(v1);
      let p1 = {friends: [{friends: []}]};
      let p2 = v2.fromBuffer(v1.toBuffer(p1), resolver);
      assert.deepEqual(p2, {age: -1});
    });

    test('resolve to recursive schema', function () {
      let v1 = Type.forSchema({
        type: 'record',
        name: 'Person',
        fields: [{name: 'age', type: 'int', 'default': -1}]
      });
      let v2 = Type.forSchema({
        type: 'record',
        name: 'Person',
        fields: [
          {
            name: 'friends',
            type: {type: 'array', items: 'Person'},
            'default': []
          }
        ]
      });
      let resolver = v2.createResolver(v1);
      let p1 = {age: 25};
      let p2 = v2.fromBuffer(v1.toBuffer(p1), resolver);
      assert.deepEqual(p2, {friends: []});
    });

    test('resolve from both recursive schema', function () {
      let v1 = Type.forSchema({
        type: 'record',
        name: 'Person',
        fields: [
          {name: 'friends', type: {type: 'array', items: 'Person'}},
          {name: 'age', type: 'int'}
        ]
      });
      let v2 = Type.forSchema({
        type: 'record',
        name: 'Person',
        fields: [{name: 'friends', type: {type: 'array', items: 'Person'}}]
      });
      let resolver = v2.createResolver(v1);
      let p1 = {friends: [{age: 1, friends: []}], age: 10};
      let p2 = v2.fromBuffer(v1.toBuffer(p1), resolver);
      assert.deepEqual(p2, {friends: [{friends: []}]});
    });

    test('resolve multiple matching aliases', function () {
      let v1 = Type.forSchema({
        type: 'record',
        name: 'Person',
        fields: [
          {name: 'phone', type: 'string'},
          {name: 'number', type: 'string'}
        ]
      });
      let v2 = Type.forSchema({
        type: 'record',
        name: 'Person',
        fields: [{name: 'number', type: 'string', aliases: ['phone']}]
      });
      assert.throws(function () { v2.createResolver(v1); });
    });

    test('resolve consolidated reads same type', function () {
      let t1 = Type.forSchema({
        type: 'record',
        name: 'Person',
        fields: [
          {name: 'phone', type: 'int'},
        ]
      });
      let t2 = Type.forSchema({
        type: 'record',
        name: 'Person',
        fields: [
          {name: 'number1', type: 'int', aliases: ['phone']},
          {name: 'number2', type: 'int', aliases: ['phone']},
          {name: 'phone', type: 'int'},
        ]
      });
      let rsv = t2.createResolver(t1);
      let buf = t1.toBuffer({phone: 123});
      assert.deepEqual(
        t2.fromBuffer(buf, rsv),
        {number1: 123, number2: 123, phone: 123}
      );
    });

    test('resolve consolidated reads different types', function () {
      let t1 = Type.forSchema({
        type: 'record',
        name: 'Person',
        fields: [
          {name: 'phone', type: 'int'},
        ]
      });
      let t2 = Type.forSchema({
        type: 'record',
        name: 'Person',
        fields: [
          {name: 'phoneLong', type: 'long', aliases: ['phone']},
          {name: 'phoneDouble', type: 'double', aliases: ['phone']},
          {name: 'phone', type: 'int'},
        ]
      });
      let rsv = t2.createResolver(t1);
      let buf = t1.toBuffer({phone: 123});
      assert.deepEqual(
        t2.fromBuffer(buf, rsv),
        {phoneLong: 123, phoneDouble: 123, phone: 123}
      );
    });

    test('getName', function () {
      let t = Type.forSchema({
        type: 'record',
        name: 'Person',
        doc: 'Hi!',
        namespace: 'earth',
        aliases: ['Human'],
        fields: [
          {name: 'friends', type: {type: 'array', items: 'string'}},
          {name: 'age', aliases: ['years'], type: {type: 'int'}}
        ]
      });
      assert.strictEqual(t.getName(), 'earth.Person');
      assert.strictEqual(t.getName(true), 'earth.Person');
      assert.equal(t.typeName, 'record');
    });

    test('getSchema', function () {
      let t = Type.forSchema({
        type: 'record',
        name: 'Person',
        doc: 'Hi!',
        namespace: 'earth',
        aliases: ['Human'],
        foo: 'bar',
        fields: [
          {name: 'friends', type: {type: 'array', items: 'string'}},
          {
            name: 'age',
            aliases: ['years'],
            type: {type: 'int'},
            'default': 0,
            order: 'descending'
          }
        ]
      });
      let schemaStr = '{"name":"earth.Person","type":"record","fields":[{"name":"friends","type":{"type":"array","items":"string"}},{"name":"age","type":"int"}]}';
      assert.equal(JSON.stringify(t.getSchema()), schemaStr);
      assert.deepEqual(t.getSchema(), JSON.parse(schemaStr));
      assert.deepEqual(t.getSchema({exportAttrs: true}), {
        type: 'record',
        name: 'earth.Person',
        aliases: ['earth.Human'],
        doc: 'Hi!',
        fields: [
          {name: 'friends', type: {type: 'array', items: 'string'}},
          {
            name: 'age',
            aliases: ['years'],
            type: 'int',
            'default': 0,
            order: 'descending'
          }
        ]
      });
      assert.equal(t.getSchema({noDeref: true}), 'earth.Person');
    });

    test('getSchema recursive schema', function () {
      let t = Type.forSchema({
        type: 'record',
        name: 'Person',
        namespace: 'earth',
        fields: [
          {name: 'friends', type: {type: 'array', items: 'Person'}},
        ]
      });
      assert.equal(
        JSON.stringify(t.getSchema()),
        '{"name":"earth.Person","type":"record","fields":[{"name":"friends","type":{"type":"array","items":"earth.Person"}}]}'
      );
      assert.equal(
        JSON.stringify(t.getSchema({noDeref: true})),
        '"earth.Person"'
      );
    });

    test('fromString', function () {
      let t = Type.forSchema({
        type: 'record',
        name: 'Person',
        fields: [
          {name: 'age', type: 'int'},
          {name: 'name', type: 'string', 'default': 'UNKNOWN'}
        ]
      });
      assert.deepEqual(
        t.fromString('{"age": 23}'),
        {age: 23, name: 'UNKNOWN'}
      );
      assert.throws(function () { t.fromString('{}'); });
    });

    test('toString record', function () {
      let T = Type.forSchema({
        type: 'record',
        name: 'Person',
        fields: [{name: 'pwd', type: 'bytes'}]
      }).getRecordConstructor();
      let r = new T(utils.bufferFrom([1, 2]));
      assert.equal(r.toString(), T.getType().toString(r));
    });

    test('clone', function () {
      let t = Type.forSchema({
        type: 'record',
        name: 'Person',
        fields: [{name: 'age', type: 'int'}, {name: 'name', type: 'string'}]
      });
      let Person = t.getRecordConstructor();
      let o = {age: 25, name: 'Ann'};
      let c;
      c = t.clone(o);
      assert.deepEqual(c, o);
      c = t.clone(o, {});
      assert.deepEqual(c, o);
      assert(c instanceof Person);
      c.age = 26;
      assert.equal(o.age, 25);
      assert.deepEqual(c.clone(), c);
    });

    test('clone field default', function () {
      let t = Type.forSchema({
        type: 'record',
        name: 'Person',
        fields: [
          {name: 'id', type: 'int'},
          {name: 'name', type: 'string', 'default': 'UNKNOWN'},
          {name: 'age', type: ['null', 'int'], 'default': null},
        ]
      }, {wrapUnions: true});
      assert.deepEqual(
        t.clone({id: 1, name: 'Ann'}),
        {id: 1, name: 'Ann', age: null}
      );
      assert.deepEqual(
        t.clone({id: 1, name: 'Ann', age: {'int': 21}}),
        {id: 1, name: 'Ann', age: {'int': 21}}
      );
      assert.deepEqual(
        t.clone({id: 1, name: 'Ann', age: {'int': 21}}, {}),
        {id: 1, name: 'Ann', age: {'int': 21}}
      );
      assert.deepEqual(
        t.clone({id: 1, name: 'Ann', age: 21}, {wrapUnions: true}),
        {id: 1, name: 'Ann', age: {'int': 21}}
      );
      assert.throws(function () { t.clone({}); });
    });

    test('clone field hook', function () {
      let t = Type.forSchema({
        type: 'record',
        name: 'Person',
        fields: [{name: 'age', type: 'int'}, {name: 'name', type: 'string'}]
      });
      let o = {name: 'Ann', age: 25};
      let c = t.clone(o, {fieldHook: function (f, o, r) {
        assert.strictEqual(r, t);
        return f.type instanceof builtins.StringType ? o.toUpperCase() : o;
      }});
      assert.deepEqual(c, {name: 'ANN', age: 25});
    });

    test('clone missing fields', function () {
      let t = Type.forSchema({
        type: 'record',
        name: 'Person',
        fields: [
          {name: 'id', type: 'int'},
          {name: 'name', type: ['null', 'string']},
          {name: 'age', type: ['null', 'int'], 'default': null},
        ]
      });
      assert.throws(function () { t.clone({id: 1}); }, /invalid/);
      assert.deepEqual(
        t.clone({id: 1}, {skipMissingFields: true}),
        {id: 1, name: undefined, age: null}
      );
    });

    test('unwrapped union field default', function () {
      assert.throws(function () {
        Type.forSchema({
          type: 'record',
          name: 'Person',
          fields: [
            {name: 'name', type: ['null', 'string'], 'default': 'Bob'}
          ]
        }, {wrapUnions: false});
      });
      let schema = {
        type: 'record',
        name: 'Person',
        fields: [
          {name: 'name', type: ['string', 'null'], 'default': 'Bob'}
        ]
      };
      let t = Type.forSchema(schema, {wrapUnions: false});
      let o = {name: 'Ann'};
      assert.deepEqual(t.clone(o), o);
      assert.deepEqual(t.clone({}), {name: 'Bob'});
      assert.deepEqual(t.toString({}), '{"name":{"string":"Bob"}}');
      assert.deepEqual(t.getSchema({exportAttrs: true}), schema);
    });

    test('wrapped union field default', function () {
      assert.throws(function () {
        Type.forSchema({
          type: 'record',
          name: 'Person',
          fields: [
            {name: 'name', type: ['null', 'string'], 'default': 'Bob'}
          ]
        }, {wrapUnions: true});
      });
      let schema = {
        type: 'record',
        name: 'Person',
        fields: [
          {name: 'name', type: ['string', 'null'], 'default': 'Bob', doc: ''}
        ]
      };
      let t = Type.forSchema(schema, {wrapUnions: true});
      let o = {name: {string: 'Ann'}};
      assert.deepEqual(t.clone(o), o);
      assert.deepEqual(t.clone({}), {name: {string: 'Bob'}});
      assert.deepEqual(t.getSchema({exportAttrs: true}), schema);
    });

    test('get full name & aliases', function () {
      let t = Type.forSchema({
        type: 'record',
        name: 'Person',
        namespace: 'a',
        fields: [{name: 'age', type: 'int'}, {name: 'name', type: 'string'}]
      });
      assert.equal(t.getName(), 'a.Person');
      assert.deepEqual(t.getAliases(), []);
    });

    test('field getters', function () {
      let t = Type.forSchema({
        type: 'record',
        name: 'Person',
        namespace: 'a',
        fields: [
          {name: 'age', type: 'int'},
          {name: 'name', type: 'string', aliases: ['word'], namespace: 'b'}
        ]
      });
      assert.equal(t.getField('age').getName(), 'age');
      assert.strictEqual(t.getField('foo'), undefined);
      let fields = t.getFields();
      assert.deepEqual(fields[0].getAliases(), []);
      assert.deepEqual(fields[1].getAliases(), ['word']);
      assert.equal(fields[1].getName(), 'name'); // Namespaces are ignored.
      assert(fields[1].getType().equals(Type.forSchema('string')));
    });

    test('field order', function () {
      let t = Type.forSchema({
        type: 'record',
        name: 'Person',
        fields: [{name: 'age', type: 'int'}]
      });
      let field = t.getFields()[0];
      assert.equal(field.order, 'ascending'); // Default.
      assert.equal(field.getOrder(), 'ascending'); // Default.
    });

    test('compare buffers default order', function () {
      let t = Type.forSchema({
        type: 'record',
        name: 'Person',
        fields: [
          {name: 'age', type: 'long'},
          {name: 'name', type: 'string'},
          {name: 'weight', type: 'float'},
        ]
      });
      let b1 = t.toBuffer({age: 20, name: 'Ann', weight: 0.5});
      assert.equal(t.compareBuffers(b1, b1), 0);
      let b2 = t.toBuffer({age: 20, name: 'Bob', weight: 0});
      assert.equal(t.compareBuffers(b1, b2), -1);
      let b3 = t.toBuffer({age: 19, name: 'Carrie', weight: 0});
      assert.equal(t.compareBuffers(b1, b3), 1);
    });

    test('compare buffers custom order', function () {
      let t = Type.forSchema({
        type: 'record',
        name: 'Person',
        fields: [
          {name: 'meta', type: {type: 'map', values: 'int'}, order: 'ignore'},
          {name: 'name', type: 'string', order: 'descending'}
        ]
      });
      let b1 = t.toBuffer({meta: {}, name: 'Ann'});
      assert.equal(t.compareBuffers(b1, b1), 0);
      let b2 = t.toBuffer({meta: {foo: 1}, name: 'Bob'});
      assert.equal(t.compareBuffers(b1, b2), 1);
      let b3 = t.toBuffer({meta: {foo: 0}, name: 'Alex'});
      assert.equal(t.compareBuffers(b1, b3), -1);
    });

    test('compare buffers invalid order', function () {
      assert.throws(function () { Type.forSchema({
        type: 'record',
        name: 'Person',
        fields: [{name: 'age', type: 'int', order: 'up'}]
      }); });
    });

    test('error type', function () {
      let t = Type.forSchema({
        type: 'error',
        name: 'Ouch',
        fields: [{name: 'name', type: 'string'}]
      });
      let E = t.getRecordConstructor();
      let err = new E('MyError');
      assert(err instanceof Error);
    });

    test('error stack field not overwritten', function() {
      let t = Type.forSchema({
        type: 'error',
        name: 'Ouch',
        fields: [
          {name: 'name', type: 'string'},
          {name: 'stack', type: 'string'},
        ]
      }, {errorStackTraces: true});
      let E = t.recordConstructor;
      let err = new E('MyError', 'my amazing stack');
      assert(err instanceof Error);
      assert(err.stack === 'my amazing stack');
    });

    test('error stack trace', function() {
      let t = Type.forSchema({
        type: 'error',
        name: 'Ouch',
        fields: [
          {name: 'name', type: 'string'},
          {name: 'stack', type: 'string'},
        ]
      }, {errorStackTraces: true});
      let E = t.recordConstructor;
      let err = new E('MyError');
      assert(err instanceof Error);
      if (supportsErrorStacks()) {
        assert(typeof err.stack === 'string');
      }
    });

    test('no stack trace by default', function() {
      let t = Type.forSchema({
        type: 'error',
        name: 'Ouch',
        fields: [{name: 'name', type: 'string'}]
      });
      let E = t.recordConstructor;
      let err = new E('MyError');
      assert(err instanceof Error);
      assert(err.stack === undefined);
    });

    test('no stack when no matching field', function() {
      let t = Type.forSchema({
        type: 'error',
        name: 'Ouch',
        fields: [{name: 'name', type: 'string'}]
      }, {errorStackTraces: true});
      let E = t.recordConstructor;
      let err = new E('MyError');
      assert(err instanceof Error);
      assert(err.stack === undefined);
    });

    test('no stack when non-string stack field', function() {
      let t = Type.forSchema({
        type: 'error',
        name: 'Ouch',
        fields: [
          {name: 'name', type: 'string'},
          {name: 'stack', type: 'boolean'},
        ]
      }, {errorStackTraces: true});
      let E = t.recordConstructor;
      let err = new E('MyError');
      assert(err instanceof Error);
      assert(err.stack === undefined);
    });

    test('anonymous error type', function () {
      assert.doesNotThrow(function () { Type.forSchema({
        type: 'error',
        fields: [{name: 'name', type: 'string'}]
      }); });
    });

    test('resolve error type', function () {
      let t1 = Type.forSchema({
        type: 'error',
        name: 'Ouch',
        fields: [{name: 'name', type: 'string'}]
      });
      let t2 = Type.forSchema({
        type: 'error',
        name: 'OuchAgain',
        aliases: ['Ouch'],
        fields: [{name: 'code', type: 'int', 'default': -1}]
      });
      let res = t2.createResolver(t1);
      let err1 = t1.random();
      let err2 = t2.fromBuffer(t1.toBuffer(err1), res);
      assert.deepEqual(err2, {code: -1});
    });

    test('isValid hook', function () {
      let t = Type.forSchema({
        type: 'record',
        name: 'Person',
        fields: [
          {name: 'age', type: 'int'},
          {name: 'names', type: {type: 'array', items: 'string'}}
        ]
      });
      let hasErr = false;
      try {
        assert(!t.isValid({age: 23, names: ['ann', null]}, {errorHook: hook}));
      } catch (err) {
        hasErr = true;
      }
      assert(hasErr);
      hasErr = false;
      try {
        // Again to make sure `PATH` was correctly reset.
        assert(!t.isValid({age: 23, names: ['ann', null]}, {errorHook: hook}));
      } catch (err) {
        hasErr = true;
      }
      assert(hasErr);

      function hook(path, obj, type) {
        assert.strictEqual(type, t.getFields()[1].getType().getItemsType());
        assert.deepEqual(path, ['names', '1']);
        throw new Error();
      }
    });

    test('isValid empty record', function () {
      let t = Type.forSchema({type: 'record', name: 'Person', fields: []});
      assert(t.isValid({}));
    });

    test('isValid no undeclared fields', function () {
      let t = Type.forSchema({
        type: 'record',
        fields: [{name: 'foo', type: 'int'}]
      });
      let obj = {foo: 1, bar: 'bar'};
      assert(t.isValid(obj));
      assert(!t.isValid(obj, {noUndeclaredFields: true}));
      assert(t.isValid({foo: 23}, {noUndeclaredFields: true}));
    });

    test('qualified name namespacing', function () {
      let t = Type.forSchema({
        type: 'record',
        name: '.Foo',
        fields: [
          {name: 'id', type: {type: 'record', name: 'Bar', fields: []}}
        ]
      }, {namespace: 'bar'});
      assert.equal(t.getField('id').getType().getName(), 'Bar');
    });

    test('omit record methods', function () {
      let t = Type.forSchema({
        type: 'record',
        name: 'Foo',
        fields: [{name: 'id', type: 'string'}]
      }, {omitRecordMethods: true});
      let Foo = t.recordConstructor;
      assert.strictEqual(Foo.type, undefined);
      let v = t.clone({id: 'abc'});
      assert.strictEqual(v.toBuffer, undefined);
    });

  });

  suite('AbstractLongType', function () {

    let fastLongType = new builtins.LongType();

    suite('unpacked', function () {

      let slowLongType = builtins.LongType.__with({
        fromBuffer: function (buf) {
          let neg = buf[7] >> 7;
          if (neg) { // Negative number.
            invert(buf);
          }
          let n = buf.readInt32LE(0) + Math.pow(2, 32) * buf.readInt32LE(4);
          if (neg) {
            invert(buf);
            n = -n - 1;
          }
          return n;
        },
        toBuffer: function (n) {
          let buf = utils.newBuffer(8);
          let neg = n < 0;
          if (neg) {
            invert(buf);
            n = -n - 1;
          }
          buf.writeInt32LE(n | 0);
          let h = n / Math.pow(2, 32) | 0;
          buf.writeInt32LE(h ? h : (n >= 0 ? 0 : -1), 4);
          if (neg) {
            invert(buf);
          }
          return buf;
        },
        isValid: function (n) {
          return typeof n == 'number' && n % 1 === 0;
        },
        fromJSON: function (n) { return n; },
        toJSON: function (n) { return n; },
        compare: function (n1, n2) {
          return n1 === n2 ? 0 : (n1 < n2 ? -1 : 1);
        }
      });

      test('schema', function () {
        assert.equal(slowLongType.schema(), 'long');
      });

      test('encode', function () {
        [123, -1, 321414, 900719925474090].forEach(function (n) {
          assert.deepEqual(slowLongType.toBuffer(n), fastLongType.toBuffer(n));
        });
      });

      test('decode', function () {
        [123, -1, 321414, 900719925474090].forEach(function (n) {
          let buf = fastLongType.toBuffer(n);
          assert.deepEqual(slowLongType.fromBuffer(buf), n);
        });
      });

      test('clone', function () {
        assert.equal(slowLongType.clone(123), 123);
        assert.equal(slowLongType.clone(123, {}), 123);
        assert.equal(slowLongType.fromString('-1'), -1);
        assert.equal(slowLongType.toString(-1), '-1');
      });

      test('random', function () {
        assert(slowLongType.isValid(slowLongType.random()));
      });

      test('isValid hook', function () {
        let s = 'hi';
        let errs = [];
        assert(!slowLongType.isValid(s, {errorHook: hook}));
        assert.deepEqual(errs, [s]);
        assert.throws(function () { slowLongType.toBuffer(s); });

        function hook(path, obj, type) {
          assert.strictEqual(type, slowLongType);
          assert.equal(path.length, 0);
          errs.push(obj);
        }
      });

      test('resolve between long', function () {
        let b = fastLongType.toBuffer(123);
        let fastToSlow = slowLongType.createResolver(fastLongType);
        assert.equal(slowLongType.fromBuffer(b, fastToSlow), 123);
        let slowToFast = fastLongType.createResolver(slowLongType);
        assert.equal(fastLongType.fromBuffer(b, slowToFast), 123);
      });

      test('resolve from int', function () {
        let intType = Type.forSchema('int');
        let b = intType.toBuffer(123);
        let r = slowLongType.createResolver(intType);
        assert.equal(slowLongType.fromBuffer(b, r), 123);
      });

      test('resolve to double and float', function () {
        let b = slowLongType.toBuffer(123);
        let floatType = Type.forSchema('float');
        let doubleType = Type.forSchema('double');
        assert.equal(
          floatType.fromBuffer(b, floatType.createResolver(slowLongType)),
          123
        );
        assert.equal(
          doubleType.fromBuffer(b, doubleType.createResolver(slowLongType)),
          123
        );
      });
    });

    suite('packed', function () {

      let slowLongType = builtins.LongType.__with({
        fromBuffer: function (buf) {
          let tap = new Tap(buf);
          return tap.readLong();
        },
        toBuffer: function (n) {
          let buf = utils.newBuffer(10);
          let tap = new Tap(buf);
          tap.writeLong(n);
          return buf.slice(0, tap.pos);
        },
        fromJSON: function (n) { return n; },
        toJSON: function (n) { return n; },
        isValid: function (n) { return typeof n == 'number' && n % 1 === 0; },
        compare: function (n1, n2) {
          return n1 === n2 ? 0 : (n1 < n2 ? -1 : 1);
        }
      }, true);

      test('encode', function () {
        [123, -1, 321414, 900719925474090].forEach(function (n) {
          assert.deepEqual(slowLongType.toBuffer(n), fastLongType.toBuffer(n));
        });
      });

      test('decode', function () {
        [123, -1, 321414, 900719925474090].forEach(function (n) {
          let buf = fastLongType.toBuffer(n);
          assert.deepEqual(slowLongType.fromBuffer(buf), n);
        });
      });

      test('clone', function () {
        assert.equal(slowLongType.clone(123), 123);
        assert.equal(slowLongType.fromString('-1'), -1);
        assert.equal(slowLongType.toString(-1), '-1');
      });

      test('random', function () {
        assert(slowLongType.isValid(slowLongType.random()));
      });

      test('evolution to/from', function () {
        let t1 = Type.forSchema({
          type: 'record',
          name: 'Foo',
          fields: [{name: 'foo', type: 'long'}],
        }, {registry: {long: slowLongType}});
        let t2 = Type.forSchema({
          type: 'record',
          name: 'Foo',
          fields: [{name: 'bar', aliases: ['foo'], type: 'long'}],
        }, {registry: {long: slowLongType}});
        let rsv = t2.createResolver(t1);
        let buf = t1.toBuffer({foo: 2});
        assert.deepEqual(t2.fromBuffer(buf, rsv), {bar: 2});
      });

    });

    test('within unwrapped union', function () {
      let longType = builtins.LongType.__with({
        fromBuffer: function (buf) { return {value: buf}; },
        toBuffer: function (obj) { return obj.value; },
        fromJSON: function () { throw new Error(); },
        toJSON: function () { throw new Error(); },
        isValid: function (obj) { return obj && Buffer.isBuffer(obj.value); },
        compare: function () { throw new Error(); }
      }, true);
      let t = Type.forSchema(['null', 'long'], {registry: {'long': longType}});
      let v = {value: utils.bufferFrom([4])}; // Long encoding of 2.

      assert(t.isValid(null));
      assert(t.isValid(v));
      assert.deepEqual(t.fromBuffer(t.toBuffer(v)), v);
    });

    test('incomplete buffer', function () {
      // Check that `fromBuffer` doesn't get called.
      let slowLongType = new builtins.LongType.__with({
        fromBuffer: function () { throw new Error('no'); },
        toBuffer: null,
        fromJSON: null,
        toJSON: null,
        isValid: null,
        compare: null
      });
      let buf = fastLongType.toBuffer(12314);
      assert.deepEqual(
        slowLongType.decode(buf.slice(0, 1)),
        {value: undefined, offset: -1}
      );
    });
  });

  suite('LogicalType', function () {

    function DateType(schema, opts) {
      LogicalType.call(this, schema, opts);
      if (!types.Type.isType(this.getUnderlyingType(), 'long', 'string')) {
        throw new Error('invalid underlying date type');
      }
    }
    util.inherits(DateType, LogicalType);

    DateType.prototype._fromValue = function (val) { return new Date(val); };

    DateType.prototype._toValue = function (date) {
      if (!(date instanceof Date)) {
        return undefined;
      }
      if (this.getUnderlyingType().typeName === 'long') {
        return +date;
      } else {
        // String.
        return '' + date;
      }
    };

    DateType.prototype._resolve = function (type) {
      if (types.Type.isType(type, 'long', 'string')) {
        return this._fromValue;
      }
    };

    function AgeType(schema, opts) {
      LogicalType.call(this, schema, opts);
    }
    util.inherits(AgeType, LogicalType);

    AgeType.prototype._fromValue = function (val) { return val; };

    AgeType.prototype._toValue = function (any) {
      if (typeof any == 'number' && any >= 0) {
        return any;
      }
    };

    AgeType.prototype._resolve = function (type) {
      if (types.Type.isType(type, 'logical:age')) {
        return this._fromValue;
      }
    };

    let logicalTypes = {age: AgeType, date: DateType};

    test('valid type', function () {
      let t = Type.forSchema({
        type: 'long',
        logicalType: 'date'
      }, {logicalTypes: logicalTypes});
      assert(t instanceof DateType);
      assert(/<(Date|Logical)Type {.+}>/.test(t.inspect())); // IE.
      assert(t.getUnderlyingType() instanceof builtins.LongType);
      assert(t.isValid(t.random()));
      let d = new Date(123);
      assert.equal(t.toString(d), '123');
      assert.deepEqual(t.wrap(d), {long: d});
      assert.strictEqual(t.getName(), undefined);
      assert.equal(t.getName(true), 'long');
      assert.equal(t.typeName, 'logical:date');
      assert.deepEqual(t.fromString('123'), d);
      assert.deepEqual(t.clone(d), d);
      assert.equal(t.compare(d, d), 0);
      assert.equal(t.getSchema(), 'long');
    });

    test('invalid type', function () {
      let schema = {
        type: 'int',
        logicalType: 'date'
      };
      let t;
      t = Type.forSchema(schema); // Missing.
      assert(t instanceof builtins.IntType);
      t = Type.forSchema(schema, {logicalTypes: logicalTypes}); // Invalid.
      assert(t instanceof builtins.IntType);
      assert.throws(function () {
        Type.forSchema(schema, {
          logicalTypes: logicalTypes,
          assertLogicalTypes: true
        });
      });
    });

    test('missing type', function () {
      let t = Type.forSchema({
        type: 'long',
        logicalType: 'date'
      }, {logicalTypes: {}});
      assert(t.typeName, 'long');
    });

    test('nested types', function () {
      let schema = {
        name: 'Person',
        type: 'record',
        fields: [
          {name: 'age', type: {type: 'int', logicalType: 'age'}},
          {name: 'time', type: {type: 'long', logicalType: 'date'}}
        ]
      };
      let base = Type.forSchema(schema);
      let derived = Type.forSchema(schema, {logicalTypes: logicalTypes});
      let fields = derived.getFields();
      let ageType = fields[0].getType();
      ageType.constructor = undefined; // Mimic missing constructor name.
      assert(ageType instanceof AgeType);
      assert.equal(
        ageType.inspect(),
        '<LogicalType {"type":"int","logicalType":"age"}>'
      );
      assert(fields[1].getType() instanceof DateType);
      let date = new Date(Date.now());
      let buf = base.toBuffer({age: 12, time: +date});
      let person = derived.fromBuffer(buf);
      assert.deepEqual(person.age, 12);
      assert.deepEqual(person.time, date);

      let invalid = {age: -1, time: date};
      assert.throws(function () { derived.toBuffer(invalid); });
      let hasError = false;
      derived.isValid(invalid, {errorHook: function (path, any, type) {
        hasError = true;
        assert.deepEqual(path, ['age']);
        assert.equal(any, -1);
        assert(type instanceof AgeType);
      }});
      assert(hasError);
    });

    test('recursive', function () {
      function Person(friends) { this.friends = friends || []; }

      function PersonType(schema, opts) {
        LogicalType.call(this, schema, opts);
      }
      util.inherits(PersonType, LogicalType);

      PersonType.prototype._fromValue = function (val) {
        return new Person(val.friends);
      };

      PersonType.prototype._toValue = function (val) { return val; };

      let schema = {
        type: 'record',
        name: 'Person',
        logicalType: 'person',
        fields: [
          {name: 'friends', type: {type: 'array', items: 'Person'}},
        ]
      };
      let t = Type.forSchema(schema, {logicalTypes: {'person': PersonType}});

      let p1 = new Person([new Person()]);
      let buf = t.toBuffer(p1);
      let p2 = t.fromBuffer(buf);
      assert(p2 instanceof Person);
      assert(p2.friends[0] instanceof Person);
      assert.deepEqual(p2, p1);
      assert.deepEqual(t.getSchema({exportAttrs: true}), schema);
    });

    test('recursive dereferencing name', function () {
      function BoxType(schema, opts) {
        LogicalType.call(this, schema, opts);
      }
      util.inherits(BoxType, LogicalType);

      BoxType.prototype._fromValue = function (val) { return val.unboxed; };

      BoxType.prototype._toValue = function (any) { return {unboxed: any}; };

      let t = Type.forSchema({
        name: 'BoxedMap',
        type: 'record',
        logicalType: 'box',
        fields: [
          {
            name: 'unboxed',
            type: {type: 'map', values: ['string', 'BoxedMap']}
          }
        ]
      }, {logicalTypes: {box: BoxType}});

      let v = {foo: 'hi', bar: {baz: {}}};
      assert(t.isValid({}));
      assert(t.isValid(v));
      assert.deepEqual(t.fromBuffer(t.toBuffer(v)), v);
    });

    test('resolve underlying > logical', function () {
      let t1 = Type.forSchema({type: 'string'});
      let t2 = Type.forSchema({
        type: 'long',
        logicalType: 'date'
      }, {logicalTypes: logicalTypes});

      let d1 = new Date(Date.now());
      let buf = t1.toBuffer('' + d1);
      let res = t2.createResolver(t1);
      assert.throws(function () { t2.createResolver(Type.forSchema('float')); });
      let d2 = t2.fromBuffer(buf, res);
      assert.deepEqual('' + d2, '' + d1); // Rounding error on date objects.
    });

    test('resolve logical > underlying', function () {
      let t1 = Type.forSchema({
        type: 'long',
        logicalType: 'date'
      }, {logicalTypes: logicalTypes});
      let t2 = Type.forSchema({type: 'double'}); // Note long > double too.

      let d = new Date(Date.now());
      let buf = t1.toBuffer(d);
      let res = t2.createResolver(t1);
      assert.throws(function () { Type.forSchema('int').createResolver(t1); });
      assert.equal(t2.fromBuffer(buf, res), +d);
    });

    test('resolve logical type into a schema without the field', function () {
      let t1 = Type.forSchema({
        name: 'Person',
        type: 'record',
        fields: [
          {name: 'age', type: {type: 'int', logicalType: 'age'}},
          {name: 'time', type: {type: 'long', logicalType: 'date'}}
        ]
      }, {logicalTypes: logicalTypes});
      let t2 = Type.forSchema({
        name: 'Person',
        type: 'record',
        fields: [
          {name: 'age', type: {type: 'int', logicalType: 'age'}}
        ]
      }, {logicalTypes: logicalTypes});

      let buf = t1.toBuffer({age: 12, time: new Date()});

      let res = t2.createResolver(t1);
      let decoded = t2.fromBuffer(buf, res);
      assert.equal(decoded.age, 12);
      assert.equal(decoded.time, undefined);
    });

    test('resolve union of logical > union of logical', function () {
      let t = types.Type.forSchema(
        ['null', {type: 'int', logicalType: 'age'}],
        {logicalTypes: logicalTypes, wrapUnions: true}
      );
      let resolver = t.createResolver(t);
      let v = {'int': 34};
      assert.deepEqual(t.fromBuffer(t.toBuffer(v), resolver), v);
    });

    test('even integer', function () {
      function EvenIntType(schema, opts) {
        LogicalType.call(this, schema, opts);
      }
      util.inherits(EvenIntType, LogicalType);
      EvenIntType.prototype._fromValue = function (val) {
        if (val !== (val | 0) || val % 2) {
          throw new Error('invalid');
        }
        return val;
      };
      EvenIntType.prototype._toValue = EvenIntType.prototype._fromValue;

      let opts = {logicalTypes: {'even-integer': EvenIntType}};
      let t = Type.forSchema({type: 'long', logicalType: 'even-integer'}, opts);
      assert(t.isValid(2));
      assert(!t.isValid(3));
      assert(!t.isValid('abc'));
      assert.equal(t.fromBuffer(utils.bufferFrom([4])), 2);
      assert.equal(t.clone(4), 4);
      assert.equal(t.fromString('6'), 6);
      assert.equal(t.getSchema(), 'long');
      assert.deepEqual(
        JSON.stringify(t.getSchema({exportAttrs: true})),
        '{"type":"long","logicalType":"even-integer"}'
      );
      assert(types.Type.isType(t));
      assert(!types.Type.isType(t, 'int'));
      assert(types.Type.isType(t, 'logical'));
      assert.throws(function () { t.clone(3); });
      assert.throws(function () { t.fromString('5'); });
      assert.throws(function () { t.toBuffer(3); });
      assert.throws(function () { t.fromBuffer(utils.bufferFrom([2])); });
    });

    test('inside unwrapped union', function () {
      let t = types.Type.forSchema(
        [
          'null',
          {type: 'long', logicalType: 'age'},
          {type: 'string', logicalType: 'date'}
        ],
        {logicalTypes: logicalTypes}
      );
      assert(t.isValid(new Date()));
      assert(t.isValid(34));
      assert(t.isValid(null));
      assert(!t.isValid(-123));
    });

    test('inside unwrapped union ambiguous conversion', function () {
      let t = types.Type.forSchema(
        ['long', {type: 'int', logicalType: 'age'}],
        {logicalTypes: logicalTypes}
      );
      assert(t.isValid(-34));
      assert.throws(function () { t.isValid(32); }, /ambiguous/);
    });

    test('inside unwrapped union with duplicate underlying type', function () {
      function FooType(schema, opts) {
        LogicalType.call(this, schema, opts);
      }
      util.inherits(FooType, LogicalType);
      assert.throws(function () {
        types.Type.forSchema([
          'int',
          {type: 'int', logicalType: 'foo'}
        ], {logicalTypes: {foo: FooType}, wrapUnions: false});
      }, /duplicate/);
    });

    test('inside wrapped union', function () {
      function EvenIntType(schema, opts) {
        LogicalType.call(this, schema, opts);
      }
      util.inherits(EvenIntType, LogicalType);
      EvenIntType.prototype._fromValue = function (val) {
        if (val !== (val | 0) || val % 2) {
          throw new Error('invalid');
        }
        return val;
      };
      EvenIntType.prototype._toValue = EvenIntType.prototype._fromValue;

      let t = types.Type.forSchema(
        [{type: 'int', logicalType: 'even'}],
        {logicalTypes: {even: EvenIntType}, wrapUnions: true}
      );
      assert(t.isValid({int: 2}));
      assert(!t.isValid({int: 3}));
    });

    test('of records inside wrapped union', function () {
      function PassThroughType(schema, opts) {
        LogicalType.call(this, schema, opts);
      }
      util.inherits(PassThroughType, LogicalType);
      PassThroughType.prototype._fromValue = function (val) { return val; };
      PassThroughType.prototype._toValue = PassThroughType.prototype._fromValue;

      let t = types.Type.forSchema(
        [
          {
            type: 'record',
            logicalType: 'pt',
            name: 'A',
            fields: [{name: 'a', type: 'int'}]
          },
          {
            type: 'record',
            logicalType: 'pt',
            name: 'B',
            fields: [{name: 'b', type: 'int'}]
          }
        ],
        {logicalTypes: {pt: PassThroughType}, wrapUnions: true}
      );
      assert(t.isValid({A: {a: 123}}));
      assert(t.isValid({B: {b: 456}}));
      assert(!t.isValid({B: {a: 456}}));
    });

    // Unions are slightly tricky to override with logical types since their
    // schemas aren't represented as objects.
    suite('union logical types', function () {

      let schema = [
        'null',
        {
          name: 'Person',
          type: 'record',
          fields: [
            {name: 'name', type: 'string'},
            {name: 'age', type: ['null', 'int'], 'default': null}
          ]
        }
      ];

      function createUnionTypeHook(Type) {
        let visited = [];
        return function(schema, opts) {
          if (Array.isArray(schema) && !~visited.indexOf(schema)) {
            visited.push(schema);
            return new Type(schema, opts);
          }
        };
      }

      /**
      * A generic union type which exposes its values directly.
      *
      * This implementation predates the existence of the
      * `UnwrappedUnionType` currently in the built-in types. It can still be
      * used as an example to implement custom unwrapped unions (which would
      * be able to cover ambiguous unions).
      *
      */
      function UnwrappedUnionType(schema, opts) {
        LogicalType.call(this, schema, opts);
      }
      util.inherits(UnwrappedUnionType, LogicalType);

      UnwrappedUnionType.prototype._fromValue = function (val) {
        return val === null ? null : val[Object.keys(val)[0]];
      };

      UnwrappedUnionType.prototype._toValue = function (any) {
        return this.getUnderlyingType().clone(any, {wrapUnions: true});
      };

      test('unwrapped', function () {

        let t1 = Type.forSchema(
          schema,
          {typeHook: createUnionTypeHook(UnwrappedUnionType), wrapUnions: true}
        );
        let obj = {name: 'Ann', age: 23};
        assert(t1.isValid(obj));
        let buf = t1.toBuffer(obj);
        let t2 = Type.forSchema(schema, {wrapUnions: true});
        assert.deepEqual(
          t2.fromBuffer(buf),
          {Person: {name: 'Ann', age: {'int': 23}}}
        );

      });

      test('unwrapped with nested logical types', function () {

        let schema = [
          'null',
          {
            type: 'record',
            name: 'Foo',
            fields: [
              {
                name: 'date',
                type: [
                  'null',
                  {type: 'long', logicalType: 'timestamp-millis'}
                ]
              }
            ]
          }
        ];
        let t1 = Type.forSchema(
          schema,
          {
            logicalTypes: {'timestamp-millis': DateType},
            typeHook: createUnionTypeHook(UnwrappedUnionType),
            wrapUnions: true,
          }
        );
        let obj = {date: new Date(1234)};
        assert(t1.isValid(obj));
        let buf = t1.toBuffer(obj);
        let t2 = Type.forSchema(schema, {wrapUnions: true});
        assert.deepEqual(t2.fromBuffer(buf), {Foo: {date: {long: 1234}}});
      });

      test('optional', function () {

        /**
        * A basic optional type.
        *
        * It assumes an underlying union of the form `["null", ???]`.
        *
        * Enhancements include:
        *
        * + Performing a check in the constructor on the underlying type (i.e.
        *   union with the correct form).
        * + Code-generating the conversion methods (especially a constructor
        *   for `_toValue`).
        *
        */
        function OptionalType(schema, opts) {
          LogicalType.call(this, schema, opts);
          let type = this.getUnderlyingType().getTypes()[1];
          this.name = type.getName(true);
        }
        util.inherits(OptionalType, LogicalType);

        OptionalType.prototype._fromValue = function (val) {
          return val === null ? null : val[this.name];
        };

        OptionalType.prototype._toValue = function (any) {
          if (any === null) {
            return null;
          }
          let obj = {};
          obj[this.name] = any;
          return obj;
        };

        let t1 = Type.forSchema(
          schema,
          {typeHook: createUnionTypeHook(OptionalType), wrapUnions: true}
        );
        let obj = {name: 'Ann', age: 23};
        assert(t1.isValid(obj));
        let buf = t1.toBuffer(obj);
        let t2 = Type.forSchema(schema, {wrapUnions: true});
        assert.deepEqual(
          t2.fromBuffer(buf),
          {Person: {name: 'Ann', age: {'int': 23}}}
        );

      });

    });

  });

  suite('Type.forSchema', function  () {

    test('null type', function () {
      assert.throws(function () { Type.forSchema(null); });
    });

    test('unknown types', function () {
      assert.throws(function () { Type.forSchema('a'); });
      assert.throws(function () { Type.forSchema({type: 'b'}); });
    });

    test('namespaced type', function () {
      let type = Type.forSchema({
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

    test('namespace scope', function () {
      let type = Type.forSchema({
        type: 'record',
        name: 'Human',
        namespace: 'earth',
        fields: [
          {
            name: 'id1',
            type: {type: 'fixed', name: 'Id', size: 2, namespace: 'all'}
          },
          {
            name: 'id2',
            type: {type: 'fixed', name: 'Id', size: 4}
          }
        ]
      });
      assert.equal(type.name, 'earth.Human');
      assert.equal(type.fields[0].type.name, 'all.Id');
      assert.equal(type.fields[1].type.name, 'earth.Id');
    });

    test('namespace reset', function () {
      let type = Type.forSchema({
        type: 'record',
        name: 'Human',
        namespace: 'earth',
        fields: [
          {
            name: 'id1',
            type: {type: 'fixed', name: 'Id', size: 2},
          },
          {
            name: 'id2',
            type: {type: 'fixed', name: 'Id', size: 4, namespace: ''}
          }
        ]
      });
      assert.equal(type.name, 'earth.Human');
      assert.equal(type.fields[0].type.name, 'earth.Id');
      assert.equal(type.fields[1].type.name, 'Id');
    });

    test('namespace reset with qualified name', function () {
      let type = Type.forSchema({
        type: 'record',
        name: 'earth.Human',
        namespace: '',
        fields: [{name: 'id', type: {type: 'fixed', name: 'Id', size: 2}}]
      });
      assert.equal(type.name, 'earth.Human');
      assert.equal(type.fields[0].type.name, 'Id');
    });

    test('absolute reference', function () {
      let type = Type.forSchema({
        type: 'record',
        namespace: 'earth',
        name: 'Human',
        fields: [
          {
            name: 'id1',
            type: {type: 'fixed', name: 'Id', namespace: '', size: 2},
          },
          {name: 'id2', type: '.Id'}, // Not `earth.Id`.
          {name: 'id3', type: '.string'} // Also works with primitives.
        ]
      });
      assert.equal(type.name, 'earth.Human');
      assert.equal(type.fields[1].type.name, 'Id');
    });

    test('wrapped primitive', function () {
      let type = Type.forSchema({
        type: 'record',
        name: 'Person',
        fields: [{name: 'nothing', type: {type: 'null'}}]
      });
      assert.strictEqual(type.fields[0].type.constructor, builtins.NullType);
    });

    test('fromBuffer truncated', function () {
      let type = Type.forSchema('int');
      assert.throws(function () {
        type.fromBuffer(utils.bufferFrom([128]));
      });
    });

    test('fromBuffer bad resolver', function () {
      let type = Type.forSchema('int');
      assert.throws(function () {
        type.fromBuffer(utils.bufferFrom([0]), 123, {});
      });
    });

    test('fromBuffer trailing', function () {
      let type = Type.forSchema('int');
      assert.throws(function () {
        type.fromBuffer(utils.bufferFrom([0, 2]));
      });
    });

    test('fromBuffer trailing with resolver', function () {
      let type = Type.forSchema('int');
      let resolver = type.createResolver(Type.forSchema(['int']));
      assert.equal(type.fromBuffer(utils.bufferFrom([0, 2]), resolver), 1);
    });

    test('toBuffer', function () {
      let type = Type.forSchema('int');
      assert.throws(function () { type.toBuffer('abc'); });
      assert.doesNotThrow(function () { type.toBuffer(123); });
    });

    test('toBuffer and resize', function () {
      let type = Type.forSchema('string');
      assert.deepEqual(type.toBuffer('\x01', 1), utils.bufferFrom([2, 1]));
    });

    test('type hook', function () {
      let refs = [];
      let ts = [];
      let o = {
        type: 'record',
        name: 'Human',
        fields: [
          {name: 'age', type: 'int'},
          {name: 'name', type: {type: 'string'}}
        ]
      };
      Type.forSchema(o, {typeHook: hook});
      assert.equal(ts.length, 1);
      assert.equal(ts[0].getName(), 'Human');

      function hook(schema, opts) {
        if (~refs.indexOf(schema)) {
          // Already seen this schema.
          return;
        }
        refs.push(schema);

        let type = Type.forSchema(schema, opts);
        if (type instanceof builtins.RecordType) {
          ts.push(type);
        }
        return type;
      }
    });

    test('type hook invalid return value', function () {
      assert.throws(function () {
        Type.forSchema({type: 'int'}, {typeHook: hook});
      });

      function hook() { return 'int'; }
    });

    test('type hook for aliases', function () {
      let a1 = {
        type: 'record',
        name: 'R1',
        fields: [
          {name: 'r2', type: 'R2'},
        ]
      };
      let a2 = {
        type: 'record',
        name: 'R2',
        fields: [
          {name: 'r1', type: 'R1'},
        ]
      };
      let opts = {typeHook: hook};
      Type.forSchema(a1, opts);
      assert.deepEqual(Object.keys(opts.registry), ['R1', 'R2']);

      function hook(name, opts) {
        if (name === 'R2') {
          return Type.forSchema(a2, opts);
        }
      }
    });

    test('fingerprint', function () {
      let t = Type.forSchema('int');
      let buf = utils.bufferFrom('ef524ea1b91e73173d938ade36c1db32', 'hex');
      assert.deepEqual(t.fingerprint('md5'), buf);
      assert.deepEqual(t.fingerprint(), buf);
    });

    test('getSchema default', function () {
      let type = Type.forSchema({
        type: 'record',
        name: 'Human',
        fields: [
          {name: 'id1', type: ['string', 'null'], 'default': ''},
          {name: 'id2', type: ['null', 'string'], 'default': null}
        ]
      });
      assert.deepEqual(
        type.getSchema(),
        {
          type: 'record',
          name: 'Human',
          fields: [
            {name: 'id1', type: ['string', 'null']}, // Stripped defaults.
            {name: 'id2', type: ['null', 'string']}
          ]
        }
      );
    });

    test('invalid unwrapped union default', function () {
      assert.throws(function () {
        Type.forSchema({
          name: 'Person',
          type: 'record',
          fields: [
            {name: 'id', type: ['null', 'int'], 'default': 2}
          ]
        }, {wrapUnions: false});
      }, /invalid "null"/);
    });

    test('anonymous types', function () {
      let t = Type.forSchema({
        type: 'record',
        fields: [{name: 'foo', type: 'int'}]
      });
      assert.strictEqual(t.name, undefined);
      assert.strictEqual(t.branchName, 'record');
      assert(t.isValid({foo: 3}));
      assert.throws(function () {
        Type.forSchema({name: '', type: 'record', fields: []});
      });
    });

    test('auto union wrapping', function () {
      let t = Type.forSchema({
        type: 'record',
        fields: [
          {name: 'wrapped', type: ['int', 'double' ]}, // Ambiguous.
          {name: 'unwrapped', type: ['string', 'int']} // Non-ambiguous.
        ]
      }, {wrapUnions: 'AUTO'});
      assert(Type.isType(t.field('wrapped').type, 'union:wrapped'));
      assert(Type.isType(t.field('unwrapped').type, 'union:unwrapped'));
    });

    test('invalid wrap unions option', function () {
      assert.throws(function () {
        Type.forSchema('string', {wrapUnions: 'FOO'});
      }, /invalid wrap unions option/);
      assert.throws(function () {
        Type.forSchema('string', {wrapUnions: 123});
      }, /invalid wrap unions option/);
    });

  });

  suite('fromString', function () {

    test('int', function () {
      let t = Type.forSchema('int');
      assert.equal(t.fromString('2'), 2);
      assert.throws(function () { t.fromString('"a"'); });
    });

    test('string', function () {
      let t = Type.forSchema('string');
      assert.equal(t.fromString('"2"'), '2');
      assert.throws(function () { t.fromString('a'); });
    });

    test('coerce buffers', function () {
      let t = Type.forSchema({
        name: 'Ids',
        type: 'record',
        fields: [{name: 'id1', type: {name: 'Id1', type: 'fixed', size: 2}}]
      });
      let o = {id1: utils.bufferFrom([0, 1])};
      let s = '{"id1": "\\u0000\\u0001"}';
      let c = t.fromString(s);
      assert.deepEqual(c, o);
      assert(c instanceof t.getRecordConstructor());
    });

  });

  suite('toString', function () {

    test('int', function () {
      let t = Type.forSchema('int');
      assert.equal(t.toString(2), '2');
      assert.throws(function () { t.toString('a'); });
    });

  });

  suite('resolve', function () {

    test('non type', function () {
      let t = Type.forSchema({type: 'map', values: 'int'});
      let obj = {type: 'map', values: 'int'};
      assert.throws(function () { t.createResolver(obj); });
    });

    test('union to valid wrapped union', function () {
      let t1 = Type.forSchema(['int', 'string']);
      let t2 = Type.forSchema(['null', 'string', 'long'], {wrapUnions: true});
      let resolver = t2.createResolver(t1);
      let buf = t1.toBuffer(12);
      assert.deepEqual(t2.fromBuffer(buf, resolver), {'long': 12});
    });

    test('union to invalid union', function () {
      let t1 = Type.forSchema(['int', 'string']);
      let t2 = Type.forSchema(['null', 'long']);
      assert.throws(function () { t2.createResolver(t1); });
    });

    test('wrapped union to non union', function () {
      let t1 = Type.forSchema(['int', 'long'], {wrapUnions: true});
      let t2 = Type.forSchema('long');
      let resolver = t2.createResolver(t1);
      let buf = t1.toBuffer({'int': 12});
      assert.equal(t2.fromBuffer(buf, resolver), 12);
      buf = utils.bufferFrom([4, 0]);
      assert.throws(function () { t2.fromBuffer(buf, resolver); });
    });

    test('union to non union', function () {
      let t1 = Type.forSchema(['bytes', 'string']);
      let t2 = Type.forSchema('bytes');
      let resolver = t2.createResolver(t1);
      let buf = t1.toBuffer('\x01\x02');
      assert.deepEqual(t2.fromBuffer(buf, resolver), utils.bufferFrom([1, 2]));
    });

    test('union to invalid non union', function () {
      let t1 = Type.forSchema(['int', 'long'], {wrapUnions: true});
      let t2 = Type.forSchema('int');
      assert.throws(function() { t2.createResolver(t1); });
    });

    test('anonymous types', function () {
      let t1 = Type.forSchema({type: 'fixed', size: 2});
      let t2 = Type.forSchema(
        {type: 'fixed', size: 2, namespace: 'foo', aliases: ['Id']}
      );
      let t3 = Type.forSchema({type: 'fixed', size: 2, name: 'foo.Id'});
      assert.throws(function () { t1.createResolver(t3); });
      assert.doesNotThrow(function () { t2.createResolver(t3); });
      assert.doesNotThrow(function () { t3.createResolver(t1); });
    });

    test('ignore namespaces', function () {
      let t1 = Type.forSchema({type: 'fixed', name: 'foo.Two', size: 2});
      let t2 = Type.forSchema(
        {type: 'fixed', size: 2, name: 'bar.Deux', aliases: ['bar.Two']}
      );
      assert.throws(function () { t1.createResolver(t2); });
      assert.doesNotThrow(function () {
        t2.createResolver(t1, {ignoreNamespaces: true});
      });
      let t3 = Type.forSchema({type: 'fixed', size: 2, name: 'Two'});
      assert.throws(function () { t3.createResolver(t1); });
      assert.doesNotThrow(function () {
        t3.createResolver(t1, {ignoreNamespaces: true});
      });
    });

  });

  suite('type references', function () {

    test('null', function () {
      assert.throws(function () { Type.forSchema(null); }, /did you mean/);
    });

    test('existing', function () {
      let type = Type.forSchema({
        type: 'record',
        name: 'Person',
        fields: [{name: 'so', type: 'Person'}]
      });
      assert.strictEqual(type, type.fields[0].type);
    });

    test('namespaced', function () {
      let type = Type.forSchema({
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

    test('namespaced global', function () {
      let type = Type.forSchema({
        type: 'record',
        name: '.Person',
        namespace: 'earth',
        fields: [
          {
            name: 'gender',
            type: {type: 'enum', name: 'Gender', symbols: ['F', 'M']}
          }
        ]
      });
      assert.equal(type.getName(), 'Person');
      assert.equal(type.fields[0].type.getName(), 'earth.Gender');
    });

    test('redefining', function () {
      assert.throws(function () {
        Type.forSchema({
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
      });
    });

    test('missing', function () {
      assert.throws(function () {
        Type.forSchema({
          type: 'record',
          name: 'Person',
          fields: [{name: 'so', type: 'Friend'}]
        });
      });
    });

    test('redefining primitive', function () {
      assert.throws( // Unqualified.
        function () { Type.forSchema({type: 'fixed', name: 'int', size: 2}); }
      );
      assert.throws( // Qualified.
        function () {
          Type.forSchema({type: 'fixed', name: 'int', size: 2, namespace: 'a'});
        }
      );
    });

    test('aliases', function () {
      let type = Type.forSchema({
        type: 'record',
        name: 'Person',
        namespace: 'a',
        aliases: ['Human', 'b.Being'],
        fields: [{name: 'age', type: 'int'}]
      });
      assert.deepEqual(type.aliases, ['a.Human', 'b.Being']);
    });

    test('invalid', function () {
      // Name.
      assert.throws(function () {
        Type.forSchema({type: 'fixed', name: 'ID$', size: 3});
      });
      // Namespace.
      assert.throws(function () {
        Type.forSchema({type: 'fixed', name: 'ID', size: 3, namespace: '1a'});
      });
      // Qualified.
      assert.throws(function () {
        Type.forSchema({type: 'fixed', name: 'a.2.ID', size: 3});
      });
    });

    test('anonymous types', function () {
      let t = Type.forSchema([
        {type: 'enum', symbols: ['A']},
        'int',
        {type: 'record', fields: [{name: 'foo', type: 'string'}]}
      ]);
      assert.equal(
        JSON.stringify(t.getSchema()),
        '[{"type":"enum","symbols":["A"]},"int",{"type":"record","fields":[{"name":"foo","type":"string"}]}]'
      );
    });

  });

  suite('decode', function () {

    test('long valid', function () {
      let t = Type.forSchema('long');
      let buf = utils.bufferFrom([0, 128, 2, 0]);
      let res = t.decode(buf, 1);
      assert.deepEqual(res, {value: 128, offset: 3});
    });

    test('bytes invalid', function () {
      let t = Type.forSchema('bytes');
      let buf = utils.bufferFrom([4, 1]);
      let res = t.decode(buf, 0);
      assert.deepEqual(res, {value: undefined, offset: -1});
    });

  });

  suite('encode', function () {

    test('int valid', function () {
      let t = Type.forSchema('int');
      let buf = utils.newBuffer(2);
      buf.fill(0);
      let n = t.encode(5, buf, 1);
      assert.equal(n, 2);
      assert.deepEqual(buf, utils.bufferFrom([0, 10]));
    });

    test('too short', function () {
      let t = Type.forSchema('string');
      let buf = utils.newBuffer(1);
      let n = t.encode('\x01\x02', buf, 0);
      assert.equal(n, -2);
    });

    test('invalid', function () {
      let t = Type.forSchema('float');
      let buf = utils.newBuffer(2);
      assert.throws(function () { t.encode('hi', buf, 0); });
    });

    test('invalid offset', function () {
      let t = Type.forSchema('string');
      let buf = utils.newBuffer(2);
      assert.throws(function () { t.encode('hi', buf, -1); });
    });

  });

  suite('inspect', function () {

    test('type', function () {
      assert.equal(Type.forSchema('int').inspect(), '<IntType>');
      assert.equal(
        Type.forSchema({type: 'map', values: 'string'}).inspect(),
        '<MapType {"values":"string"}>'
      );
      assert.equal(
        Type.forSchema({type: 'fixed', name: 'Id', size: 2}).inspect(),
        '<FixedType "Id">'
      );
    });

    test('resolver', function () {
      let t1 = Type.forSchema('int');
      let t2 = Type.forSchema('double');
      let resolver = t2.createResolver(t1);
      assert.equal(resolver.inspect(), '<Resolver>');
    });

  });

  test('equals', function () {
    let t1 = Type.forSchema('int');
    let t2 = Type.forSchema('int');
    assert(t1.equals(t2));
    assert(t2.equals(t1));
    assert(!t1.equals(Type.forSchema('long')));
    assert(!t1.equals(null));
  });

  test('equals strict', function () {
    let t1 = Type.forSchema({
      type: 'record',
      name: 'Foo',
      fields: [{name: 'foo', type: 'int', default: 0}],
    });
    let t2 = Type.forSchema({
      type: 'record',
      name: 'Foo',
      fields: [{name: 'foo', type: 'int', default: 1}],
    });
    assert(t1.equals(t2));
    assert(!t1.equals(t2, {strict: true}));
  });

  test('documentation', function () {
    assert.strictEqual(Type.forSchema('int').doc, undefined);
    let t1 = Type.forSchema({
      type: 'record',
      doc: 'A foo.',
      fields: [
        {name: 'bar', doc: 'Bar', type: 'int'}
      ]
    });
    assert.equal(t1.doc, 'A foo.');
    assert.equal(t1.getField('bar').doc, 'Bar');
    let t2 = Type.forSchema({type: 'int', doc: 'A foo.'});
    assert.strictEqual(t2.doc, undefined);
  });

  test('isType', function () {
    let t = Type.forSchema('int');
    assert(types.Type.isType(t));
    assert(types.Type.isType(t, 'int'));
    assert(!types.Type.isType());
    assert(!types.Type.isType('int'));
    assert(!types.Type.isType(function () {}));
  });

  test('reset', function () {
    types.Type.__reset(0);
    let t = Type.forSchema('string');
    let buf = t.toBuffer('\x01');
    assert.deepEqual(buf, utils.bufferFrom([2, 1]));
  });

  suite('forTypes', function () {

    let combine = Type.forTypes;

    test('empty', function () {
      assert.throws(function () { combine([]); });
    });

    test('numbers', function () {
      let t1 = Type.forSchema('int');
      let t2 = Type.forSchema('long');
      let t3 = Type.forSchema('float');
      let t4 = Type.forSchema('double');
      assert.strictEqual(combine([t1, t2]), t2);
      assert.strictEqual(combine([t1, t2, t3, t4]), t4);
      assert.strictEqual(combine([t3, t2]), t3);
      assert.strictEqual(combine([t2]), t2);
    });

    test('string & int', function () {
      let t1 = Type.forSchema('int');
      let t2 = Type.forSchema('string');
      assertUnionsEqual(combine([t1, t2]), Type.forSchema(['int', 'string']));
    });

    test('records & maps', function () {
      let t1 = Type.forSchema({
        type: 'record',
        fields: [{name: 'foo', type: 'int', 'default': 2}]
      });
      let t2 = Type.forSchema({type: 'map', values: 'string'});
      let t3;
      t3 = combine([t1, t2]);
      assertUnionsEqual(t3.getValuesType(), Type.forSchema(['int', 'string']));
      t3 = combine([t2, t1]);
      assertUnionsEqual(t3.getValuesType(), Type.forSchema(['int', 'string']));
    });

    test('arrays', function () {
      let t1 = Type.forSchema({type: 'array', items: 'null'});
      let t2 = Type.forSchema({type: 'array', items: 'int'});
      let t3 = combine([t1, t2]);
      assertUnionsEqual(t3.getItemsType(), Type.forSchema(['null', 'int']));
    });

    test('field single default', function () {
      let t1 = Type.forSchema({
        type: 'record',
        fields: [{name: 'foo', type: 'int', 'default': 2}]
      });
      let t2 = Type.forSchema({
        type: 'record',
        fields: []
      });
      let t3 = combine([t1, t2], {strictDefaults: true});
      assert.deepEqual(
        t3.getSchema({exportAttrs: true}),
        {
          type: 'record',
          fields: [
            {name: 'foo', type: 'int', 'default': 2}
          ]
        }
      );
    });

    test('field multiple types default', function () {
      let t1 = Type.forSchema({
        type: 'record',
        fields: [{name: 'foo', type: 'string'}]
      });
      let t2 = Type.forSchema({
        type: 'record',
        fields: [{name: 'foo', type: 'int', 'default': 2}]
      });
      let t3 = combine([t1, t2], {strictDefaults: true});
      assert.deepEqual(
        t3.getSchema({exportAttrs: true}),
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
      let t1 = Type.forSchema({
        type: 'record',
        fields: [{name: 'foo', type: 'int'}, {name: 'bar', type: 'string'}]
      });
      let t2 = Type.forSchema({
        type: 'record',
        fields: [{name: 'bar', type: 'string'}]
      });
      let t3;
      t3 = combine([t1, t2]);
      assert.deepEqual(
        t3.getSchema({exportAttrs: true}),
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
      assertUnionsEqual(t3.getValuesType(), Type.forSchema(['int', 'string']));
    });

    test('logical types', function () {
      let opts = {logicalTypes: {even: EvenType, odd: OddType}};

      function EvenType(schema, opts) { LogicalType.call(this, schema, opts); }
      util.inherits(EvenType, LogicalType);
      EvenType.prototype._fromValue = function (val) { return 2 * val; };
      EvenType.prototype._toValue = function (any) {
        if (any === (any | 0) && any % 2 === 0) {
          return any / 2;
        }
      };

      function OddType(schema, opts) { LogicalType.call(this, schema, opts); }
      util.inherits(OddType, LogicalType);
      OddType.prototype._fromValue = function (val) { return 2 * val + 1; };
      OddType.prototype._toValue = function (any) {
        if (any === (any | 0) && any % 2 === 1) {
          return any / 2;
        }
      };

      let t1 = Type.forSchema({type: 'int', logicalType: 'even'}, opts);
      let t2 = Type.forSchema({type: 'long', logicalType: 'odd'}, opts);
      assertUnionsEqual(combine([t1, t2]), Type.forSchema([t1, t2]));
      assert.throws(function () { combine([t1, t1]); });
    });

    test('invalid wrapped union', function () {
      let t1 = Type.forSchema(['int'], {wrapUnions: true});
      let t2 = Type.forSchema('string');
      assert.throws(function () { combine([t1, t2]); }, /cannot combine/);
    });

    test('error while creating wrapped union', function () {
      let opts = {typeHook: hook, wrapUnions: false};
      let t1 = Type.forSchema(['int'], {wrapUnions: true});
      let t2 = Type.forSchema(['string'], {wrapUnions: true});
      assert.throws(function () { combine([t1, t2], opts); }, /foo/);
      assert(!opts.wrapUnions);

      function hook() { throw new Error('foo'); }
    });

    test('inconsistent wrapped union', function () {
      let t1 = Type.forSchema(
        [{type: 'fixed', name: 'Id', size: 2}],
        {wrapUnions: true}
      );
      let t2 = Type.forSchema(
        [{type: 'fixed', name: 'Id', size: 3}],
        {wrapUnions: true}
      );
      assert.throws(function () { combine([t1, t2]); }, /inconsistent/);
    });

    test('valid wrapped unions', function () {
      let opts = {wrapUnions: true};
      let t1 = Type.forSchema(['int', 'string', 'null'], opts);
      let t2 = Type.forSchema(['null', 'long'], opts);
      assertUnionsEqual(
        combine([t1, t2]),
        Type.forSchema(['int', 'long', 'string', 'null'], opts)
      );
    });

    test('valid unwrapped unions', function () {
      let t1 = Type.forSchema(['int', 'string', 'null']);
      let t2 = Type.forSchema(['null', 'long']);
      assertUnionsEqual(
        combine([t1, t2]),
        Type.forSchema(['long', 'string', 'null'])
      );
    });

    test('buffers', function () {
      let t1 = Type.forSchema({type: 'fixed', size: 2});
      let t2 = Type.forSchema({type: 'fixed', size: 4});
      let t3 = Type.forSchema('bytes');
      assert.strictEqual(combine([t1, t1]), t1);
      assert.strictEqual(combine([t1, t3]), t3);
      assert(combine([t1, t2]).equals(t3));
    });

    test('strings', function () {
      let t1 = Type.forSchema({type: 'enum', symbols: ['A', 'b']});
      let t2 = Type.forSchema({type: 'enum', symbols: ['A', 'B']});
      let t3 = Type.forSchema('string');
      let symbols;
      symbols = combine([t1, t1]).getSymbols().slice();
      assert.deepEqual(symbols.sort(), ['A', 'b']);
      assert.strictEqual(combine([t1, t3]), t3);
      assert.strictEqual(combine([t1, t2, t3]), t3);
      symbols = combine([t1, t2]).getSymbols().slice();
      assert.deepEqual(symbols.sort(), ['A', 'B', 'b']);
    });

    test('strings', function () {
      let opts = {wrapUnions: true};
      let t1 = Type.forSchema(['null', 'int'], opts);
      let t2 = Type.forSchema(['null', 'long', 'string'], opts);
      let t3 = Type.forSchema(['string'], opts);
      let t4 = combine([t1, t2, t3]);
      assert.deepEqual(
        t4.getSchema(),
        ['null', 'int', 'long', 'string']
      );
    });

  });

  suite('forValue', function () {

    let infer = Type.forValue;

    test('numbers', function () {
      assert.equal(infer(1).typeName, 'int');
      assert.equal(infer(1.2).typeName, 'float');
      assert.equal(infer(9007199254740991).typeName, 'double');
    });

    test('function', function () {
      assert.throws(function () { infer(function () {}); });
    });

    test('record', function () {
      let t = infer({b: true, n: null, s: '', f: utils.newBuffer(0)});
      assert.deepEqual(
        t.getSchema(),
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
      let t1 = infer({0: [], 1: [true]});
      assert.equal(t1.getValuesType().getItemsType().typeName, 'boolean');
      let t2 = infer({0: [], 1: [true], 2: [null]});
      assertUnionsEqual(
        t2.getValuesType().getItemsType(),
        Type.forSchema(['boolean', 'null'])
      );
      let t3 = infer({0: [], 1: []});
      assert.equal(t3.getValuesType().getItemsType().typeName, 'null');
    });

    test('value hook', function () {
      let t = infer({foo: 23, bar: 'hi'}, {valueHook: hook});
      assert.equal(t.getField('foo').getType().typeName, 'long');
      assert.equal(t.getField('bar').getType().typeName, 'string');
      assert.throws(function () {
        infer({foo: function () {}}, {valueHook: hook});
      });

      function hook(val, opts) {
        if (typeof val == 'number') {
          return Type.forSchema('long', opts);
        }
        if (typeof val == 'function') {
          // This will throw an error.
          return null;
        }
      }
    });

    test('type hook array', function () {
      let i = 1;
      let t = infer([{foo: 2}, {foo: 3}], {typeHook: hook}).itemsType;
      assert.equal(t.name, 'Foo3');
      assert.equal(t.field('foo').type.typeName, 'int');

      function hook(schema) {
        if (schema.type !== 'record') {
          return;
        }
        schema.name = 'Foo' + (i++);
      }
    });

    test('type hook nested array', function () {
      let i = 1;
      let outer = infer([[{foo: 2}], [{foo: 3}]], {typeHook: hook});
      let inner = outer.itemsType.itemsType;
      assert.equal(inner.name, 'Foo3');
      assert.equal(inner.field('foo').type.typeName, 'int');

      function hook(schema) {
        if (schema.type !== 'record') {
          return;
        }
        schema.name = 'Foo' + (i++);
      }
    });

  });

});

function testType(Type, data, invalidSchemas) {

  data.forEach(function (elem) {
    test('roundtrip', function () {
      let type = new Type(elem.schema);
      elem.valid.forEach(function (v) {
        assert(type.isValid(v), '' + v);
        let fn = elem.check || assert.deepEqual;
        fn(type.fromBuffer(type.toBuffer(v)), v);
        fn(type.fromString(type.toString(v), {coerceBuffers: true}), v);
      });
      elem.invalid.forEach(function (v) {
        assert(!type.isValid(v), '' + v);
        assert.throws(function () { type.isValid(v, {errorHook: hook}); });
        assert.throws(function () { type.toBuffer(v); });

        function hook() { throw new Error(); }
      });
      let n = 50;
      while (n--) {
        // Run a few times to make sure we cover any branches.
        assert(type.isValid(type.random()));
      }
    });
  });

  test('skip', function () {
    data.forEach(function (elem) {
      let fn = elem.check || assert.deepEqual;
      let items = elem.valid;
      if (items.length > 1) {
        let type = new Type(elem.schema);
        let buf = utils.newBuffer(1024);
        let tap = new Tap(buf);
        type._write(tap, items[0]);
        type._write(tap, items[1]);
        tap.pos = 0;
        type._skip(tap);
        fn(type._read(tap), items[1]);
      }
    });
  });

  if (invalidSchemas) {
    test('invalid', function () {
      invalidSchemas.forEach(function (schema) {
        assert.throws(function () { new Type(schema); });
      });
    });
  }

}

function getResolver(reader, writer) {
  return Type.forSchema(reader).createResolver(Type.forSchema(writer));
}

function floatEquals(a, b) {
  return Math.abs((a - b) / Math.min(a, b)) < 1e-7;
}

function invert(buf) {
  let len = buf.length;
  while (len--) {
    buf[len] = ~buf[len];
  }
}

function assertUnionsEqual(t1, t2) {
  // The order of branches in combined unions is undefined, this function
  // allows a safe equality check.
  assert.equal(t1.types.length, t2.types.length);
  let b1 = utils.toMap(t1.types, function (t) { return t.branchName; });
  t2.types.forEach(function (t) {
    assert(t.equals(b1[t.branchName]));
  });
}

function supportsErrorStacks() {
  return typeof Error.captureStackTrace == 'function' || Error().stack;
}
