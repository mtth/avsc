'use strict';

let types = require('../lib/types'),
    utils = require('../lib/utils'),
    assert = require('assert'),
    buffer = require('buffer');


let Buffer = buffer.Buffer;

let LogicalType = types.builtins.LogicalType;
let Tap = utils.Tap;
let Type = types.Type;
let builtins = types.builtins;


suite('types', () => {

  suite('BooleanType', () => {

    let data = [
      {
        valid: [true, false],
        invalid: [null, 'hi', undefined, 1.5, 1e28, 123124123123213]
      }
    ];

    testType(builtins.BooleanType, data);

    test('to JSON', () => {
      let t = new builtins.BooleanType();
      assert.equal(t.toJSON(), 'boolean');
    });

    test('compare buffers', () => {
      let t = new builtins.BooleanType();
      let bt = t.toBuffer(true);
      let bf = t.toBuffer(false);
      assert.equal(t.compareBuffers(bt, bf), 1);
      assert.equal(t.compareBuffers(bf, bt), -1);
      assert.equal(t.compareBuffers(bt, bt), 0);
    });

    test('get name', () => {
      let t = new builtins.BooleanType();
      assert.strictEqual(t.getName(), undefined);
      assert.equal(t.getName(true), 'boolean');
    });

  });

  suite('IntType', () => {

    let data = [
      {
        valid: [1, -3, 12314, 0, 1e9],
        invalid: [null, 'hi', undefined, 1.5, 1e28, 123124123123213]
      }
    ];

    testType(builtins.IntType, data);

    test('toBuffer int', () => {

      let type = Type.forSchema('int');
      assert.equal(type.fromBuffer(Buffer.from([0x80, 0x01])), 64);
      assert(Buffer.from([0]).equals(type.toBuffer(0)));

    });

    test('resolve int > long', () => {
      let intType = Type.forSchema('int');
      let longType = Type.forSchema('long');
      let buf = intType.toBuffer(123);
      assert.equal(
        longType.fromBuffer(buf, longType.createResolver(intType)),
        123
      );
    });

    test('resolve int > U[null, int]', () => {
      let wt = Type.forSchema('int');
      let rt = Type.forSchema(['null', 'int']);
      let buf = wt.toBuffer(123);
      assert.deepEqual(rt.fromBuffer(buf, rt.createResolver(wt)), 123);
    });

    test('resolve int > W[null, int]', () => {
      let wt = Type.forSchema('int');
      let rt = Type.forSchema(['null', 'int'], {wrapUnions: true});
      let buf = wt.toBuffer(123);
      assert.deepEqual(
        rt.fromBuffer(buf, rt.createResolver(wt)),
        {'int': 123}
      );
    });

    test('resolve int > float', () => {
      let wt = Type.forSchema('int');
      let rt = Type.forSchema('float');
      let buf = wt.toBuffer(123);
      assert.deepEqual(rt.fromBuffer(buf, rt.createResolver(wt)), 123);
    });

    test('resolve int > double', () => {
      let wt = Type.forSchema('int');
      let rt = Type.forSchema('double');
      let n = Math.pow(2, 30) + 1;
      let buf = wt.toBuffer(n);
      assert.deepEqual(rt.fromBuffer(buf, rt.createResolver(wt)), n);
    });

    test('toString', () => {
      assert.equal(Type.forSchema('int').toString(), '"int"');
    });

    test('clone', () => {
      let t = Type.forSchema('int');
      assert.equal(t.clone(123), 123);
      assert.equal(t.clone(123, {}), 123);
      assert.throws(() => { t.clone(''); });
    });

    test('resolve invalid', () => {
      assert.throws(() => { getResolver('int', 'long'); });
    });

  });

  suite('LongType', () => {

    let data = [
      {
        valid: [1, -3, 12314, 4503599627370496],
        invalid: [null, 'hi', undefined, 9007199254740990, 1.3, 1e67]
      }
    ];

    testType(builtins.LongType, data);

    test('resolve invalid', () => {
      assert.throws(() => { getResolver('long', 'double'); });
    });

    test('resolve long > float', () => {
      let t1 = Type.forSchema('long');
      let t2 = Type.forSchema('float');
      let n = 4503599627370496; // Number.MAX_SAFE_INTEGER / 2
      let buf = t1.toBuffer(n);
      let f = t2.fromBuffer(buf, t2.createResolver(t1));
      assert(Math.abs(f - n) / n < 1e-7);
      assert(t2.isValid(f));
    });

    test('precision loss', () => {
      let type = Type.forSchema('long');
      let buf = Buffer.from(
        [0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x20]
      );
      assert.throws(() => { type.fromBuffer(buf); });
    });

    test('using missing methods', () => {
      assert.throws(() => { builtins.LongType.__with(); });
    });

  });

  suite('StringType', () => {

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

    test('fromBuffer string', () => {
      let type = Type.forSchema('string');
      let buf = Buffer.from([0x06, 0x68, 0x69, 0x21]);
      let s = 'hi!';
      assert.equal(type.fromBuffer(buf), s);
      assert(buf.equals(type.toBuffer(s)));
    });

    test('toBuffer string', () => {
      let type = Type.forSchema('string');
      let buf = Buffer.from([0x06, 0x68, 0x69, 0x21]);
      assert(buf.equals(type.toBuffer('hi!', 1)));
    });

    test('resolve string > bytes', () => {
      let stringT = Type.forSchema('string');
      let bytesT = Type.forSchema('bytes');
      let buf = stringT.toBuffer('\x00\x01');
      assert.deepEqual(
        bytesT.fromBuffer(buf, bytesT.createResolver(stringT)),
        Buffer.from([0, 1])
      );
    });

    test('encode resize', () => {
      let t = Type.forSchema('string');
      let s = 'hello';
      let b, pos;
      b = Buffer.alloc(2);
      pos = t.encode(s, b);
      assert(pos < 0);
      b = Buffer.alloc(b.length - pos);
      pos = t.encode(s, b);
      assert(pos >= 0);
      assert.equal(s, t.fromBuffer(b)); // Also checks exact length match.
    });

  });

  suite('NullType', () => {

    let data = [
      {
        schema: 'null',
        valid: [null],
        invalid: [0, 1, 'hi', undefined]
      }
    ];

    testType(builtins.NullType, data);

    test('wrap', () => {
      let t = Type.forSchema('null');
      assert.strictEqual(t.wrap(null), null);
    });

  });

  suite('FloatType', () => {

    let data = [
      {
        valid: [1, -3, 123e7],
        invalid: [null, 'hi', undefined],
        check: function (a, b) { assert(floatEquals(a, b)); }
      }
    ];

    testType(builtins.FloatType, data);

    test('compare buffer', () => {
      let t = Type.forSchema('float');
      let b1 = t.toBuffer(0.5);
      assert.equal(t.compareBuffers(b1, b1), 0);
      let b2 = t.toBuffer(-0.75);
      assert.equal(t.compareBuffers(b1, b2), 1);
      let b3 = t.toBuffer(175);
      assert.equal(t.compareBuffers(b1, b3), -1);
    });

    test('resolver float > float', () => {
      assert.doesNotThrow(() => { getResolver('float', 'float'); });
    });

    test('resolver double > float', () => {
      assert.throws(() => { getResolver('float', 'double'); });
    });

    test('fromString', () => {
      let t = Type.forSchema('float');
      let f = t.fromString('3.1');
      assert(t.isValid(f));
    });

    test('clone from double', () => {
      let t = Type.forSchema('float');
      let d = 3.1;
      let f;
      f = t.clone(d);
      assert(t.isValid(f));
    });

  });

  suite('DoubleType', () => {

    let data = [
      {
        valid: [1, -3.4, 12314e31, 5e37],
        invalid: [null, 'hi', undefined],
        check: function (a, b) { assert(floatEquals(a, b), '' + [a, b]); }
      }
    ];

    testType(builtins.DoubleType, data);

    test('resolver string > double', () => {
      assert.throws(() => { getResolver('double', 'string'); });
    });

    test('compare buffer', () => {
      let t = Type.forSchema('double');
      let b1 = t.toBuffer(0.5);
      assert.equal(t.compareBuffers(b1, b1), 0);
      let b2 = t.toBuffer(-0.75);
      assert.equal(t.compareBuffers(b1, b2), 1);
      let b3 = t.toBuffer(175);
      assert.equal(t.compareBuffers(b1, b3), -1);
    });

  });

  suite('BytesType', () => {

    let data = [
      {
        valid: [Buffer.alloc(1), Buffer.from('abc')],
        invalid: [null, 'hi', undefined, 1, 0, -3.5]
      }
    ];

    testType(builtins.BytesType, data);

    test('resolve string > bytes', () => {
      let bytesT = Type.forSchema('bytes');
      let stringT = Type.forSchema('string');
      let buf = Buffer.from([4, 0, 1]);
      assert.deepEqual(
        stringT.fromBuffer(buf, stringT.createResolver(bytesT)),
        '\x00\x01'
      );
    });

    test('clone', () => {
      let t = Type.forSchema('bytes');
      let s = '\x01\x02';
      let buf = Buffer.from(s);
      let clone;
      clone = t.clone(buf);
      assert.deepEqual(clone, buf);
      clone = t.clone(buf, {});
      assert.deepEqual(clone, buf);
      clone[0] = 0;
      assert.equal(buf[0], 1);
      assert.throws(() => { t.clone(s); });
      clone = t.clone(buf.toJSON(), {coerceBuffers: true});
      assert.deepEqual(clone, buf);
      assert.throws(() => { t.clone(1, {coerceBuffers: true}); });
    });

    test('fromString', () => {
      let t = Type.forSchema('bytes');
      let s = '\x01\x02';
      let buf = Buffer.from(s);
      let clone;
      clone = t.fromString(JSON.stringify(s));
      assert.deepEqual(clone, buf);
      clone = t.fromString(JSON.stringify(s), {});
      assert.deepEqual(clone, buf);
    });

    test('compare', () => {
      let t = Type.forSchema('bytes');
      let b1 = t.toBuffer(Buffer.from([0, 2]));
      assert.equal(t.compareBuffers(b1, b1), 0);
      let b2 = t.toBuffer(Buffer.from([0, 2, 3]));
      assert.equal(t.compareBuffers(b1, b2), -1);
      let b3 = t.toBuffer(Buffer.from([1]));
      assert.equal(t.compareBuffers(b3, b1), 1);
    });

  });

  suite('UnwrappedUnionType', () => {

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
        valid: [null, Buffer.alloc(2)],
        invalid: [{'a.B': Buffer.alloc(2)}],
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

    test('getTypes', () => {
      let t = new builtins.UnwrappedUnionType(['null', 'int']);
      let ts = t.getTypes();
      assert(ts[0].equals(Type.forSchema('null')));
      assert(ts[1].equals(Type.forSchema('int')));
    });

    test('getTypeName', () => {
      let t = new builtins.UnwrappedUnionType(['null', 'int']);
      assert.strictEqual(t.getName(), undefined);
      assert.strictEqual(t.getName(true), undefined);
      assert.equal(t.typeName, 'union:unwrapped');
    });

    test('invalid read', () => {
      let type = new builtins.UnwrappedUnionType(['null', 'int']);
      assert.throws(() => { type.fromBuffer(Buffer.from([4])); });
    });

    test('missing bucket write', () => {
      let type = new builtins.UnwrappedUnionType(['null', 'int']);
      assert.throws(() => { type.toBuffer('hi'); });
    });

    test('invalid bucket write', () => {
      let type = new builtins.UnwrappedUnionType(['null', 'int']);
      assert.throws(() => { type.toBuffer(2.5); });
    });

    test('fromString', () => {
      let type = new builtins.UnwrappedUnionType(['null', 'int']);
      assert.strictEqual(type.fromString('null'), null);
      assert.deepEqual(type.fromString('{"int": 48}'), 48);
      assert.throws(() => { type.fromString('48'); });
      assert.throws(() => { type.fromString('{"long": 48}'); });
    });

    test('toString', () => {
      let type = new builtins.UnwrappedUnionType(['null', 'int']);
      assert.strictEqual(type.toString(null), 'null');
      assert.deepEqual(type.toString(48), '{"int":48}');
      assert.throws(() => { type.toString(2.5); });
    });

    test('non wrapped write', () => {
      let type = new builtins.UnwrappedUnionType(['null', 'int']);
      assert.deepEqual(type.toBuffer(23), Buffer.from([2, 46]));
      assert.deepEqual(type.toBuffer(null), Buffer.from([0]));
    });

    test('coerce buffers', () => {
      let type = new builtins.UnwrappedUnionType(['null', 'bytes']);
      let obj = {type: 'Buffer', data: [1, 2]};
      assert.throws(() => { type.clone(obj); });
      assert.deepEqual(
        type.clone(obj, {coerceBuffers: true}),
        Buffer.from([1, 2])
      );
      assert.deepEqual(type.clone(null, {coerceBuffers: true}), null);
    });

    test('wrapped write', () => {
      let type = new builtins.UnwrappedUnionType(['null', 'int']);
      assert.throws(() => { type.toBuffer({'int': 1}); });
    });

    test('to JSON', () => {
      let type = new builtins.UnwrappedUnionType(['null', 'int']);
      assert.equal(JSON.stringify(type), '["null","int"]');
      assert.equal(type.inspect(), '<UnwrappedUnionType ["null","int"]>');
    });

    test('resolve int to [string, long]', () => {
      let t1 = Type.forSchema('int');
      let t2 = new builtins.UnwrappedUnionType(['string', 'long']);
      let a = t2.createResolver(t1);
      let buf = t1.toBuffer(23);
      assert.deepEqual(t2.fromBuffer(buf, a), 23);
    });

    test('resolve null to [null, int]', () => {
      let t1 = Type.forSchema('null');
      let t2 = new builtins.UnwrappedUnionType(['null', 'int']);
      let a = t2.createResolver(t1);
      assert.deepEqual(t2.fromBuffer(Buffer.alloc(0), a), null);
    });

    test('resolve [string, int] to unwrapped [float, bytes]', () => {
      let t1 = new builtins.WrappedUnionType(['string', 'int']);
      let t2 = new builtins.UnwrappedUnionType(['float', 'bytes']);
      let a = t2.createResolver(t1);
      let buf;
      buf = t1.toBuffer({string: 'hi'});
      assert.deepEqual(t2.fromBuffer(buf, a), Buffer.from('hi'));
      buf = t1.toBuffer({'int': 1});
      assert.deepEqual(t2.fromBuffer(buf, a), 1);
    });

    test('clone', () => {
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
      assert.throws(() => { t.clone([]); });
      assert.throws(() => { t.clone([], {}); });
      assert.throws(() => { t.clone(undefined); });
    });

    test('invalid null', () => {
      let t = new builtins.UnwrappedUnionType(['string', 'int']);
      assert.throws(() => { t.fromString(null); }, /invalid/);
    });

    test('invalid multiple keys', () => {
      let t = new builtins.UnwrappedUnionType(['null', 'int']);
      let o = {'int': 2};
      assert.equal(t.fromString(JSON.stringify(o)), 2);
      o.foo = 3;
      assert.throws(() => { t.fromString(JSON.stringify(o)); });
    });

    test('clone named type', () => {
      let t = Type.forSchema({
        name: 'Person',
        type: 'record',
        fields: [
          {name: 'id1', type: {name: 'an.Id', type: 'fixed', size: 1}},
          {name: 'id2', type: ['null', 'an.Id']}
        ]
      }, {wrapUnions: false});
      let b = Buffer.from([0]);
      let o = {id1: b, id2: b};
      assert.deepEqual(t.clone(o), o);
    });

    test('compare buffers', () => {
      let t = new builtins.UnwrappedUnionType(['null', 'double']);
      let b1 = t.toBuffer(null);
      assert.equal(t.compareBuffers(b1, b1), 0);
      let b2 = t.toBuffer(4);
      assert.equal(t.compareBuffers(b2, b1), 1);
      assert.equal(t.compareBuffers(b1, b2), -1);
      let b3 = t.toBuffer(6);
      assert.equal(t.compareBuffers(b3, b2), 1);
    });

    test('compare', () => {
      let t;
      t = new builtins.UnwrappedUnionType(['null', 'int']);
      assert.equal(t.compare(null, 3), -1);
      assert.equal(t.compare(null, null), 0);
      assert.throws(() => { t.compare('hi', 2); });
      assert.throws(() => { t.compare(null, 'hey'); });
    });

    test('wrap', () => {
      let t = new builtins.UnwrappedUnionType(['null', 'double']);
      assert.throws(() => { t.wrap(1.0); }, /directly/);
    });

  });

  suite('WrappedUnionType', () => {

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
        valid: [null, {'a.B': Buffer.alloc(2)}],
        invalid: [Buffer.alloc(2)],
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

    test('getTypes', () => {
      let t = Type.forSchema(['null', 'int']);
      let ts = t.types;
      assert(ts[0].equals(Type.forSchema('null')));
      assert(ts[1].equals(Type.forSchema('int')));
    });

    test('get branch type', () => {
      let type = new builtins.WrappedUnionType(['null', 'int']);
      let buf = type.toBuffer({'int': 48});
      let branchType = type.fromBuffer(buf).constructor.type;
      assert(branchType instanceof builtins.IntType);
    });

    test('missing name write', () => {
      let type = new builtins.WrappedUnionType(['null', 'int']);
      assert.throws(() => {
        type.toBuffer({b: 'a'});
      });
    });

    test('read invalid index', () => {
      let type = new builtins.WrappedUnionType(['null', 'int']);
      let buf = Buffer.from([1, 0]);
      assert.throws(() => { type.fromBuffer(buf); });
    });

    test('non wrapped write', () => {
      let type = new builtins.WrappedUnionType(['null', 'int']);
      assert.throws(() => {
        type.toBuffer(1, true);
      }, Error);
    });

    test('to JSON', () => {
      let type = new builtins.WrappedUnionType(['null', 'int']);
      assert.equal(JSON.stringify(type), '["null","int"]');
    });

    test('resolve int to [long, int]', () => {
      let t1 = Type.forSchema('int');
      let t2 = new builtins.WrappedUnionType(['long', 'int']);
      let a = t2.createResolver(t1);
      let buf = t1.toBuffer(23);
      assert.deepEqual(t2.fromBuffer(buf, a), {'long': 23});
    });

    test('resolve null to [null, int]', () => {
      let t1 = Type.forSchema('null');
      let t2 = new builtins.WrappedUnionType(['null', 'int']);
      let a = t2.createResolver(t1);
      assert.deepEqual(t2.fromBuffer(Buffer.alloc(0), a), null);
    });

    test('resolve [string, int] to [long, bytes]', () => {
      let t1 = new builtins.WrappedUnionType(['string', 'int']);
      let t2 = new builtins.WrappedUnionType(['long', 'bytes']);
      let a = t2.createResolver(t1);
      let buf;
      buf = t1.toBuffer({string: 'hi'});
      assert.deepEqual(
        t2.fromBuffer(buf, a),
        {'bytes': Buffer.from('hi')}
      );
      buf = t1.toBuffer({'int': 1});
      assert.deepEqual(t2.fromBuffer(buf, a), {'long': 1});
    });

    test('resolve unwrapped [string, int] to [long, bytes]', () => {
      let t1 = new builtins.UnwrappedUnionType(['string', 'int']);
      let t2 = new builtins.WrappedUnionType(['long', 'bytes']);
      let a = t2.createResolver(t1);
      let buf;
      buf = t1.toBuffer('hi');
      assert.deepEqual(
        t2.fromBuffer(buf, a),
        {'bytes': Buffer.from('hi')}
      );
      buf = t1.toBuffer(1);
      assert.deepEqual(t2.fromBuffer(buf, a), {'long': 1});
    });

    test('clone', () => {
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
      assert.throws(() => { t.clone([]); });
      assert.throws(() => { t.clone([], {}); });
      assert.throws(() => { t.clone(undefined); });
    });

    test('clone and wrap', () => {
      let t = new builtins.WrappedUnionType(['string', 'int']);
      let o;
      o = t.clone('hi', {wrapUnions: true});
      assert.deepEqual(o, {'string': 'hi'});
      o = t.clone(3, {wrapUnions: true});
      assert.deepEqual(o, {'int': 3});
      assert.throws(() => { t.clone(null, {wrapUnions: 2}); });
    });

    test('unwrap', () => {
      let t = new builtins.WrappedUnionType(['string', 'int']);
      let v = t.clone({string: 'hi'});
      assert.equal(v.unwrap(), 'hi');
    });

    test('invalid multiple keys', () => {
      let t = new builtins.WrappedUnionType(['null', 'int']);
      let o = {'int': 2};
      assert(t.isValid(o));
      o.foo = 3;
      assert(!t.isValid(o));
    });

    test('clone multiple keys', () => {
      let t = new builtins.WrappedUnionType(['null', 'int']);
      let o = {'int': 2, foo: 3};
      assert.throws(() => { t.clone(o); });
      assert.throws(() => { t.clone(o, {}); });
    });

    test('clone qualify names', () => {
      let t = Type.forSchema({
        name: 'Person',
        type: 'record',
        fields: [
          {name: 'id1', type: {name: 'an.Id', type: 'fixed', size: 1}},
          {name: 'id2', type: ['null', 'an.Id']}
        ]
      }, {wrapUnions: true});
      let b = Buffer.from([0]);
      let o = {id1: b, id2: {Id: b}};
      let c = {id1: b, id2: {'an.Id': b}};
      assert.throws(() => { t.clone(o, {}); });
      assert.deepEqual(t.clone(o, {qualifyNames: true}), c);
    });

    test('clone invalid qualified names', () => {
      let t = Type.forSchema({
        name: 'Person',
        type: 'record',
        fields: [
          {name: 'id1', type: {name: 'Id', type: 'fixed', size: 1}},
          {name: 'id2', type: ['null', 'Id']}
        ]
      }, {wrapUnions: true});
      let b = Buffer.from([0]);
      let o = {id1: b, id2: {'an.Id': b}};
      assert.throws(() => { t.clone(o); });
      assert.throws(() => { t.clone(o, {}); });
    });

    test('compare buffers', () => {
      let t = new builtins.WrappedUnionType(['null', 'double']);
      let b1 = t.toBuffer(null);
      assert.equal(t.compareBuffers(b1, b1), 0);
      let b2 = t.toBuffer({'double': 4});
      assert.equal(t.compareBuffers(b2, b1), 1);
      assert.equal(t.compareBuffers(b1, b2), -1);
      let b3 = t.toBuffer({'double': 6});
      assert.equal(t.compareBuffers(b3, b2), 1);
    });

    test('compare', () => {
      let t;
      t = new builtins.WrappedUnionType(['null', 'int']);
      assert.equal(t.compare(null, {'int': 3}), -1);
      assert.equal(t.compare(null, null), 0);
      t = new builtins.WrappedUnionType(['int', 'float']);
      assert.equal(t.compare({'int': 2}, {'float': 0.5}), -1);
      assert.equal(t.compare({'int': 20}, {'int': 5}), 1);
    });

    test('isValid hook', () => {
      let t = new builtins.WrappedUnionType(['null', 'int']);
      let paths = [];
      assert(t.isValid(null, {errorHook: hook}));
      assert(t.isValid({'int': 1}, {errorHook: hook}));
      assert(!paths.length);
      assert(!t.isValid({'int': 'hi'}, {errorHook: hook}));
      assert.deepEqual(paths, [['int']]);

      function hook(path) { paths.push(path); }
    });

    // via https://github.com/mtth/avsc/pull/469
    test('synthetic constructor', () => {
      const name = 'Foo';
      const type = types.Type.forSchema([
        'null',
        {type: 'record', name: `test.${name}`, fields: [{name: 'id', type: 'string'}]},
      ]);

      const data = {id: 'abc'};
      const roundtripped = type.fromBuffer(type.toBuffer(data));
      assert.equal(roundtripped.constructor.name, name);
      assert.deepEqual(roundtripped, data);
    });
  });

  suite('EnumType', () => {

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

    test('get full name', () => {
      let t = Type.forSchema({
        type: 'enum',
        symbols: ['A', 'B'],
        name: 'Letter',
        namespace: 'latin'
      });
      assert.equal(t.name, 'latin.Letter');
      assert.equal(t.branchName, 'latin.Letter');
    });

    test('get aliases', () => {
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

    test('get symbols', () => {
      let t = Type.forSchema({
        type: 'enum',
        symbols: ['A', 'B'],
        name: 'Letter'
      });
      let symbols = t.getSymbols();
      assert.deepEqual(symbols, ['A', 'B']);
    });

    test('duplicate symbol', () => {
      assert.throws(() => {
        Type.forSchema({type: 'enum', symbols: ['A', 'B', 'A'], name: 'B'});
      });
    });

    test('missing name', () => {
      let schema = {type: 'enum', symbols: ['A', 'B']};
      let t = Type.forSchema(schema);
      assert.strictEqual(t.getName(), undefined);
      assert.strictEqual(t.getName(true), 'enum');
      assert.throws(() => {
        Type.forSchema(schema, {noAnonymousTypes: true});
      });
    });

    test('write invalid', () => {
      let type = Type.forSchema({type: 'enum', symbols: ['A'], name: 'a'});
      assert.throws(() => {
        type.toBuffer('B');
      });
    });

    test('read invalid index', () => {
      let type = new builtins.EnumType({
        type: 'enum', symbols: ['A'], name: 'a'
      });
      let buf = Buffer.from([2]);
      assert.throws(() => { type.fromBuffer(buf); });
    });

    test('resolve', () => {
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
      assert.throws(() => {
        t1.createResolver(newEnum('Foo2', ['bar', 'baz', 'bax']));
      });
      assert.throws(() => {
        t1.createResolver(newEnum('Foo3', ['foo', 'bar']));
      });
      assert.throws(() => {
        t1.createResolver(Type.forSchema('int'));
      });
      function newEnum(name, symbols, aliases, namespace) {
        let obj = {type: 'enum', name, symbols};
        if (aliases !== undefined) {
          obj.aliases = aliases;
        }
        if (namespace !== undefined) {
          obj.namespace = namespace;
        }
        return new builtins.EnumType(obj);
      }
    });

    test('resolve with default', () => {
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

    test('invalid default', () => {
      assert.throws(() => {
        new builtins.EnumType({
          name: 'W',
          symbols: ['A', 'B'],
          default: 'D',
        });
      });
    });

    test('clone', () => {
      let t = Type.forSchema({
        type: 'enum',
        name: 'Foo',
        symbols: ['bar', 'baz']
      });
      assert.equal(t.clone('bar'), 'bar');
      assert.equal(t.clone('bar', {}), 'bar');
      assert.throws(() => { t.clone('BAR'); });
      assert.throws(() => { t.clone(null); });
    });

    test('compare buffers', () => {
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

    test('compare', () => {
      let t = Type.forSchema({type: 'enum', name: 'Foo', symbols: ['b', 'a']});
      assert.equal(t.compare('b', 'a'), -1);
      assert.equal(t.compare('a', 'a'), 0);
    });

  });

  suite('FixedType', () => {

    let data = [
      {
        name: 'size 1',
        schema: {name: 'Foo', size: 2},
        valid: [Buffer.from([1, 2]), Buffer.from([2, 3])],
        invalid: [
          'HEY',
          null,
          undefined,
          0,
          Buffer.alloc(1),
          Buffer.alloc(3)
        ],
        check: function (a, b) { assert(Buffer.compare(a, b) === 0); }
      }
    ];

    let invalidSchemas = [
      {name: 'Foo', size: NaN},
      {name: 'Foo', size: -2},
      {name: 'Foo'},
      {}
    ];

    testType(builtins.FixedType, data, invalidSchemas);

    test('get full name', () => {
      let t = Type.forSchema({
        type: 'fixed',
        size: 2,
        name: 'Id',
        namespace: 'id'
      });
      assert.equal(t.getName(), 'id.Id');
      assert.equal(t.getName(true), 'id.Id');
    });

    test('get aliases', () => {
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

    test('get size', () => {
      let t = Type.forSchema({type: 'fixed', size: 5, name: 'Id'});
      assert.equal(t.getSize(), 5);
    });

    test('get zero size', () => {
      let t = Type.forSchema({type: 'fixed', size: 0, name: 'Id'});
      assert.equal(t.getSize(), 0);
    });

    test('resolve', () => {
      let t1 = new builtins.FixedType({name: 'Id', size: 4});
      let t2 = new builtins.FixedType({name: 'Id', size: 4});
      assert.doesNotThrow(() => { t2.createResolver(t1); });
      t2 = new builtins.FixedType({name: 'Id2', size: 4});
      assert.throws(() => { t2.createResolver(t1); });
      t2 = new builtins.FixedType({name: 'Id2', size: 4, aliases: ['Id']});
      assert.doesNotThrow(() => { t2.createResolver(t1); });
      t2 = new builtins.FixedType({name: 'Id2', size: 5, aliases: ['Id']});
      assert.throws(() => { t2.createResolver(t1); });
    });

    test('clone', () => {
      let t = new builtins.FixedType({name: 'Id', size: 2});
      let s = '\x01\x02';
      let buf = Buffer.from(s);
      let clone;
      clone = t.clone(buf);
      assert.deepEqual(clone, buf);
      clone = t.clone(buf, {});
      assert.deepEqual(clone, buf);
      clone[0] = 0;
      assert.equal(buf[0], 1);
      assert.throws(() => { t.clone(s); });
      assert.throws(() => { t.clone(s, {}); });
      clone = t.clone(buf.toJSON(), {coerceBuffers: true});
      assert.deepEqual(clone, buf);
      assert.throws(() => { t.clone(1, {coerceBuffers: true}); });
      assert.throws(() => { t.clone(Buffer.from([2])); });
    });

    test('fromString', () => {
      let t = new builtins.FixedType({name: 'Id', size: 2});
      let s = '\x01\x02';
      let buf = Buffer.from(s);
      let clone = t.fromString(JSON.stringify(s));
      assert.deepEqual(clone, buf);
    });

    test('compare buffers', () => {
      let t = Type.forSchema({type: 'fixed', name: 'Id', size: 2});
      let b1 = Buffer.from([1, 2]);
      assert.equal(t.compareBuffers(b1, b1), 0);
      let b2 = Buffer.from([2, 2]);
      assert.equal(t.compareBuffers(b1, b2), -1);
    });

  });

  suite('MapType', () => {

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

    test('get values type', () => {
      let t = new builtins.MapType({type: 'map', values: 'int'});
      assert(t.getValuesType().equals(Type.forSchema('int')));
    });

    test('write int', () => {
      let t = new builtins.MapType({type: 'map', values: 'int'});
      let buf = t.toBuffer({'\x01': 3, '\x02': 4});
      assert.deepEqual(buf, Buffer.from([4, 2, 1, 6, 2, 2, 8, 0]));
    });

    test('read long', () => {
      let t = new builtins.MapType({type: 'map', values: 'long'});
      let buf = Buffer.from([4, 2, 1, 6, 2, 2, 8, 0]);
      assert.deepEqual(t.fromBuffer(buf), {'\x01': 3, '\x02': 4});
    });

    test('read with sizes', () => {
      let t = new builtins.MapType({type: 'map', values: 'int'});
      let buf = Buffer.from([1,6,2,97,2,0]);
      assert.deepEqual(t.fromBuffer(buf), {a: 1});
    });

    test('skip', () => {
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
      let b1 = Buffer.from([2,2,97,2,0,6]); // Without sizes.
      let b2 = Buffer.from([1,6,2,97,2,0,6]); // With sizes.
      let resolver = v2.createResolver(v1);
      assert.deepEqual(v2.fromBuffer(b1, resolver), {val: 3});
      assert.deepEqual(v2.fromBuffer(b2, resolver), {val: 3});
    });

    test('resolve int > long', () => {
      let t1 = new builtins.MapType({type: 'map', values: 'int'});
      let t2 = new builtins.MapType({type: 'map', values: 'long'});
      let resolver = t2.createResolver(t1);
      let obj = {one: 1, two: 2};
      let buf = t1.toBuffer(obj);
      assert.deepEqual(t2.fromBuffer(buf, resolver), obj);
    });

    test('resolve double > double', () => {
      let t = new builtins.MapType({type: 'map', values: 'double'});
      let resolver = t.createResolver(t);
      let obj = {one: 1, two: 2};
      let buf = t.toBuffer(obj);
      assert.deepEqual(t.fromBuffer(buf, resolver), obj);
    });

    test('resolve invalid', () => {
      let t1 = new builtins.MapType({type: 'map', values: 'int'});
      let t2 = new builtins.MapType({type: 'map', values: 'string'});
      assert.throws(() => { t2.createResolver(t1); });
      t2 = new builtins.ArrayType({type: 'array', items: 'string'});
      assert.throws(() => { t2.createResolver(t1); });
    });

    test('resolve fixed', () => {
      let t1 = Type.forSchema({
        type: 'map', values: {name: 'Id', type: 'fixed', size: 2}
      });
      let t2 = Type.forSchema({
        type: 'map', values: {
          name: 'Id2', aliases: ['Id'], type: 'fixed', size: 2
        }
      });
      let resolver = t2.createResolver(t1);
      let obj = {one: Buffer.from([1, 2])};
      let buf = t1.toBuffer(obj);
      assert.deepEqual(t2.fromBuffer(buf, resolver), obj);
    });

    test('clone', () => {
      let t = new builtins.MapType({type: 'map', values: 'int'});
      let o = {one: 1, two: 2};
      let c;
      c = t.clone(o);
      assert.deepEqual(c, o);
      c = t.clone(o, {});
      assert.deepEqual(c, o);
      c.one = 3;
      assert.equal(o.one, 1);
      assert.throws(() => { t.clone(undefined); });
      assert.throws(() => { t.clone(undefined, {}); });
    });

    test('clone coerce buffers', () => {
      let t = new builtins.MapType({type: 'map', values: 'bytes'});
      let o = {one: {type: 'Buffer', data: [1]}};
      assert.throws(() => { t.clone(o, {}); });
      assert.throws(() => { t.clone(o); });
      let c = t.clone(o, {coerceBuffers: true});
      assert.deepEqual(c, {one: Buffer.from([1])});
    });

    test('compare buffers', () => {
      let t = new builtins.MapType({type: 'map', values: 'bytes'});
      let b1 = t.toBuffer({});
      assert.throws(() => { t.compareBuffers(b1, b1); });
    });

    test('isValid hook', () => {
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

    test('getName', () => {
      let t = new builtins.MapType({type: 'map', values: 'int'});
      assert.strictEqual(t.getName(), undefined);
      assert.strictEqual(t.getName(true), 'map');
    });

  });

  suite('ArrayType', () => {

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

    test('get items type', () => {
      let t = new builtins.ArrayType({type: 'array', items: 'int'});
      assert(t.getItemsType().equals(Type.forSchema('int')));
    });

    test('read with sizes', () => {
      let t = new builtins.ArrayType({type: 'array', items: 'int'});
      let buf = Buffer.from([1,2,2,0]);
      assert.deepEqual(t.fromBuffer(buf), [1]);
    });

    test('skip', () => {
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
      let b1 = Buffer.from([2,2,0,6]); // Without sizes.
      let b2 = Buffer.from([1,2,2,0,6]); // With sizes.
      let resolver = v2.createResolver(v1);
      assert.deepEqual(v2.fromBuffer(b1, resolver), {val: 3});
      assert.deepEqual(v2.fromBuffer(b2, resolver), {val: 3});
    });

    test('resolve string items to bytes items', () => {
      let t1 = new builtins.ArrayType({type: 'array', items: 'string'});
      let t2 = new builtins.ArrayType({type: 'array', items: 'bytes'});
      let resolver = t2.createResolver(t1);
      let obj = ['\x01\x02'];
      let buf = t1.toBuffer(obj);
      assert.deepEqual(
        t2.fromBuffer(buf, resolver),
        [Buffer.from([1, 2])]
      );
    });

    test('resolve invalid', () => {
      let t1 = new builtins.ArrayType({type: 'array', items: 'string'});
      let t2 = new builtins.ArrayType({type: 'array', items: 'long'});
      assert.throws(() => { t2.createResolver(t1); });
      t2 = new builtins.MapType({type: 'map', values: 'string'});
      assert.throws(() => { t2.createResolver(t1); });
    });

    test('clone', () => {
      let t = new builtins.ArrayType({type: 'array', items: 'int'});
      let o = [1, 2];
      let c;
      c = t.clone(o);
      assert.deepEqual(c, o);
      c = t.clone(o, {});
      assert.deepEqual(c, o);
      c.one = 3;
      assert.equal(o[0], 1);
      assert.throws(() => { t.clone({}); });
      assert.throws(() => { t.clone({}, {}); });
    });

    test('clone coerce buffers', () => {
      let t = Type.forSchema({
        type: 'array',
        items: {type: 'fixed', name: 'Id', size: 2}
      });
      let o = [{type: 'Buffer', data: [1, 2]}];
      assert.throws(() => { t.clone(o); });
      assert.throws(() => { t.clone(o, {}); });
      let c = t.clone(o, {coerceBuffers: true});
      assert.deepEqual(c, [Buffer.from([1, 2])]);
    });

    test('compare buffers', () => {
      let t = Type.forSchema({type: 'array', items: 'int'});
      assert.equal(t.compareBuffers(t.toBuffer([]), t.toBuffer([])), 0);
      assert.equal(t.compareBuffers(t.toBuffer([1, 2]), t.toBuffer([])), 1);
      assert.equal(t.compareBuffers(t.toBuffer([1]), t.toBuffer([1, -1])), -1);
      assert.equal(t.compareBuffers(t.toBuffer([1]), t.toBuffer([2])), -1);
      assert.equal(t.compareBuffers(t.toBuffer([1, 2]), t.toBuffer([1])), 1);
    });

    test('compare', () => {
      let t = Type.forSchema({type: 'array', items: 'int'});
      assert.equal(t.compare([], []), 0);
      assert.equal(t.compare([], [-1]), -1);
      assert.equal(t.compare([1], [1]), 0);
      assert.equal(t.compare([2], [1, 2]), 1);
    });

    test('isValid hook invalid array', () => {
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

    test('isValid hook invalid elems', () => {
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

    test('isValid hook reentrant', () => {
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

    test('round-trip multi-block array', () => {
      let tap = Tap.withCapacity(64);
      tap.writeLong(2);
      tap.writeString('hi');
      tap.writeString('hey');
      tap.writeLong(1);
      tap.writeString('hello');
      tap.writeLong(0);
      let t = new builtins.ArrayType({items: 'string'});
      assert.deepEqual(
        t.fromBuffer(tap.subarray(0, tap.pos)),
        ['hi', 'hey', 'hello']
      );
    });

  });

  suite('RecordType', () => {

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

    test('duplicate field names', () => {
      assert.throws(() => {
        Type.forSchema({
          type: 'record',
          name: 'Person',
          fields: [{name: 'age', type: 'int'}, {name: 'age', type: 'float'}]
        });
      });
    });

    test('invalid name', () => {
      let schema = {
        name: 'foo-bar.Bar',
        type: 'record',
        fields: [{name: 'id', type: 'int'}]
      };
      assert.throws(() => {
        Type.forSchema(schema);
      }, /invalid name/);
    });

    test('reserved name', () => {
      let schema = {
        name: 'case',
        type: 'record',
        fields: [{name: 'id', type: 'int'}]
      };
      let Case = Type.forSchema(schema).recordConstructor;
      let c = new Case(123);
      assert.equal(c.id, 123);
    });

    test('default constructor', () => {
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

    test('wrap values', () => {
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

    test('default check & write', () => {
      let type = Type.forSchema({
        type: 'record',
        name: 'Person',
        fields: [
          {name: 'age', type: 'int', 'default': 25},
          {name: 'name', type: 'string', 'default': '\x01'}
        ]
      });
      assert.deepEqual(type.toBuffer({}), Buffer.from([50, 2, 1]));
    });

    test('fixed string default', () => {
      let s = '\x01\x04';
      let b = Buffer.from(s);
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
      assert.deepEqual(obj.id, Buffer.from([1, 4]));
      assert.deepEqual(type.toBuffer({}), b);
    });

    test('fixed buffer invalid default', () => {
      assert.throws(() => {
        Type.forSchema({
          type: 'record',
          name: 'Object',
          fields: [
            {
              name: 'id',
              type: {type: 'fixed', size: 2, name: 'Id'},
              'default': Buffer.from([0])
            }
          ]
        });
      });
    });

    test('union invalid default', () => {
      assert.throws(() => {
        Type.forSchema({
          type: 'record',
          name: 'Person',
          fields: [{name: 'name', type: ['null', 'string'], 'default': ''}]
        });
      }, /incompatible.*first branch/);
    });

    test('record default', () => {
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

    test('record keyword field name', () => {
      let type = Type.forSchema({
        type: 'record',
        name: 'Person',
        fields: [{name: 'null', type: 'int'}]
      });
      let Person = type.getRecordConstructor();
      assert.deepEqual(new Person(2), {'null': 2});
    });

    test('record isValid', () => {
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

    test('record toBuffer', () => {
      let type = Type.forSchema({
        type: 'record',
        name: 'Person',
        fields: [{name: 'age', type: 'int'}]
      });
      let Person = type.getRecordConstructor();
      assert.deepEqual((new Person(48)).toBuffer(), Buffer.from([96]));
      assert.throws(() => { (new Person()).toBuffer(); });
    });

    test('record compare', () => {
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

    test('Record type', () => {
      let type = Type.forSchema({
        type: 'record',
        name: 'Person',
        fields: [{name: 'age', type: 'int'}]
      });
      let Person = type.getRecordConstructor();
      assert.strictEqual(Person.getType(), type);
    });

    test('mutable defaults', () => {
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

    test('resolve alias', () => {
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
      assert.throws(() => { v3.createResolver(v1); });
    });

    test('resolve alias with namespace', () => {
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
      assert.throws(() => { v2.createResolver(v1); });
      let v3 = Type.forSchema({
        type: 'record',
        name: 'Human',
        aliases: ['earth.Person'],
        fields: [{name: 'name', type: 'string'}]
      });
      assert.doesNotThrow(() => { v3.createResolver(v1); });
    });

    test('resolve skip field', () => {
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

    test('resolve new field', () => {
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

    test('resolve field with javascript keyword as name', () => {
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

    test('resolve new field no default', () => {
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
      assert.throws(() => { v2.createResolver(v1); });
    });

    test('resolve from recursive schema', () => {
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

    test('resolve to recursive schema', () => {
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

    test('resolve from both recursive schema', () => {
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

    test('resolve multiple matching aliases', () => {
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
      assert.throws(() => { v2.createResolver(v1); });
    });

    test('resolve consolidated reads same type', () => {
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

    test('resolve consolidated reads different types', () => {
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

    test('getName', () => {
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

    test('getSchema', () => {
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

    test('getSchema recursive schema', () => {
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

    test('fromString', () => {
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
      assert.throws(() => { t.fromString('{}'); });
    });

    test('toString record', () => {
      let T = Type.forSchema({
        type: 'record',
        name: 'Person',
        fields: [{name: 'pwd', type: 'bytes'}]
      }).getRecordConstructor();
      let r = new T(Buffer.from([1, 2]));
      assert.equal(r.toString(), T.getType().toString(r));
    });

    test('clone', () => {
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

    test('clone field default', () => {
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
      assert.throws(() => { t.clone({}); });
    });

    test('clone field hook', () => {
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

    test('clone missing fields', () => {
      let t = Type.forSchema({
        type: 'record',
        name: 'Person',
        fields: [
          {name: 'id', type: 'int'},
          {name: 'name', type: ['null', 'string']},
          {name: 'age', type: ['null', 'int'], 'default': null},
        ]
      });
      assert.throws(() => { t.clone({id: 1}); }, /invalid/);
      assert.deepEqual(
        t.clone({id: 1}, {skipMissingFields: true}),
        {id: 1, name: undefined, age: null}
      );
    });

    test('unwrapped union field default', () => {
      assert.throws(() => {
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

    test('wrapped union field default', () => {
      assert.throws(() => {
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

    test('get full name & aliases', () => {
      let t = Type.forSchema({
        type: 'record',
        name: 'Person',
        namespace: 'a',
        fields: [{name: 'age', type: 'int'}, {name: 'name', type: 'string'}]
      });
      assert.equal(t.getName(), 'a.Person');
      assert.deepEqual(t.getAliases(), []);
    });

    test('field getters', () => {
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

    test('field order', () => {
      let t = Type.forSchema({
        type: 'record',
        name: 'Person',
        fields: [{name: 'age', type: 'int'}]
      });
      let field = t.getFields()[0];
      assert.equal(field.order, 'ascending'); // Default.
      assert.equal(field.getOrder(), 'ascending'); // Default.
    });

    test('compare buffers default order', () => {
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

    test('compare buffers custom order', () => {
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

    test('compare buffers invalid order', () => {
      assert.throws(() => { Type.forSchema({
        type: 'record',
        name: 'Person',
        fields: [{name: 'age', type: 'int', order: 'up'}]
      }); });
    });

    test('error type', () => {
      let t = Type.forSchema({
        type: 'error',
        name: 'Ouch',
        fields: [{name: 'name', type: 'string'}]
      });
      let E = t.getRecordConstructor();
      let err = new E('MyError');
      assert(err instanceof Error);
    });

    test('error stack field not overwritten', () => {
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

    test('error stack trace', () => {
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

    test('no stack trace by default', () => {
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

    test('no stack when no matching field', () => {
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

    test('no stack when non-string stack field', () => {
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

    test('anonymous error type', () => {
      assert.doesNotThrow(() => { Type.forSchema({
        type: 'error',
        fields: [{name: 'name', type: 'string'}]
      }); });
    });

    test('resolve error type', () => {
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

    test('isValid hook', () => {
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

    test('isValid empty record', () => {
      let t = Type.forSchema({type: 'record', name: 'Person', fields: []});
      assert(t.isValid({}));
    });

    test('isValid no undeclared fields', () => {
      let t = Type.forSchema({
        type: 'record',
        fields: [{name: 'foo', type: 'int'}]
      });
      let obj = {foo: 1, bar: 'bar'};
      assert(t.isValid(obj));
      assert(!t.isValid(obj, {noUndeclaredFields: true}));
      assert(t.isValid({foo: 23}, {noUndeclaredFields: true}));
    });

    test('qualified name namespacing', () => {
      let t = Type.forSchema({
        type: 'record',
        name: '.Foo',
        fields: [
          {name: 'id', type: {type: 'record', name: 'Bar', fields: []}}
        ]
      }, {namespace: 'bar'});
      assert.equal(t.getField('id').getType().getName(), 'Bar');
    });

    test('omit record methods', () => {
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

  suite('AbstractLongType', () => {

    let fastLongType = new builtins.LongType();

    suite('unpacked', () => {

      let slowLongType = builtins.LongType.__with({
        fromBuffer: function (buf) {
          let dv = new DataView(buf.buffer, buf.byteOffset, buf.byteLength);
          let neg = buf[7] >> 7;
          if (neg) { // Negative number.
            invert(buf);
          }
          let n = dv.getInt32(0, true) + Math.pow(2, 32) * dv.getInt32(4, true);
          if (neg) {
            invert(buf);
            n = -n - 1;
          }
          return n;
        },
        toBuffer: function (n) {
          let buf = new Uint8Array(8);
          let dv = new DataView(buf.buffer, buf.byteOffset, buf.byteLength);
          let neg = n < 0;
          if (neg) {
            invert(buf);
            n = -n - 1;
          }
          dv.setInt32(0, n | 0, true);
          let h = n / Math.pow(2, 32) | 0;
          dv.setInt32(4, h ? h : (n >= 0 ? 0 : -1), true);
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

      test('schema', () => {
        assert.equal(slowLongType.schema(), 'long');
      });

      test('encode', () => {
        [123, -1, 321414, 900719925474090].forEach((n) => {
          assert.deepEqual(slowLongType.toBuffer(n), fastLongType.toBuffer(n));
        });
      });

      test('decode', () => {
        [123, -1, 321414, 900719925474090].forEach((n) => {
          let buf = fastLongType.toBuffer(n);
          assert.deepEqual(slowLongType.fromBuffer(buf), n);
        });
      });

      test('clone', () => {
        assert.equal(slowLongType.clone(123), 123);
        assert.equal(slowLongType.clone(123, {}), 123);
        assert.equal(slowLongType.fromString('-1'), -1);
        assert.equal(slowLongType.toString(-1), '-1');
      });

      test('random', () => {
        assert(slowLongType.isValid(slowLongType.random()));
      });

      test('isValid hook', () => {
        let s = 'hi';
        let errs = [];
        assert(!slowLongType.isValid(s, {errorHook: hook}));
        assert.deepEqual(errs, [s]);
        assert.throws(() => { slowLongType.toBuffer(s); });

        function hook(path, obj, type) {
          assert.strictEqual(type, slowLongType);
          assert.equal(path.length, 0);
          errs.push(obj);
        }
      });

      test('resolve between long', () => {
        let b = fastLongType.toBuffer(123);
        let fastToSlow = slowLongType.createResolver(fastLongType);
        assert.equal(slowLongType.fromBuffer(b, fastToSlow), 123);
        let slowToFast = fastLongType.createResolver(slowLongType);
        assert.equal(fastLongType.fromBuffer(b, slowToFast), 123);
      });

      test('resolve from int', () => {
        let intType = Type.forSchema('int');
        let b = intType.toBuffer(123);
        let r = slowLongType.createResolver(intType);
        assert.equal(slowLongType.fromBuffer(b, r), 123);
      });

      test('resolve to double and float', () => {
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

    suite('packed', () => {

      let slowLongType = builtins.LongType.__with({
        fromBuffer: function (buf) {
          let tap = Tap.fromBuffer(buf);
          return tap.readLong();
        },
        toBuffer: function (n) {
          let buf = Buffer.alloc(10);
          let tap = Tap.fromBuffer(buf);
          tap.writeLong(n);
          return buf.subarray(0, tap.pos);
        },
        fromJSON: function (n) { return n; },
        toJSON: function (n) { return n; },
        isValid: function (n) { return typeof n == 'number' && n % 1 === 0; },
        compare: function (n1, n2) {
          return n1 === n2 ? 0 : (n1 < n2 ? -1 : 1);
        }
      }, true);

      test('encode', () => {
        [123, -1, 321414, 900719925474090].forEach((n) => {
          assert.deepEqual(slowLongType.toBuffer(n), fastLongType.toBuffer(n));
        });
      });

      test('decode', () => {
        [123, -1, 321414, 900719925474090].forEach((n) => {
          let buf = fastLongType.toBuffer(n);
          assert.deepEqual(slowLongType.fromBuffer(buf), n);
        });
      });

      test('clone', () => {
        assert.equal(slowLongType.clone(123), 123);
        assert.equal(slowLongType.fromString('-1'), -1);
        assert.equal(slowLongType.toString(-1), '-1');
      });

      test('random', () => {
        assert(slowLongType.isValid(slowLongType.random()));
      });

      test('evolution to/from', () => {
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

    test('within unwrapped union', () => {
      let longType = builtins.LongType.__with({
        fromBuffer: function (buf) { return {value: buf}; },
        toBuffer: function (obj) { return obj.value; },
        fromJSON: function () { throw new Error(); },
        toJSON: function () { throw new Error(); },
        isValid: function (obj) { return obj && Buffer.isBuffer(obj.value); },
        compare: function () { throw new Error(); }
      }, true);
      let t = Type.forSchema(['null', 'long'], {registry: {'long': longType}});
      let v = {value: Buffer.from([4])}; // Long encoding of 2.

      assert(t.isValid(null));
      assert(t.isValid(v));
      assert.deepEqual(t.fromBuffer(t.toBuffer(v)), v);
    });

    test('incomplete buffer', () => {
      // Check that `fromBuffer` doesn't get called.
      let slowLongType = builtins.LongType.__with({
        fromBuffer: function () { throw new Error('no'); },
        toBuffer: null,
        fromJSON: null,
        toJSON: null,
        isValid: null,
        compare: null
      });
      let buf = fastLongType.toBuffer(12314);
      assert.deepEqual(
        slowLongType.decode(buf.subarray(0, 1)),
        {value: undefined, offset: -1}
      );
    });
  });

  suite('LogicalType', () => {

    class DateType extends LogicalType {
      constructor (schema, opts) {
        super(schema, opts);
        if (!types.Type.isType(this.getUnderlyingType(), 'long', 'string')) {
          throw new Error('invalid underlying date type');
        }
      }

      _fromValue (val) { return new Date(val); }

      _toValue (date) {
        if (!(date instanceof Date)) {
          return undefined;
        }
        if (this.getUnderlyingType().typeName === 'long') {
          return +date;
        } else {
          // String.
          return '' + date;
        }
      }

      _resolve (type) {
        if (types.Type.isType(type, 'long', 'string')) {
          return this._fromValue;
        }
      }
    }

    class AgeType extends LogicalType {
      _fromValue (val) { return val; }

      _toValue (any) {
        if (typeof any == 'number' && any >= 0) {
          return any;
        }
      }

      _resolve (type) {
        if (types.Type.isType(type, 'logical:age')) {
          return this._fromValue;
        }
      }
    }

    let logicalTypes = {age: AgeType, date: DateType};

    test('valid type', () => {
      let t = Type.forSchema({
        type: 'long',
        logicalType: 'date'
      }, {logicalTypes});
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

    test('invalid type', () => {
      let schema = {
        type: 'int',
        logicalType: 'date'
      };
      let t;
      t = Type.forSchema(schema); // Missing.
      assert(t instanceof builtins.IntType);
      t = Type.forSchema(schema, {logicalTypes}); // Invalid.
      assert(t instanceof builtins.IntType);
      assert.throws(() => {
        Type.forSchema(schema, {
          logicalTypes,
          assertLogicalTypes: true
        });
      });
    });

    test('missing type', () => {
      let t = Type.forSchema({
        type: 'long',
        logicalType: 'date'
      }, {logicalTypes: {}});
      assert(t.typeName, 'long');
    });

    test('nested types', () => {
      let schema = {
        name: 'Person',
        type: 'record',
        fields: [
          {name: 'age', type: {type: 'int', logicalType: 'age'}},
          {name: 'time', type: {type: 'long', logicalType: 'date'}}
        ]
      };
      let base = Type.forSchema(schema);
      let derived = Type.forSchema(schema, {logicalTypes});
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
      assert.throws(() => { derived.toBuffer(invalid); });
      let hasError = false;
      derived.isValid(invalid, {errorHook: function (path, any, type) {
        hasError = true;
        assert.deepEqual(path, ['age']);
        assert.equal(any, -1);
        assert(type instanceof AgeType);
      }});
      assert(hasError);
    });

    test('recursive', () => {
      function Person(friends) { this.friends = friends || []; }

      class PersonType extends LogicalType {
        _fromValue (val) {
          return new Person(val.friends);
        }

        _toValue (val) { return val; }
      }


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

    test('recursive dereferencing name', () => {
      class BoxType extends LogicalType {
        _fromValue (val) { return val.unboxed; }
        _toValue (any) { return {unboxed: any}; }
      }

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

    test('resolve underlying > logical', () => {
      let t1 = Type.forSchema({type: 'string'});
      let t2 = Type.forSchema({
        type: 'long',
        logicalType: 'date'
      }, {logicalTypes});

      let d1 = new Date(Date.now());
      let buf = t1.toBuffer('' + d1);
      let res = t2.createResolver(t1);
      assert.throws(() => { t2.createResolver(Type.forSchema('float')); });
      let d2 = t2.fromBuffer(buf, res);
      assert.deepEqual('' + d2, '' + d1); // Rounding error on date objects.
    });

    test('resolve logical > underlying', () => {
      let t1 = Type.forSchema({
        type: 'long',
        logicalType: 'date'
      }, {logicalTypes});
      let t2 = Type.forSchema({type: 'double'}); // Note long > double too.

      let d = new Date(Date.now());
      let buf = t1.toBuffer(d);
      let res = t2.createResolver(t1);
      assert.throws(() => { Type.forSchema('int').createResolver(t1); });
      assert.equal(t2.fromBuffer(buf, res), +d);
    });

    test('resolve logical type into a schema without the field', () => {
      let t1 = Type.forSchema({
        name: 'Person',
        type: 'record',
        fields: [
          {name: 'age', type: {type: 'int', logicalType: 'age'}},
          {name: 'time', type: {type: 'long', logicalType: 'date'}}
        ]
      }, {logicalTypes});
      let t2 = Type.forSchema({
        name: 'Person',
        type: 'record',
        fields: [
          {name: 'age', type: {type: 'int', logicalType: 'age'}}
        ]
      }, {logicalTypes});

      let buf = t1.toBuffer({age: 12, time: new Date()});

      let res = t2.createResolver(t1);
      let decoded = t2.fromBuffer(buf, res);
      assert.equal(decoded.age, 12);
      assert.equal(decoded.time, undefined);
    });

    test('resolve union of logical > union of logical', () => {
      let t = types.Type.forSchema(
        ['null', {type: 'int', logicalType: 'age'}],
        {logicalTypes, wrapUnions: true}
      );
      let resolver = t.createResolver(t);
      let v = {'int': 34};
      assert.deepEqual(t.fromBuffer(t.toBuffer(v), resolver), v);
    });

    test('even integer', () => {
      class EvenIntType extends LogicalType {
        _fromValue (val) {
          if (val !== (val | 0) || val % 2) {
            throw new Error('invalid');
          }
          return val;
        }

        _toValue (val) {
          return this._fromValue(val);
        }
      }

      let opts = {logicalTypes: {'even-integer': EvenIntType}};
      let t = Type.forSchema({type: 'long', logicalType: 'even-integer'}, opts);
      assert(t.isValid(2));
      assert(!t.isValid(3));
      assert(!t.isValid('abc'));
      assert.equal(t.fromBuffer(Buffer.from([4])), 2);
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
      assert.throws(() => { t.clone(3); });
      assert.throws(() => { t.fromString('5'); });
      assert.throws(() => { t.toBuffer(3); });
      assert.throws(() => { t.fromBuffer(Buffer.from([2])); });
    });

    test('inside unwrapped union', () => {
      let t = types.Type.forSchema(
        [
          'null',
          {type: 'long', logicalType: 'age'},
          {type: 'string', logicalType: 'date'}
        ],
        {logicalTypes}
      );
      assert(t.isValid(new Date()));
      assert(t.isValid(34));
      assert(t.isValid(null));
      assert(!t.isValid(-123));
    });

    test('inside unwrapped union ambiguous conversion', () => {
      let t = types.Type.forSchema(
        ['long', {type: 'int', logicalType: 'age'}],
        {logicalTypes}
      );
      assert(t.isValid(-34));
      assert.throws(() => { t.isValid(32); }, /ambiguous/);
    });

    test('inside unwrapped union with duplicate underlying type', () => {
      class FooType extends LogicalType {}
      assert.throws(() => {
        types.Type.forSchema([
          'int',
          {type: 'int', logicalType: 'foo'}
        ], {logicalTypes: {foo: FooType}, wrapUnions: false});
      }, /duplicate/);
    });

    test('inside wrapped union', () => {
      class EvenIntType extends LogicalType {
        _fromValue (val) {
          if (val !== (val | 0) || val % 2) {
            throw new Error('invalid');
          }
          return val;
        }

        _toValue (val) {
          return this._fromValue(val);
        }
      }

      let t = types.Type.forSchema(
        [{type: 'int', logicalType: 'even'}],
        {logicalTypes: {even: EvenIntType}, wrapUnions: true}
      );
      assert(t.isValid({int: 2}));
      assert(!t.isValid({int: 3}));
    });

    test('of records inside wrapped union', () => {
      class PassThroughType extends LogicalType {
        _fromValue (val) { return val; }
        _toValue (val) { return val; }
      }

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
    suite('union logical types', () => {

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
      class UnwrappedUnionType extends LogicalType {
        _fromValue (val) {
          return val === null ? null : val[Object.keys(val)[0]];
        }

        _toValue (any) {
          return this.getUnderlyingType().clone(any, {wrapUnions: true});
        }
      }

      test('unwrapped', () => {

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

      test('unwrapped with nested logical types', () => {

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

      test('optional', () => {

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
        class OptionalType extends LogicalType {
          constructor (schema, opts) {
            super(schema, opts);
            let type = this.getUnderlyingType().getTypes()[1];
            this.name = type.getName(true);
          }

          _fromValue (val) {
            return val === null ? null : val[this.name];
          }

          _toValue (any) {
            if (any === null) {
              return null;
            }
            let obj = {};
            obj[this.name] = any;
            return obj;
          }
        }

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

  suite('Type.forSchema', () => {

    test('null type', () => {
      assert.throws(() => { Type.forSchema(null); });
    });

    test('unknown types', () => {
      assert.throws(() => { Type.forSchema('a'); });
      assert.throws(() => { Type.forSchema({type: 'b'}); });
    });

    test('namespaced type', () => {
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

    test('namespace scope', () => {
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

    test('namespace reset', () => {
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

    test('namespace reset with qualified name', () => {
      let type = Type.forSchema({
        type: 'record',
        name: 'earth.Human',
        namespace: '',
        fields: [{name: 'id', type: {type: 'fixed', name: 'Id', size: 2}}]
      });
      assert.equal(type.name, 'earth.Human');
      assert.equal(type.fields[0].type.name, 'Id');
    });

    test('absolute reference', () => {
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

    test('wrapped primitive', () => {
      let type = Type.forSchema({
        type: 'record',
        name: 'Person',
        fields: [{name: 'nothing', type: {type: 'null'}}]
      });
      assert.strictEqual(type.fields[0].type.constructor, builtins.NullType);
    });

    test('fromBuffer truncated', () => {
      let type = Type.forSchema('int');
      assert.throws(() => {
        type.fromBuffer(Buffer.from([128]));
      });
    });

    test('fromBuffer bad resolver', () => {
      let type = Type.forSchema('int');
      assert.throws(() => {
        type.fromBuffer(Buffer.from([0]), 123, {});
      });
    });

    test('fromBuffer trailing', () => {
      let type = Type.forSchema('int');
      assert.throws(() => {
        type.fromBuffer(Buffer.from([0, 2]));
      });
    });

    test('fromBuffer trailing with resolver', () => {
      let type = Type.forSchema('int');
      let resolver = type.createResolver(Type.forSchema(['int']));
      assert.equal(type.fromBuffer(Buffer.from([0, 2]), resolver), 1);
    });

    test('toBuffer', () => {
      let type = Type.forSchema('int');
      assert.throws(() => { type.toBuffer('abc'); });
      assert.doesNotThrow(() => { type.toBuffer(123); });
    });

    test('toBuffer and resize', () => {
      let type = Type.forSchema('string');
      assert.deepEqual(type.toBuffer('\x01', 1), Buffer.from([2, 1]));
    });

    test('type hook', () => {
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

    test('type hook invalid return value', () => {
      assert.throws(() => {
        Type.forSchema({type: 'int'}, {typeHook: hook});
      });

      function hook() { return 'int'; }
    });

    test('type hook for aliases', () => {
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
      let opts = {typeHook: hook, registry: {}};
      Type.forSchema(a1, opts);
      assert.deepEqual(Object.keys(opts.registry), ['R1', 'R2']);

      function hook(name, opts) {
        if (name === 'R2') {
          return Type.forSchema(a2, opts);
        }
      }
    });

    test('fingerprint', () => {
      let t = Type.forSchema('int');
      let buf = Buffer.from('ef524ea1b91e73173d938ade36c1db32', 'hex');
      assert.deepEqual(t.fingerprint('md5'), buf);
      assert.deepEqual(t.fingerprint(), buf);
    });

    test('getSchema default', () => {
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

    test('invalid unwrapped union default', () => {
      assert.throws(() => {
        Type.forSchema({
          name: 'Person',
          type: 'record',
          fields: [
            {name: 'id', type: ['null', 'int'], 'default': 2}
          ]
        }, {wrapUnions: false});
      }, /invalid "null"/);
    });

    test('anonymous types', () => {
      let t = Type.forSchema({
        type: 'record',
        fields: [{name: 'foo', type: 'int'}]
      });
      assert.strictEqual(t.name, undefined);
      assert.strictEqual(t.branchName, 'record');
      assert(t.isValid({foo: 3}));
      assert.throws(() => {
        Type.forSchema({name: '', type: 'record', fields: []});
      });
    });

    test('auto union wrapping', () => {
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

    test('union projection', () => {
      const Dog = {
        type: 'record',
        name: 'Dog',
        fields: [
          { type: 'string', name: 'bark' }
        ],
      };
      const Cat = {
        type: 'record',
        name: 'Cat',
        fields: [
          { type: 'string', name: 'meow' }
        ],
      };
      const animalTypes = [Dog, Cat];

      let callsToWrapUnions = 0;
      const wrapUnions = (types) => {
        callsToWrapUnions++;
        assert.deepEqual(types.map((t) => t.name), ['Dog', 'Cat']);
        return (animal) => {
          const animalType = ((animal) => {
            if ('bark' in animal) {
              return 'Dog';
            } else if ('meow' in animal) {
              return 'Cat';
            }
            throw new Error('Unknown animal');
          })(animal);
          return types.indexOf(types.find((type) => type.name === animalType));
        };
      };

      // Ambiguous, but we have a projection function
      const Animal = Type.forSchema(animalTypes, { wrapUnions });
      Animal.toBuffer({ meow: '🐈' });
      assert.equal(callsToWrapUnions, 1);
      assert.throws(() => Animal.toBuffer({ snap: '🐊' }), /Unknown animal/);
    });

    test('union projection with fallback', () => {
      let t = Type.forSchema({
        type: 'record',
        fields: [
          {name: 'wrapped', type: ['int', 'double' ]}, // Ambiguous.
        ]
      }, {wrapUnions: () => undefined });
      assert(Type.isType(t.field('wrapped').type, 'union:wrapped'));
    });

    test('invalid wrap unions option', () => {
      assert.throws(() => {
        Type.forSchema('string', {wrapUnions: 'FOO'});
      }, /invalid wrap unions option/);
      assert.throws(() => {
        Type.forSchema('string', {wrapUnions: 123});
      }, /invalid wrap unions option/);
    });

  });

  suite('fromString', () => {

    test('int', () => {
      let t = Type.forSchema('int');
      assert.equal(t.fromString('2'), 2);
      assert.throws(() => { t.fromString('"a"'); });
    });

    test('string', () => {
      let t = Type.forSchema('string');
      assert.equal(t.fromString('"2"'), '2');
      assert.throws(() => { t.fromString('a'); });
    });

    test('coerce buffers', () => {
      let t = Type.forSchema({
        name: 'Ids',
        type: 'record',
        fields: [{name: 'id1', type: {name: 'Id1', type: 'fixed', size: 2}}]
      });
      let o = {id1: Buffer.from([0, 1])};
      let s = '{"id1": "\\u0000\\u0001"}';
      let c = t.fromString(s);
      assert.deepEqual(c, o);
      assert(c instanceof t.getRecordConstructor());
    });

  });

  suite('toString', () => {

    test('int', () => {
      let t = Type.forSchema('int');
      assert.equal(t.toString(2), '2');
      assert.throws(() => { t.toString('a'); });
    });

  });

  suite('resolve', () => {

    test('non type', () => {
      let t = Type.forSchema({type: 'map', values: 'int'});
      let obj = {type: 'map', values: 'int'};
      assert.throws(() => { t.createResolver(obj); });
    });

    test('union to valid wrapped union', () => {
      let t1 = Type.forSchema(['int', 'string']);
      let t2 = Type.forSchema(['null', 'string', 'long'], {wrapUnions: true});
      let resolver = t2.createResolver(t1);
      let buf = t1.toBuffer(12);
      assert.deepEqual(t2.fromBuffer(buf, resolver), {'long': 12});
    });

    test('union to invalid union', () => {
      let t1 = Type.forSchema(['int', 'string']);
      let t2 = Type.forSchema(['null', 'long']);
      assert.throws(() => { t2.createResolver(t1); });
    });

    test('wrapped union to non union', () => {
      let t1 = Type.forSchema(['int', 'long'], {wrapUnions: true});
      let t2 = Type.forSchema('long');
      let resolver = t2.createResolver(t1);
      let buf = t1.toBuffer({'int': 12});
      assert.equal(t2.fromBuffer(buf, resolver), 12);
      buf = Buffer.from([4, 0]);
      assert.throws(() => { t2.fromBuffer(buf, resolver); });
    });

    test('union to non union', () => {
      let t1 = Type.forSchema(['bytes', 'string']);
      let t2 = Type.forSchema('bytes');
      let resolver = t2.createResolver(t1);
      let buf = t1.toBuffer('\x01\x02');
      assert.deepEqual(t2.fromBuffer(buf, resolver), Buffer.from([1, 2]));
    });

    test('union to invalid non union', () => {
      let t1 = Type.forSchema(['int', 'long'], {wrapUnions: true});
      let t2 = Type.forSchema('int');
      assert.throws(() => { t2.createResolver(t1); });
    });

    test('anonymous types', () => {
      let t1 = Type.forSchema({type: 'fixed', size: 2});
      let t2 = Type.forSchema(
        {type: 'fixed', size: 2, namespace: 'foo', aliases: ['Id']}
      );
      let t3 = Type.forSchema({type: 'fixed', size: 2, name: 'foo.Id'});
      assert.throws(() => { t1.createResolver(t3); });
      assert.doesNotThrow(() => { t2.createResolver(t3); });
      assert.doesNotThrow(() => { t3.createResolver(t1); });
    });

    test('ignore namespaces', () => {
      let t1 = Type.forSchema({type: 'fixed', name: 'foo.Two', size: 2});
      let t2 = Type.forSchema(
        {type: 'fixed', size: 2, name: 'bar.Deux', aliases: ['bar.Two']}
      );
      assert.throws(() => { t1.createResolver(t2); });
      assert.doesNotThrow(() => {
        t2.createResolver(t1, {ignoreNamespaces: true});
      });
      let t3 = Type.forSchema({type: 'fixed', size: 2, name: 'Two'});
      assert.throws(() => { t3.createResolver(t1); });
      assert.doesNotThrow(() => {
        t3.createResolver(t1, {ignoreNamespaces: true});
      });
    });

  });

  suite('type references', () => {

    test('null', () => {
      assert.throws(() => { Type.forSchema(null); }, /did you mean/);
    });

    test('existing', () => {
      let type = Type.forSchema({
        type: 'record',
        name: 'Person',
        fields: [{name: 'so', type: 'Person'}]
      });
      assert.strictEqual(type, type.fields[0].type);
    });

    test('namespaced', () => {
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

    test('namespaced global', () => {
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

    test('redefining', () => {
      assert.throws(() => {
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

    test('missing', () => {
      assert.throws(() => {
        Type.forSchema({
          type: 'record',
          name: 'Person',
          fields: [{name: 'so', type: 'Friend'}]
        });
      });
    });

    test('redefining primitive', () => {
      assert.throws( // Unqualified.
        () => { Type.forSchema({type: 'fixed', name: 'int', size: 2}); }
      );
      assert.throws( // Qualified.
        () => {
          Type.forSchema({type: 'fixed', name: 'int', size: 2, namespace: 'a'});
        }
      );
    });

    test('aliases', () => {
      let type = Type.forSchema({
        type: 'record',
        name: 'Person',
        namespace: 'a',
        aliases: ['Human', 'b.Being'],
        fields: [{name: 'age', type: 'int'}]
      });
      assert.deepEqual(type.aliases, ['a.Human', 'b.Being']);
    });

    test('invalid', () => {
      // Name.
      assert.throws(() => {
        Type.forSchema({type: 'fixed', name: 'ID$', size: 3});
      });
      // Namespace.
      assert.throws(() => {
        Type.forSchema({type: 'fixed', name: 'ID', size: 3, namespace: '1a'});
      });
      // Qualified.
      assert.throws(() => {
        Type.forSchema({type: 'fixed', name: 'a.2.ID', size: 3});
      });
    });

    test('anonymous types', () => {
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

  suite('decode', () => {

    test('long valid', () => {
      let t = Type.forSchema('long');
      let buf = Buffer.from([0, 128, 2, 0]);
      let res = t.decode(buf, 1);
      assert.deepEqual(res, {value: 128, offset: 3});
    });

    test('bytes invalid', () => {
      let t = Type.forSchema('bytes');
      let buf = Buffer.from([4, 1]);
      let res = t.decode(buf, 0);
      assert.deepEqual(res, {value: undefined, offset: -1});
    });

  });

  suite('encode', () => {

    test('int valid', () => {
      let t = Type.forSchema('int');
      let buf = Buffer.alloc(2);
      buf.fill(0);
      let n = t.encode(5, buf, 1);
      assert.equal(n, 2);
      assert.deepEqual(buf, Buffer.from([0, 10]));
    });

    test('too short', () => {
      let t = Type.forSchema('string');
      let buf = Buffer.alloc(1);
      let n = t.encode('\x01\x02', buf, 0);
      assert.equal(n, -2);
    });

    test('invalid', () => {
      let t = Type.forSchema('float');
      let buf = Buffer.alloc(2);
      assert.throws(() => { t.encode('hi', buf, 0); });
    });

    test('invalid offset', () => {
      let t = Type.forSchema('string');
      let buf = Buffer.alloc(2);
      assert.throws(() => { t.encode('hi', buf, -1); });
    });

  });

  suite('inspect', () => {

    test('type', () => {
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

    test('resolver', () => {
      let t1 = Type.forSchema('int');
      let t2 = Type.forSchema('double');
      let resolver = t2.createResolver(t1);
      assert.equal(resolver.inspect(), '<Resolver>');
    });

  });

  test('equals', () => {
    let t1 = Type.forSchema('int');
    let t2 = Type.forSchema('int');
    assert(t1.equals(t2));
    assert(t2.equals(t1));
    assert(!t1.equals(Type.forSchema('long')));
    assert(!t1.equals(null));
  });

  test('equals strict', () => {
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

  test('documentation', () => {
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

  test('isType', () => {
    let t = Type.forSchema('int');
    assert(types.Type.isType(t));
    assert(types.Type.isType(t, 'int'));
    assert(!types.Type.isType());
    assert(!types.Type.isType('int'));
    assert(!types.Type.isType(() => {}));
  });

  test('reset', () => {
    types.Type.__reset(0);
    let t = Type.forSchema('string');
    let buf = t.toBuffer('\x01');
    assert.deepEqual(buf, Buffer.from([2, 1]));
  });

  suite('forTypes', () => {

    let combine = Type.forTypes;

    test('empty', () => {
      assert.throws(() => { combine([]); });
    });

    test('numbers', () => {
      let t1 = Type.forSchema('int');
      let t2 = Type.forSchema('long');
      let t3 = Type.forSchema('float');
      let t4 = Type.forSchema('double');
      assert.strictEqual(combine([t1, t2]), t2);
      assert.strictEqual(combine([t1, t2, t3, t4]), t4);
      assert.strictEqual(combine([t3, t2]), t3);
      assert.strictEqual(combine([t2]), t2);
    });

    test('string & int', () => {
      let t1 = Type.forSchema('int');
      let t2 = Type.forSchema('string');
      assertUnionsEqual(combine([t1, t2]), Type.forSchema(['int', 'string']));
    });

    test('records & maps', () => {
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

    test('arrays', () => {
      let t1 = Type.forSchema({type: 'array', items: 'null'});
      let t2 = Type.forSchema({type: 'array', items: 'int'});
      let t3 = combine([t1, t2]);
      assertUnionsEqual(t3.getItemsType(), Type.forSchema(['null', 'int']));
    });

    test('field single default', () => {
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

    test('field multiple types default', () => {
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

    test('missing fields no null default', () => {
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

    test('logical types', () => {
      class EvenType extends LogicalType {
        _fromValue (val) { return 2 * val; }
        _toValue (any) {
          if (any === (any | 0) && any % 2 === 0) {
            return any / 2;
          }
        }
      }

      class OddType extends LogicalType {
        _fromValue (val) { return 2 * val + 1; }
        _toValue (any) {
          if (any === (any | 0) && any % 2 === 1) {
            return any / 2;
          }
        }
      }

      let opts = {logicalTypes: {even: EvenType, odd: OddType}};

      let t1 = Type.forSchema({type: 'int', logicalType: 'even'}, opts);
      let t2 = Type.forSchema({type: 'long', logicalType: 'odd'}, opts);
      assertUnionsEqual(combine([t1, t2]), Type.forSchema([t1, t2]));
      assert.throws(() => { combine([t1, t1]); });
    });

    test('invalid wrapped union', () => {
      let t1 = Type.forSchema(['int'], {wrapUnions: true});
      let t2 = Type.forSchema('string');
      assert.throws(() => { combine([t1, t2]); }, /cannot combine/);
    });

    test('error while creating wrapped union', () => {
      let opts = {typeHook: hook, wrapUnions: false};
      let t1 = Type.forSchema(['int'], {wrapUnions: true});
      let t2 = Type.forSchema(['string'], {wrapUnions: true});
      assert.throws(() => { combine([t1, t2], opts); }, /foo/);
      assert(!opts.wrapUnions);

      function hook() { throw new Error('foo'); }
    });

    test('inconsistent wrapped union', () => {
      let t1 = Type.forSchema(
        [{type: 'fixed', name: 'Id', size: 2}],
        {wrapUnions: true}
      );
      let t2 = Type.forSchema(
        [{type: 'fixed', name: 'Id', size: 3}],
        {wrapUnions: true}
      );
      assert.throws(() => { combine([t1, t2]); }, /inconsistent/);
    });

    test('valid wrapped unions', () => {
      let opts = {wrapUnions: true};
      let t1 = Type.forSchema(['int', 'string', 'null'], opts);
      let t2 = Type.forSchema(['null', 'long'], opts);
      assertUnionsEqual(
        combine([t1, t2]),
        Type.forSchema(['int', 'long', 'string', 'null'], opts)
      );
    });

    test('valid unwrapped unions', () => {
      let t1 = Type.forSchema(['int', 'string', 'null']);
      let t2 = Type.forSchema(['null', 'long']);
      assertUnionsEqual(
        combine([t1, t2]),
        Type.forSchema(['long', 'string', 'null'])
      );
    });

    test('buffers', () => {
      let t1 = Type.forSchema({type: 'fixed', size: 2});
      let t2 = Type.forSchema({type: 'fixed', size: 4});
      let t3 = Type.forSchema('bytes');
      assert.strictEqual(combine([t1, t1]), t1);
      assert.strictEqual(combine([t1, t3]), t3);
      assert(combine([t1, t2]).equals(t3));
    });

    test('strings', () => {
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

    test('strings', () => {
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

  suite('forValue', () => {

    let infer = Type.forValue;

    test('numbers', () => {
      assert.equal(infer(1).typeName, 'int');
      assert.equal(infer(1.2).typeName, 'float');
      assert.equal(infer(9007199254740991).typeName, 'double');
    });

    test('function', () => {
      assert.throws(() => { infer(() => {}); });
    });

    test('record', () => {
      let t = infer({b: true, n: null, s: '', f: Buffer.alloc(0)});
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

    test('empty array', () => {
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

    test('value hook', () => {
      let t = infer({foo: 23, bar: 'hi'}, {valueHook: hook});
      assert.equal(t.getField('foo').getType().typeName, 'long');
      assert.equal(t.getField('bar').getType().typeName, 'string');
      assert.throws(() => {
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

    test('type hook array', () => {
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

    test('type hook nested array', () => {
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

  data.forEach((elem) => {
    test('roundtrip', () => {
      let type = new Type(elem.schema);
      elem.valid.forEach((v) => {
        assert(type.isValid(v), '' + v);
        let fn = elem.check || assert.deepEqual;
        fn(type.fromBuffer(type.toBuffer(v)), v);
        fn(type.fromString(type.toString(v), {coerceBuffers: true}), v);
      });
      elem.invalid.forEach((v) => {
        assert(!type.isValid(v), '' + v);
        assert.throws(() => { type.isValid(v, {errorHook: hook}); });
        assert.throws(() => { type.toBuffer(v); });

        function hook() { throw new Error(); }
      });
      let n = 50;
      while (n--) {
        // Run a few times to make sure we cover any branches.
        assert(type.isValid(type.random()));
      }
    });
  });

  test('skip', () => {
    data.forEach((elem) => {
      let fn = elem.check || assert.deepEqual;
      let items = elem.valid;
      if (items.length > 1) {
        let type = new Type(elem.schema);
        let buf = Buffer.alloc(1024);
        let tap = Tap.fromBuffer(buf);
        type._write(tap, items[0]);
        type._write(tap, items[1]);
        tap.pos = 0;
        type._skip(tap);
        fn(type._read(tap), items[1]);
      }
    });
  });

  if (invalidSchemas) {
    test('invalid', () => {
      invalidSchemas.forEach((schema) => {
        assert.throws(() => { new Type(schema); });
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
  let b1 = utils.toMap(t1.types, (t) => { return t.branchName; });
  t2.types.forEach((t) => {
    assert(t.equals(b1[t.branchName]));
  });
}

function supportsErrorStacks() {
  return typeof Error.captureStackTrace == 'function' || Error().stack;
}
