'use strict';

const types = require('../lib/types'),
  utils = require('../lib/utils'),
  assert = require('assert'),
  buffer = require('buffer');

const Buffer = buffer.Buffer;

const LogicalType = types.builtins.LogicalType;
const Tap = utils.Tap;
const Type = types.Type;
const builtins = types.builtins;

suite('types', () => {
  suite('BooleanType', () => {
    const data = [
      {
        valid: [true, false],
        invalid: [null, 'hi', undefined, 1.5, 1e28, 123124123123213],
      },
    ];

    testType(builtins.BooleanType, data);

    test('to JSON', () => {
      const t = new builtins.BooleanType();
      assert.equal(t.toJSON(), 'boolean');
    });

    test('compare buffers', () => {
      const t = new builtins.BooleanType();
      const bt = t.toBuffer(true);
      const bf = t.toBuffer(false);
      assert.equal(t.compareBuffers(bt, bf), 1);
      assert.equal(t.compareBuffers(bf, bt), -1);
      assert.equal(t.compareBuffers(bt, bt), 0);
    });

    test('get name', () => {
      const t = new builtins.BooleanType();
      assert.strictEqual(t.getName(), undefined);
      assert.equal(t.getName(true), 'boolean');
    });
  });

  suite('IntType', () => {
    const data = [
      {
        valid: [1, -3, 12314, 0, 1e9],
        invalid: [null, 'hi', undefined, 1.5, 1e28, 123124123123213],
      },
    ];

    testType(builtins.IntType, data);

    test('toBuffer int', () => {
      const type = Type.forSchema('int');
      assert.equal(type.fromBuffer(Buffer.from([0x80, 0x01])), 64);
      assert(Buffer.from([0]).equals(type.toBuffer(0)));
    });

    test('resolve int > long', () => {
      const intType = Type.forSchema('int');
      const longType = Type.forSchema('long');
      const buf = intType.toBuffer(123);
      assert.equal(
        longType.fromBuffer(buf, longType.createResolver(intType)),
        123
      );
    });

    test('resolve int > U[null, int]', () => {
      const wt = Type.forSchema('int');
      const rt = Type.forSchema(['null', 'int']);
      const buf = wt.toBuffer(123);
      assert.deepEqual(rt.fromBuffer(buf, rt.createResolver(wt)), 123);
    });

    test('resolve int > W[null, int]', () => {
      const wt = Type.forSchema('int');
      const rt = Type.forSchema(['null', 'int'], {wrapUnions: true});
      const buf = wt.toBuffer(123);
      assert.deepEqual(rt.fromBuffer(buf, rt.createResolver(wt)), {int: 123});
    });

    test('resolve int > float', () => {
      const wt = Type.forSchema('int');
      const rt = Type.forSchema('float');
      const buf = wt.toBuffer(123);
      assert.deepEqual(rt.fromBuffer(buf, rt.createResolver(wt)), 123);
    });

    test('resolve int > double', () => {
      const wt = Type.forSchema('int');
      const rt = Type.forSchema('double');
      const n = Math.pow(2, 30) + 1;
      const buf = wt.toBuffer(n);
      assert.deepEqual(rt.fromBuffer(buf, rt.createResolver(wt)), n);
    });

    test('toString', () => {
      assert.equal(Type.forSchema('int').toString(), '"int"');
    });

    test('clone', () => {
      const t = Type.forSchema('int');
      assert.equal(t.clone(123), 123);
      assert.equal(t.clone(123, {}), 123);
      assert.throws(() => {
        t.clone('');
      });
    });

    test('resolve invalid', () => {
      assert.throws(() => {
        getResolver('int', 'long');
      });
    });
  });

  suite('LongType', () => {
    const data = [
      {
        valid: [1, -3, 12314, 4503599627370496],
        invalid: [null, 'hi', undefined, 9007199254740990, 1.3, 1e67],
      },
    ];

    testType(builtins.LongType, data);

    test('resolve invalid', () => {
      assert.throws(() => {
        getResolver('long', 'double');
      });
    });

    test('resolve long > float', () => {
      const t1 = Type.forSchema('long');
      const t2 = Type.forSchema('float');
      const n = 4503599627370496; // Number.MAX_SAFE_INTEGER / 2
      const buf = t1.toBuffer(n);
      const f = t2.fromBuffer(buf, t2.createResolver(t1));
      assert(Math.abs(f - n) / n < 1e-7);
      assert(t2.isValid(f));
    });

    test('precision loss', () => {
      const type = Type.forSchema('long');
      const buf = Buffer.from([0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x20]);
      assert.throws(() => {
        type.fromBuffer(buf);
      });
    });

    test('using missing methods', () => {
      assert.throws(() => {
        builtins.LongType.__with();
      });
    });
  });

  suite('StringType', () => {
    const data = [
      {
        valid: [
          '',
          'hi',
          'ᚠᛇᚻ᛫ᛒᛦᚦ᛫ᚠᚱᚩᚠᚢᚱ᛫ᚠᛁᚱᚪ᛫ᚷᛖᚻᚹᛦᛚᚳᚢᛗ',
          'Sîne klâwen durh die wolken sint geslagen',
          ' ვეპხის ტყაოსანი შოთა რუსთაველი ',
          '私はガラスを食べられます。それは私を傷つけません。',
          'ฉันกินกระจกได้ แต่มันไม่ทำให้ฉันเจ็บ',
          '\ud800\udfff',
        ],
        invalid: [null, undefined, 1, 0],
      },
    ];

    testType(builtins.StringType, data);

    test('fromBuffer string', () => {
      const type = Type.forSchema('string');
      const buf = Buffer.from([0x06, 0x68, 0x69, 0x21]);
      const s = 'hi!';
      assert.equal(type.fromBuffer(buf), s);
      assert(buf.equals(type.toBuffer(s)));
    });

    test('toBuffer string', () => {
      const type = Type.forSchema('string');
      const buf = Buffer.from([0x06, 0x68, 0x69, 0x21]);
      assert(buf.equals(type.toBuffer('hi!', 1)));
    });

    test('resolve string > bytes', () => {
      const stringT = Type.forSchema('string');
      const bytesT = Type.forSchema('bytes');
      const buf = stringT.toBuffer('\x00\x01');
      assert.deepEqual(
        bytesT.fromBuffer(buf, bytesT.createResolver(stringT)),
        Buffer.from([0, 1])
      );
    });

    test('encode resize', () => {
      const t = Type.forSchema('string');
      const s = 'hello';
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
    const data = [
      {
        schema: 'null',
        valid: [null],
        invalid: [0, 1, 'hi', undefined],
      },
    ];

    testType(builtins.NullType, data);

    test('wrap', () => {
      const t = Type.forSchema('null');
      assert.strictEqual(t.wrap(null), null);
    });
  });

  suite('FloatType', () => {
    const data = [
      {
        valid: [1, -3, 123e7],
        invalid: [null, 'hi', undefined],
        check (a, b) {
          assert(floatEquals(a, b));
        },
      },
    ];

    testType(builtins.FloatType, data);

    test('compare buffer', () => {
      const t = Type.forSchema('float');
      const b1 = t.toBuffer(0.5);
      assert.equal(t.compareBuffers(b1, b1), 0);
      const b2 = t.toBuffer(-0.75);
      assert.equal(t.compareBuffers(b1, b2), 1);
      const b3 = t.toBuffer(175);
      assert.equal(t.compareBuffers(b1, b3), -1);
    });

    test('resolver float > float', () => {
      assert.doesNotThrow(() => {
        getResolver('float', 'float');
      });
    });

    test('resolver double > float', () => {
      assert.throws(() => {
        getResolver('float', 'double');
      });
    });

    test('fromString', () => {
      const t = Type.forSchema('float');
      const f = t.fromString('3.1');
      assert(t.isValid(f));
    });

    test('clone from double', () => {
      const t = Type.forSchema('float');
      const d = 3.1;
      let f;
      f = t.clone(d);
      assert(t.isValid(f));
    });
  });

  suite('DoubleType', () => {
    const data = [
      {
        valid: [1, -3.4, 12314e31, 5e37],
        invalid: [null, 'hi', undefined],
        check (a, b) {
          assert(floatEquals(a, b), '' + [a, b]);
        },
      },
    ];

    testType(builtins.DoubleType, data);

    test('resolver string > double', () => {
      assert.throws(() => {
        getResolver('double', 'string');
      });
    });

    test('compare buffer', () => {
      const t = Type.forSchema('double');
      const b1 = t.toBuffer(0.5);
      assert.equal(t.compareBuffers(b1, b1), 0);
      const b2 = t.toBuffer(-0.75);
      assert.equal(t.compareBuffers(b1, b2), 1);
      const b3 = t.toBuffer(175);
      assert.equal(t.compareBuffers(b1, b3), -1);
    });
  });

  suite('BytesType', () => {
    const data = [
      {
        valid: [Buffer.alloc(1), Buffer.from('abc')],
        invalid: [null, 'hi', undefined, 1, 0, -3.5],
      },
    ];

    testType(builtins.BytesType, data);

    test('resolve string > bytes', () => {
      const bytesT = Type.forSchema('bytes');
      const stringT = Type.forSchema('string');
      const buf = Buffer.from([4, 0, 1]);
      assert.deepEqual(
        stringT.fromBuffer(buf, stringT.createResolver(bytesT)),
        '\x00\x01'
      );
    });

    test('clone', () => {
      const t = Type.forSchema('bytes');
      const s = '\x01\x02';
      const buf = Buffer.from(s);
      let clone;
      clone = t.clone(buf);
      assert.deepEqual(clone, buf);
      clone = t.clone(buf, {});
      assert.deepEqual(clone, buf);
      clone[0] = 0;
      assert.equal(buf[0], 1);
      assert.throws(() => {
        t.clone(s);
      });
      clone = t.clone(buf.toJSON(), {coerceBuffers: true});
      assert.deepEqual(clone, buf);
      assert.throws(() => {
        t.clone(1, {coerceBuffers: true});
      });
    });

    test('fromString', () => {
      const t = Type.forSchema('bytes');
      const s = '\x01\x02';
      const buf = Buffer.from(s);
      let clone;
      clone = t.fromString(JSON.stringify(s));
      assert.deepEqual(clone, buf);
      clone = t.fromString(JSON.stringify(s), {});
      assert.deepEqual(clone, buf);
    });

    test('compare', () => {
      const t = Type.forSchema('bytes');
      const b1 = t.toBuffer(Buffer.from([0, 2]));
      assert.equal(t.compareBuffers(b1, b1), 0);
      const b2 = t.toBuffer(Buffer.from([0, 2, 3]));
      assert.equal(t.compareBuffers(b1, b2), -1);
      const b3 = t.toBuffer(Buffer.from([1]));
      assert.equal(t.compareBuffers(b3, b1), 1);
    });
  });

  suite('UnwrappedUnionType', () => {
    const data = [
      {
        name: 'null & string',
        schema: ['null', 'string'],
        valid: [null, 'hi'],
        invalid: [undefined, {string: 'hi'}],
        check: assert.deepEqual,
      },
      {
        name: 'qualified name',
        schema: ['null', {type: 'fixed', name: 'a.B', size: 2}],
        valid: [null, Buffer.alloc(2)],
        invalid: [{'a.B': Buffer.alloc(2)}],
        check: assert.deepEqual,
      },
      {
        name: 'array int',
        schema: ['int', {type: 'array', items: 'int'}],
        valid: [1, [1, 3]],
        invalid: [null, 'hi', {array: [2]}],
        check: assert.deepEqual,
      },
      {
        name: 'null',
        schema: ['null'],
        valid: [null],
        invalid: [{array: ['a']}, [4], 'null'],
        check: assert.deepEqual,
      },
    ];

    const schemas = [
      {},
      [],
      ['null', 'null'],
      ['int', 'long'],
      ['fixed', 'bytes'],
      [{name: 'Letter', type: 'enum', symbols: ['A', 'B']}, 'string'],
      ['null', {type: 'map', values: 'int'}, {type: 'map', values: 'long'}],
      ['null', ['int', 'string']],
    ];

    testType(builtins.UnwrappedUnionType, data, schemas);

    test('getTypes', () => {
      const t = new builtins.UnwrappedUnionType(['null', 'int']);
      const ts = t.getTypes();
      assert(ts[0].equals(Type.forSchema('null')));
      assert(ts[1].equals(Type.forSchema('int')));
    });

    test('getTypeName', () => {
      const t = new builtins.UnwrappedUnionType(['null', 'int']);
      assert.strictEqual(t.getName(), undefined);
      assert.strictEqual(t.getName(true), undefined);
      assert.equal(t.typeName, 'union:unwrapped');
    });

    test('invalid read', () => {
      const type = new builtins.UnwrappedUnionType(['null', 'int']);
      assert.throws(() => {
        type.fromBuffer(Buffer.from([4]));
      });
    });

    test('missing bucket write', () => {
      const type = new builtins.UnwrappedUnionType(['null', 'int']);
      assert.throws(() => {
        type.toBuffer('hi');
      });
    });

    test('invalid bucket write', () => {
      const type = new builtins.UnwrappedUnionType(['null', 'int']);
      assert.throws(() => {
        type.toBuffer(2.5);
      });
    });

    test('fromString', () => {
      const type = new builtins.UnwrappedUnionType(['null', 'int']);
      assert.strictEqual(type.fromString('null'), null);
      assert.deepEqual(type.fromString('{"int": 48}'), 48);
      assert.throws(() => {
        type.fromString('48');
      });
      assert.throws(() => {
        type.fromString('{"long": 48}');
      });
    });

    test('toString', () => {
      const type = new builtins.UnwrappedUnionType(['null', 'int']);
      assert.strictEqual(type.toString(null), 'null');
      assert.deepEqual(type.toString(48), '{"int":48}');
      assert.throws(() => {
        type.toString(2.5);
      });
    });

    test('non wrapped write', () => {
      const type = new builtins.UnwrappedUnionType(['null', 'int']);
      assert.deepEqual(type.toBuffer(23), Buffer.from([2, 46]));
      assert.deepEqual(type.toBuffer(null), Buffer.from([0]));
    });

    test('coerce buffers', () => {
      const type = new builtins.UnwrappedUnionType(['null', 'bytes']);
      const obj = {type: 'Buffer', data: [1, 2]};
      assert.throws(() => {
        type.clone(obj);
      });
      assert.deepEqual(
        type.clone(obj, {coerceBuffers: true}),
        Buffer.from([1, 2])
      );
      assert.deepEqual(type.clone(null, {coerceBuffers: true}), null);
    });

    test('wrapped write', () => {
      const type = new builtins.UnwrappedUnionType(['null', 'int']);
      assert.throws(() => {
        type.toBuffer({int: 1});
      });
    });

    test('to JSON', () => {
      const type = new builtins.UnwrappedUnionType(['null', 'int']);
      assert.equal(JSON.stringify(type), '["null","int"]');
      assert.equal(type.inspect(), '<UnwrappedUnionType ["null","int"]>');
    });

    test('resolve int to [string, long]', () => {
      const t1 = Type.forSchema('int');
      const t2 = new builtins.UnwrappedUnionType(['string', 'long']);
      const a = t2.createResolver(t1);
      const buf = t1.toBuffer(23);
      assert.deepEqual(t2.fromBuffer(buf, a), 23);
    });

    test('resolve null to [null, int]', () => {
      const t1 = Type.forSchema('null');
      const t2 = new builtins.UnwrappedUnionType(['null', 'int']);
      const a = t2.createResolver(t1);
      assert.deepEqual(t2.fromBuffer(Buffer.alloc(0), a), null);
    });

    test('resolve [string, int] to unwrapped [float, bytes]', () => {
      const t1 = new builtins.WrappedUnionType(['string', 'int']);
      const t2 = new builtins.UnwrappedUnionType(['float', 'bytes']);
      const a = t2.createResolver(t1);
      let buf;
      buf = t1.toBuffer({string: 'hi'});
      assert.deepEqual(t2.fromBuffer(buf, a), Buffer.from('hi'));
      buf = t1.toBuffer({int: 1});
      assert.deepEqual(t2.fromBuffer(buf, a), 1);
    });

    test('clone', () => {
      const t = new builtins.UnwrappedUnionType([
        'null',
        {type: 'map', values: 'int'},
      ]);
      const o = {int: 1};
      assert.strictEqual(t.clone(null), null);
      let c;
      c = t.clone(o);
      assert.deepEqual(c, o);
      c = t.clone(o, {});
      assert.deepEqual(c, o);
      c.int = 2;
      assert.equal(o.int, 1);
      assert.throws(() => {
        t.clone([]);
      });
      assert.throws(() => {
        t.clone([], {});
      });
      assert.throws(() => {
        t.clone(undefined);
      });
    });

    test('invalid null', () => {
      const t = new builtins.UnwrappedUnionType(['string', 'int']);
      assert.throws(() => {
        t.fromString(null);
      }, /invalid/);
    });

    test('invalid multiple keys', () => {
      const t = new builtins.UnwrappedUnionType(['null', 'int']);
      const o = {int: 2};
      assert.equal(t.fromString(JSON.stringify(o)), 2);
      o.foo = 3;
      assert.throws(() => {
        t.fromString(JSON.stringify(o));
      });
    });

    test('clone named type', () => {
      const t = Type.forSchema(
        {
          name: 'Person',
          type: 'record',
          fields: [
            {name: 'id1', type: {name: 'an.Id', type: 'fixed', size: 1}},
            {name: 'id2', type: ['null', 'an.Id']},
          ],
        },
        {wrapUnions: false}
      );
      const b = Buffer.from([0]);
      const o = {id1: b, id2: b};
      assert.deepEqual(t.clone(o), o);
    });

    test('compare buffers', () => {
      const t = new builtins.UnwrappedUnionType(['null', 'double']);
      const b1 = t.toBuffer(null);
      assert.equal(t.compareBuffers(b1, b1), 0);
      const b2 = t.toBuffer(4);
      assert.equal(t.compareBuffers(b2, b1), 1);
      assert.equal(t.compareBuffers(b1, b2), -1);
      const b3 = t.toBuffer(6);
      assert.equal(t.compareBuffers(b3, b2), 1);
    });

    test('compare', () => {
      let t;
      t = new builtins.UnwrappedUnionType(['null', 'int']);
      assert.equal(t.compare(null, 3), -1);
      assert.equal(t.compare(null, null), 0);
      assert.throws(() => {
        t.compare('hi', 2);
      });
      assert.throws(() => {
        t.compare(null, 'hey');
      });
    });

    test('wrap', () => {
      const t = new builtins.UnwrappedUnionType(['null', 'double']);
      assert.throws(() => {
        t.wrap(1.0);
      }, /directly/);
    });
  });

  suite('WrappedUnionType', () => {
    const data = [
      {
        name: 'null & string',
        schema: ['null', 'string'],
        valid: [null, {string: 'hi'}],
        invalid: ['null', undefined, {string: 1}],
        check: assert.deepEqual,
      },
      {
        name: 'qualified name',
        schema: ['null', {type: 'fixed', name: 'a.B', size: 2}],
        valid: [null, {'a.B': Buffer.alloc(2)}],
        invalid: [Buffer.alloc(2)],
        check: assert.deepEqual,
      },
      {
        name: 'array int',
        schema: ['int', {type: 'array', items: 'int'}],
        valid: [{int: 1}, {array: [1, 3]}],
        invalid: [null, 2, {array: ['a']}, [4], 2],
        check: assert.deepEqual,
      },
      {
        name: 'null',
        schema: ['null'],
        valid: [null],
        invalid: [{array: ['a']}, [4], 'null'],
        check: assert.deepEqual,
      },
    ];

    const schemas = [
      {},
      [],
      ['null', 'null'],
      ['null', {type: 'map', values: 'int'}, {type: 'map', values: 'long'}],
      ['null', ['int', 'string']],
    ];

    testType(builtins.WrappedUnionType, data, schemas);

    test('getTypes', () => {
      const t = Type.forSchema(['null', 'int']);
      const ts = t.types;
      assert(ts[0].equals(Type.forSchema('null')));
      assert(ts[1].equals(Type.forSchema('int')));
    });

    test('get branch type', () => {
      const type = new builtins.WrappedUnionType(['null', 'int']);
      const buf = type.toBuffer({int: 48});
      const branchType = type.fromBuffer(buf).constructor.type;
      assert(branchType instanceof builtins.IntType);
    });

    test('missing name write', () => {
      const type = new builtins.WrappedUnionType(['null', 'int']);
      assert.throws(() => {
        type.toBuffer({b: 'a'});
      });
    });

    test('read invalid index', () => {
      const type = new builtins.WrappedUnionType(['null', 'int']);
      const buf = Buffer.from([1, 0]);
      assert.throws(() => {
        type.fromBuffer(buf);
      });
    });

    test('non wrapped write', () => {
      const type = new builtins.WrappedUnionType(['null', 'int']);
      assert.throws(() => {
        type.toBuffer(1, true);
      }, Error);
    });

    test('to JSON', () => {
      const type = new builtins.WrappedUnionType(['null', 'int']);
      assert.equal(JSON.stringify(type), '["null","int"]');
    });

    test('resolve int to [long, int]', () => {
      const t1 = Type.forSchema('int');
      const t2 = new builtins.WrappedUnionType(['long', 'int']);
      const a = t2.createResolver(t1);
      const buf = t1.toBuffer(23);
      assert.deepEqual(t2.fromBuffer(buf, a), {long: 23});
    });

    test('resolve null to [null, int]', () => {
      const t1 = Type.forSchema('null');
      const t2 = new builtins.WrappedUnionType(['null', 'int']);
      const a = t2.createResolver(t1);
      assert.deepEqual(t2.fromBuffer(Buffer.alloc(0), a), null);
    });

    test('resolve [string, int] to [long, bytes]', () => {
      const t1 = new builtins.WrappedUnionType(['string', 'int']);
      const t2 = new builtins.WrappedUnionType(['long', 'bytes']);
      const a = t2.createResolver(t1);
      let buf;
      buf = t1.toBuffer({string: 'hi'});
      assert.deepEqual(t2.fromBuffer(buf, a), {bytes: Buffer.from('hi')});
      buf = t1.toBuffer({int: 1});
      assert.deepEqual(t2.fromBuffer(buf, a), {long: 1});
    });

    test('resolve unwrapped [string, int] to [long, bytes]', () => {
      const t1 = new builtins.UnwrappedUnionType(['string', 'int']);
      const t2 = new builtins.WrappedUnionType(['long', 'bytes']);
      const a = t2.createResolver(t1);
      let buf;
      buf = t1.toBuffer('hi');
      assert.deepEqual(t2.fromBuffer(buf, a), {bytes: Buffer.from('hi')});
      buf = t1.toBuffer(1);
      assert.deepEqual(t2.fromBuffer(buf, a), {long: 1});
    });

    test('clone', () => {
      const t = new builtins.WrappedUnionType(['null', 'int']);
      const o = {int: 1};
      assert.strictEqual(t.clone(null), null);
      let c;
      c = t.clone(o);
      assert.deepEqual(c, o);
      c = t.clone(o, {});
      assert.deepEqual(c, o);
      c.int = 2;
      assert.equal(o.int, 1);
      assert.throws(() => {
        t.clone([]);
      });
      assert.throws(() => {
        t.clone([], {});
      });
      assert.throws(() => {
        t.clone(undefined);
      });
    });

    test('clone and wrap', () => {
      const t = new builtins.WrappedUnionType(['string', 'int']);
      let o;
      o = t.clone('hi', {wrapUnions: true});
      assert.deepEqual(o, {string: 'hi'});
      o = t.clone(3, {wrapUnions: true});
      assert.deepEqual(o, {int: 3});
      assert.throws(() => {
        t.clone(null, {wrapUnions: 2});
      });
    });

    test('unwrap', () => {
      const t = new builtins.WrappedUnionType(['string', 'int']);
      const v = t.clone({string: 'hi'});
      assert.equal(v.unwrap(), 'hi');
    });

    test('invalid multiple keys', () => {
      const t = new builtins.WrappedUnionType(['null', 'int']);
      const o = {int: 2};
      assert(t.isValid(o));
      o.foo = 3;
      assert(!t.isValid(o));
    });

    test('clone multiple keys', () => {
      const t = new builtins.WrappedUnionType(['null', 'int']);
      const o = {int: 2, foo: 3};
      assert.throws(() => {
        t.clone(o);
      });
      assert.throws(() => {
        t.clone(o, {});
      });
    });

    test('clone qualify names', () => {
      const t = Type.forSchema(
        {
          name: 'Person',
          type: 'record',
          fields: [
            {name: 'id1', type: {name: 'an.Id', type: 'fixed', size: 1}},
            {name: 'id2', type: ['null', 'an.Id']},
          ],
        },
        {wrapUnions: true}
      );
      const b = Buffer.from([0]);
      const o = {id1: b, id2: {Id: b}};
      const c = {id1: b, id2: {'an.Id': b}};
      assert.throws(() => {
        t.clone(o, {});
      });
      assert.deepEqual(t.clone(o, {qualifyNames: true}), c);
    });

    test('clone invalid qualified names', () => {
      const t = Type.forSchema(
        {
          name: 'Person',
          type: 'record',
          fields: [
            {name: 'id1', type: {name: 'Id', type: 'fixed', size: 1}},
            {name: 'id2', type: ['null', 'Id']},
          ],
        },
        {wrapUnions: true}
      );
      const b = Buffer.from([0]);
      const o = {id1: b, id2: {'an.Id': b}};
      assert.throws(() => {
        t.clone(o);
      });
      assert.throws(() => {
        t.clone(o, {});
      });
    });

    test('compare buffers', () => {
      const t = new builtins.WrappedUnionType(['null', 'double']);
      const b1 = t.toBuffer(null);
      assert.equal(t.compareBuffers(b1, b1), 0);
      const b2 = t.toBuffer({double: 4});
      assert.equal(t.compareBuffers(b2, b1), 1);
      assert.equal(t.compareBuffers(b1, b2), -1);
      const b3 = t.toBuffer({double: 6});
      assert.equal(t.compareBuffers(b3, b2), 1);
    });

    test('compare', () => {
      let t;
      t = new builtins.WrappedUnionType(['null', 'int']);
      assert.equal(t.compare(null, {int: 3}), -1);
      assert.equal(t.compare(null, null), 0);
      t = new builtins.WrappedUnionType(['int', 'float']);
      assert.equal(t.compare({int: 2}, {float: 0.5}), -1);
      assert.equal(t.compare({int: 20}, {int: 5}), 1);
    });

    test('isValid hook', () => {
      const t = new builtins.WrappedUnionType(['null', 'int']);
      const paths = [];
      assert(t.isValid(null, {errorHook: hook}));
      assert(t.isValid({int: 1}, {errorHook: hook}));
      assert(!paths.length);
      assert(!t.isValid({int: 'hi'}, {errorHook: hook}));
      assert.deepEqual(paths, [['int']]);

      function hook(path) {
        paths.push(path);
      }
    });

    // via https://github.com/mtth/avsc/pull/469
    test('synthetic constructor', () => {
      const name = 'Foo';
      const type = types.Type.forSchema([
        'null',
        {
          type: 'record',
          name: `test.${name}`,
          fields: [{name: 'id', type: 'string'}],
        },
      ]);

      const data = {id: 'abc'};
      const roundtripped = type.fromBuffer(type.toBuffer(data));
      assert.equal(roundtripped.constructor.name, name);
      assert.deepEqual(roundtripped, data);
    });
  });

  suite('EnumType', () => {
    const data = [
      {
        name: 'single symbol',
        schema: {name: 'Foo', symbols: ['HI']},
        valid: ['HI'],
        invalid: ['HEY', null, undefined, 0],
      },
      {
        name: 'number-ish as symbol',
        schema: {name: 'Foo', symbols: ['HI', 'A0']},
        valid: ['HI', 'A0'],
        invalid: ['HEY', null, undefined, 0, 'a0'],
      },
    ];

    const schemas = [
      {name: 'Foo', symbols: []},
      {name: 'Foo'},
      {name: 'G', symbols: ['0']},
    ];

    testType(builtins.EnumType, data, schemas);

    test('get full name', () => {
      const t = Type.forSchema({
        type: 'enum',
        symbols: ['A', 'B'],
        name: 'Letter',
        namespace: 'latin',
      });
      assert.equal(t.name, 'latin.Letter');
      assert.equal(t.branchName, 'latin.Letter');
    });

    test('get aliases', () => {
      const t = Type.forSchema({
        type: 'enum',
        symbols: ['A', 'B'],
        name: 'Letter',
        namespace: 'latin',
        aliases: ['Character', 'alphabet.Letter'],
      });
      const aliases = t.getAliases();
      assert.deepEqual(aliases, ['latin.Character', 'alphabet.Letter']);
      aliases.push('Char');
      assert.equal(t.getAliases().length, 3);
    });

    test('get symbols', () => {
      const t = Type.forSchema({
        type: 'enum',
        symbols: ['A', 'B'],
        name: 'Letter',
      });
      const symbols = t.getSymbols();
      assert.deepEqual(symbols, ['A', 'B']);
    });

    test('duplicate symbol', () => {
      assert.throws(() => {
        Type.forSchema({type: 'enum', symbols: ['A', 'B', 'A'], name: 'B'});
      });
    });

    test('missing name', () => {
      const schema = {type: 'enum', symbols: ['A', 'B']};
      const t = Type.forSchema(schema);
      assert.strictEqual(t.getName(), undefined);
      assert.strictEqual(t.getName(true), 'enum');
      assert.throws(() => {
        Type.forSchema(schema, {noAnonymousTypes: true});
      });
    });

    test('write invalid', () => {
      const type = Type.forSchema({type: 'enum', symbols: ['A'], name: 'a'});
      assert.throws(() => {
        type.toBuffer('B');
      });
    });

    test('read invalid index', () => {
      const type = new builtins.EnumType({
        type: 'enum',
        symbols: ['A'],
        name: 'a',
      });
      const buf = Buffer.from([2]);
      assert.throws(() => {
        type.fromBuffer(buf);
      });
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
        const obj = {type: 'enum', name, symbols};
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
      const wt = new builtins.EnumType({name: 'W', symbols: ['A', 'B']});
      const rt = new builtins.EnumType({
        name: 'W',
        symbols: ['D', 'A', 'C'],
        default: 'D',
      });
      const resolver = rt.createResolver(wt);
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
      const t = Type.forSchema({
        type: 'enum',
        name: 'Foo',
        symbols: ['bar', 'baz'],
      });
      assert.equal(t.clone('bar'), 'bar');
      assert.equal(t.clone('bar', {}), 'bar');
      assert.throws(() => {
        t.clone('BAR');
      });
      assert.throws(() => {
        t.clone(null);
      });
    });

    test('compare buffers', () => {
      const t = Type.forSchema({
        type: 'enum',
        name: 'Foo',
        symbols: ['bar', 'baz'],
      });
      const b1 = t.toBuffer('bar');
      const b2 = t.toBuffer('baz');
      assert.equal(t.compareBuffers(b1, b1), 0);
      assert.equal(t.compareBuffers(b2, b1), 1);
    });

    test('compare', () => {
      const t = Type.forSchema({type: 'enum', name: 'Foo', symbols: ['b', 'a']});
      assert.equal(t.compare('b', 'a'), -1);
      assert.equal(t.compare('a', 'a'), 0);
    });
  });

  suite('FixedType', () => {
    const data = [
      {
        name: 'size 1',
        schema: {name: 'Foo', size: 2},
        valid: [Buffer.from([1, 2]), Buffer.from([2, 3])],
        invalid: ['HEY', null, undefined, 0, Buffer.alloc(1), Buffer.alloc(3)],
        check (a, b) {
          assert(Buffer.compare(a, b) === 0);
        },
      },
    ];

    const invalidSchemas = [
      {name: 'Foo', size: NaN},
      {name: 'Foo', size: -2},
      {name: 'Foo'},
      {},
    ];

    testType(builtins.FixedType, data, invalidSchemas);

    test('get full name', () => {
      const t = Type.forSchema({
        type: 'fixed',
        size: 2,
        name: 'Id',
        namespace: 'id',
      });
      assert.equal(t.getName(), 'id.Id');
      assert.equal(t.getName(true), 'id.Id');
    });

    test('get aliases', () => {
      const t = Type.forSchema({
        type: 'fixed',
        size: 3,
        name: 'Id',
      });
      const aliases = t.getAliases();
      assert.deepEqual(aliases, []);
      aliases.push('ID');
      assert.equal(t.getAliases().length, 1);
    });

    test('get size', () => {
      const t = Type.forSchema({type: 'fixed', size: 5, name: 'Id'});
      assert.equal(t.getSize(), 5);
    });

    test('get zero size', () => {
      const t = Type.forSchema({type: 'fixed', size: 0, name: 'Id'});
      assert.equal(t.getSize(), 0);
    });

    test('resolve', () => {
      const t1 = new builtins.FixedType({name: 'Id', size: 4});
      let t2 = new builtins.FixedType({name: 'Id', size: 4});
      assert.doesNotThrow(() => {
        t2.createResolver(t1);
      });
      t2 = new builtins.FixedType({name: 'Id2', size: 4});
      assert.throws(() => {
        t2.createResolver(t1);
      });
      t2 = new builtins.FixedType({name: 'Id2', size: 4, aliases: ['Id']});
      assert.doesNotThrow(() => {
        t2.createResolver(t1);
      });
      t2 = new builtins.FixedType({name: 'Id2', size: 5, aliases: ['Id']});
      assert.throws(() => {
        t2.createResolver(t1);
      });
    });

    test('clone', () => {
      const t = new builtins.FixedType({name: 'Id', size: 2});
      const s = '\x01\x02';
      const buf = Buffer.from(s);
      let clone;
      clone = t.clone(buf);
      assert.deepEqual(clone, buf);
      clone = t.clone(buf, {});
      assert.deepEqual(clone, buf);
      clone[0] = 0;
      assert.equal(buf[0], 1);
      assert.throws(() => {
        t.clone(s);
      });
      assert.throws(() => {
        t.clone(s, {});
      });
      clone = t.clone(buf.toJSON(), {coerceBuffers: true});
      assert.deepEqual(clone, buf);
      assert.throws(() => {
        t.clone(1, {coerceBuffers: true});
      });
      assert.throws(() => {
        t.clone(Buffer.from([2]));
      });
    });

    test('fromString', () => {
      const t = new builtins.FixedType({name: 'Id', size: 2});
      const s = '\x01\x02';
      const buf = Buffer.from(s);
      const clone = t.fromString(JSON.stringify(s));
      assert.deepEqual(clone, buf);
    });

    test('compare buffers', () => {
      const t = Type.forSchema({type: 'fixed', name: 'Id', size: 2});
      const b1 = Buffer.from([1, 2]);
      assert.equal(t.compareBuffers(b1, b1), 0);
      const b2 = Buffer.from([2, 2]);
      assert.equal(t.compareBuffers(b1, b2), -1);
    });
  });

  suite('MapType', () => {
    const data = [
      {
        name: 'int',
        schema: {values: 'int'},
        valid: [{one: 1}, {two: 2, o: 0}],
        invalid: [1, {o: null}, [], undefined, {o: 'hi'}, {1: '', 2: 3}, ''],
        check: assert.deepEqual,
      },
      {
        name: 'enum',
        schema: {values: {type: 'enum', name: 'a', symbols: ['A', 'B']}},
        valid: [{a: 'A'}, {a: 'A', b: 'B'}, {}],
        invalid: [{o: 'a'}, {1: 'A', 2: 'b'}, {a: 3}],
        check: assert.deepEqual,
      },
      {
        name: 'array of string',
        schema: {values: {type: 'array', items: 'string'}},
        valid: [{a: []}, {a: ['A'], b: ['B', '']}, {}],
        invalid: [{o: 'a', b: []}, {a: [1, 2]}, {a: {b: ''}}],
        check: assert.deepEqual,
      },
    ];

    const schemas = [{}, {values: ''}, {values: {type: 'array'}}];

    testType(builtins.MapType, data, schemas);

    test('get values type', () => {
      const t = new builtins.MapType({type: 'map', values: 'int'});
      assert(t.getValuesType().equals(Type.forSchema('int')));
    });

    test('write int', () => {
      const t = new builtins.MapType({type: 'map', values: 'int'});
      const buf = t.toBuffer({'\x01': 3, '\x02': 4});
      assert.deepEqual(buf, Buffer.from([4, 2, 1, 6, 2, 2, 8, 0]));
    });

    test('read long', () => {
      const t = new builtins.MapType({type: 'map', values: 'long'});
      const buf = Buffer.from([4, 2, 1, 6, 2, 2, 8, 0]);
      assert.deepEqual(t.fromBuffer(buf), {'\x01': 3, '\x02': 4});
    });

    test('read with sizes', () => {
      const t = new builtins.MapType({type: 'map', values: 'int'});
      const buf = Buffer.from([1, 6, 2, 97, 2, 0]);
      assert.deepEqual(t.fromBuffer(buf), {a: 1});
    });

    test('skip', () => {
      const v1 = Type.forSchema({
        name: 'Foo',
        type: 'record',
        fields: [
          {name: 'map', type: {type: 'map', values: 'int'}},
          {name: 'val', type: 'int'},
        ],
      });
      const v2 = Type.forSchema({
        name: 'Foo',
        type: 'record',
        fields: [{name: 'val', type: 'int'}],
      });
      const b1 = Buffer.from([2, 2, 97, 2, 0, 6]); // Without sizes.
      const b2 = Buffer.from([1, 6, 2, 97, 2, 0, 6]); // With sizes.
      const resolver = v2.createResolver(v1);
      assert.deepEqual(v2.fromBuffer(b1, resolver), {val: 3});
      assert.deepEqual(v2.fromBuffer(b2, resolver), {val: 3});
    });

    test('resolve int > long', () => {
      const t1 = new builtins.MapType({type: 'map', values: 'int'});
      const t2 = new builtins.MapType({type: 'map', values: 'long'});
      const resolver = t2.createResolver(t1);
      const obj = {one: 1, two: 2};
      const buf = t1.toBuffer(obj);
      assert.deepEqual(t2.fromBuffer(buf, resolver), obj);
    });

    test('resolve double > double', () => {
      const t = new builtins.MapType({type: 'map', values: 'double'});
      const resolver = t.createResolver(t);
      const obj = {one: 1, two: 2};
      const buf = t.toBuffer(obj);
      assert.deepEqual(t.fromBuffer(buf, resolver), obj);
    });

    test('resolve invalid', () => {
      const t1 = new builtins.MapType({type: 'map', values: 'int'});
      let t2 = new builtins.MapType({type: 'map', values: 'string'});
      assert.throws(() => {
        t2.createResolver(t1);
      });
      t2 = new builtins.ArrayType({type: 'array', items: 'string'});
      assert.throws(() => {
        t2.createResolver(t1);
      });
    });

    test('resolve fixed', () => {
      const t1 = Type.forSchema({
        type: 'map',
        values: {name: 'Id', type: 'fixed', size: 2},
      });
      const t2 = Type.forSchema({
        type: 'map',
        values: {
          name: 'Id2',
          aliases: ['Id'],
          type: 'fixed',
          size: 2,
        },
      });
      const resolver = t2.createResolver(t1);
      const obj = {one: Buffer.from([1, 2])};
      const buf = t1.toBuffer(obj);
      assert.deepEqual(t2.fromBuffer(buf, resolver), obj);
    });

    test('clone', () => {
      const t = new builtins.MapType({type: 'map', values: 'int'});
      const o = {one: 1, two: 2};
      let c;
      c = t.clone(o);
      assert.deepEqual(c, o);
      c = t.clone(o, {});
      assert.deepEqual(c, o);
      c.one = 3;
      assert.equal(o.one, 1);
      assert.throws(() => {
        t.clone(undefined);
      });
      assert.throws(() => {
        t.clone(undefined, {});
      });
    });

    test('clone coerce buffers', () => {
      const t = new builtins.MapType({type: 'map', values: 'bytes'});
      const o = {one: {type: 'Buffer', data: [1]}};
      assert.throws(() => {
        t.clone(o, {});
      });
      assert.throws(() => {
        t.clone(o);
      });
      const c = t.clone(o, {coerceBuffers: true});
      assert.deepEqual(c, {one: Buffer.from([1])});
    });

    test('compare buffers', () => {
      const t = new builtins.MapType({type: 'map', values: 'bytes'});
      const b1 = t.toBuffer({});
      assert.throws(() => {
        t.compareBuffers(b1, b1);
      });
    });

    test('isValid hook', () => {
      const t = new builtins.MapType({type: 'map', values: 'int'});
      const o = {one: 1, two: 'deux', three: null, four: 4};
      const errs = {};
      assert(!t.isValid(o, {errorHook: hook}));
      assert.deepEqual(errs, {two: 'deux', three: null});

      function hook(path, obj, type) {
        assert.strictEqual(type, t.getValuesType());
        assert.equal(path.length, 1);
        errs[path[0]] = obj;
      }
    });

    test('getName', () => {
      const t = new builtins.MapType({type: 'map', values: 'int'});
      assert.strictEqual(t.getName(), undefined);
      assert.strictEqual(t.getName(true), 'map');
    });
  });

  suite('ArrayType', () => {
    const data = [
      {
        name: 'int',
        schema: {items: 'int'},
        valid: [[1, 3, 4], []],
        invalid: [1, {o: null}, undefined, ['a'], [true]],
        check: assert.deepEqual,
      },
    ];

    const schemas = [{}, {items: ''}];

    testType(builtins.ArrayType, data, schemas);

    test('get items type', () => {
      const t = new builtins.ArrayType({type: 'array', items: 'int'});
      assert(t.getItemsType().equals(Type.forSchema('int')));
    });

    test('read with sizes', () => {
      const t = new builtins.ArrayType({type: 'array', items: 'int'});
      const buf = Buffer.from([1, 2, 2, 0]);
      assert.deepEqual(t.fromBuffer(buf), [1]);
    });

    test('skip', () => {
      const v1 = Type.forSchema({
        name: 'Foo',
        type: 'record',
        fields: [
          {name: 'array', type: {type: 'array', items: 'int'}},
          {name: 'val', type: 'int'},
        ],
      });
      const v2 = Type.forSchema({
        name: 'Foo',
        type: 'record',
        fields: [{name: 'val', type: 'int'}],
      });
      const b1 = Buffer.from([2, 2, 0, 6]); // Without sizes.
      const b2 = Buffer.from([1, 2, 2, 0, 6]); // With sizes.
      const resolver = v2.createResolver(v1);
      assert.deepEqual(v2.fromBuffer(b1, resolver), {val: 3});
      assert.deepEqual(v2.fromBuffer(b2, resolver), {val: 3});
    });

    test('resolve string items to bytes items', () => {
      const t1 = new builtins.ArrayType({type: 'array', items: 'string'});
      const t2 = new builtins.ArrayType({type: 'array', items: 'bytes'});
      const resolver = t2.createResolver(t1);
      const obj = ['\x01\x02'];
      const buf = t1.toBuffer(obj);
      assert.deepEqual(t2.fromBuffer(buf, resolver), [Buffer.from([1, 2])]);
    });

    test('resolve invalid', () => {
      const t1 = new builtins.ArrayType({type: 'array', items: 'string'});
      let t2 = new builtins.ArrayType({type: 'array', items: 'long'});
      assert.throws(() => {
        t2.createResolver(t1);
      });
      t2 = new builtins.MapType({type: 'map', values: 'string'});
      assert.throws(() => {
        t2.createResolver(t1);
      });
    });

    test('clone', () => {
      const t = new builtins.ArrayType({type: 'array', items: 'int'});
      const o = [1, 2];
      let c;
      c = t.clone(o);
      assert.deepEqual(c, o);
      c = t.clone(o, {});
      assert.deepEqual(c, o);
      c.one = 3;
      assert.equal(o[0], 1);
      assert.throws(() => {
        t.clone({});
      });
      assert.throws(() => {
        t.clone({}, {});
      });
    });

    test('clone coerce buffers', () => {
      const t = Type.forSchema({
        type: 'array',
        items: {type: 'fixed', name: 'Id', size: 2},
      });
      const o = [{type: 'Buffer', data: [1, 2]}];
      assert.throws(() => {
        t.clone(o);
      });
      assert.throws(() => {
        t.clone(o, {});
      });
      const c = t.clone(o, {coerceBuffers: true});
      assert.deepEqual(c, [Buffer.from([1, 2])]);
    });

    test('compare buffers', () => {
      const t = Type.forSchema({type: 'array', items: 'int'});
      assert.equal(t.compareBuffers(t.toBuffer([]), t.toBuffer([])), 0);
      assert.equal(t.compareBuffers(t.toBuffer([1, 2]), t.toBuffer([])), 1);
      assert.equal(t.compareBuffers(t.toBuffer([1]), t.toBuffer([1, -1])), -1);
      assert.equal(t.compareBuffers(t.toBuffer([1]), t.toBuffer([2])), -1);
      assert.equal(t.compareBuffers(t.toBuffer([1, 2]), t.toBuffer([1])), 1);
    });

    test('compare', () => {
      const t = Type.forSchema({type: 'array', items: 'int'});
      assert.equal(t.compare([], []), 0);
      assert.equal(t.compare([], [-1]), -1);
      assert.equal(t.compare([1], [1]), 0);
      assert.equal(t.compare([2], [1, 2]), 1);
    });

    test('isValid hook invalid array', () => {
      const t = Type.forSchema({type: 'array', items: 'int'});
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
      const t = Type.forSchema({type: 'array', items: 'int'});
      const paths = [];
      assert(!t.isValid([0, 3, 'hi', 5, 'hey'], {errorHook: hook}));
      assert.deepEqual(paths, [['2'], ['4']]);

      function hook(path, obj, type) {
        assert.strictEqual(type, t.getItemsType());
        assert.equal(typeof obj, 'string');
        paths.push(path);
      }
    });

    test('isValid hook reentrant', () => {
      const t = new builtins.ArrayType({
        items: new builtins.ArrayType({items: 'int'}),
      });
      const a1 = [
        [1, 3],
        ['a', 2, 'c'],
        [3, 'b'],
      ];
      const a2 = [[1, 3]];
      const paths = [];
      assert(!t.isValid(a1, {errorHook: hook}));
      assert.deepEqual(paths, [
        ['1', '0'],
        ['1', '2'],
        ['2', '1'],
      ]);

      function hook(path, any, type, val) {
        paths.push(path);
        assert.strictEqual(val, a1);
        assert(t.isValid(a2, {errorHook: hook}));
      }
    });

    test('round-trip multi-block array', () => {
      const tap = Tap.withCapacity(64);
      tap.writeLong(2);
      tap.writeString('hi');
      tap.writeString('hey');
      tap.writeLong(1);
      tap.writeString('hello');
      tap.writeLong(0);
      const t = new builtins.ArrayType({items: 'string'});
      assert.deepEqual(t.fromBuffer(tap.subarray(0, tap.pos)), [
        'hi',
        'hey',
        'hello',
      ]);
    });
  });

  suite('RecordType', () => {
    const data = [
      {
        name: 'union field null and string with default',
        schema: {
          type: 'record',
          name: 'a',
          fields: [{name: 'b', type: ['null', 'string'], default: null}],
        },
        valid: [],
        invalid: [],
        check: assert.deepEqual,
      },
    ];

    const schemas = [
      {type: 'record', name: 'a', fields: ['null', 'string']},
      {type: 'record', name: 'a', fields: [{type: ['null', 'string']}]},
      {
        type: 'record',
        name: 'a',
        fields: [{name: 'b', type: ['null', 'string'], default: 'a'}],
      },
      {type: 'record', name: 'a', fields: {type: 'int', name: 'age'}},
    ];

    testType(builtins.RecordType, data, schemas);

    test('duplicate field names', () => {
      assert.throws(() => {
        Type.forSchema({
          type: 'record',
          name: 'Person',
          fields: [
            {name: 'age', type: 'int'},
            {name: 'age', type: 'float'},
          ],
        });
      });
    });

    test('invalid name', () => {
      const schema = {
        name: 'foo-bar.Bar',
        type: 'record',
        fields: [{name: 'id', type: 'int'}],
      };
      assert.throws(() => {
        Type.forSchema(schema);
      }, /invalid name/);
    });

    test('reserved name', () => {
      const schema = {
        name: 'case',
        type: 'record',
        fields: [{name: 'id', type: 'int'}],
      };
      const Case = Type.forSchema(schema).recordConstructor;
      const c = new Case(123);
      assert.equal(c.id, 123);
    });

    test('default constructor', () => {
      const type = Type.forSchema({
        type: 'record',
        name: 'Person',
        fields: [{name: 'age', type: 'int', default: 25}],
      });
      const Person = type.getRecordConstructor();
      const p = new Person();
      assert.equal(p.age, 25);
      assert.strictEqual(p.constructor, Person);
    });

    test('wrap values', () => {
      const type = Type.forSchema({
        namespace: 'id',
        type: 'record',
        name: 'Id',
        fields: [{name: 'n', type: 'int'}],
      });
      const Id = type.recordConstructor;
      const id = new Id(12);
      const wrappedId = {'id.Id': id};
      assert.deepEqual(type.wrap(id), wrappedId);
      assert.deepEqual(id.wrap(), wrappedId);
    });

    test('default check & write', () => {
      const type = Type.forSchema({
        type: 'record',
        name: 'Person',
        fields: [
          {name: 'age', type: 'int', default: 25},
          {name: 'name', type: 'string', default: '\x01'},
        ],
      });
      assert.deepEqual(type.toBuffer({}), Buffer.from([50, 2, 1]));
    });

    test('fixed string default', () => {
      const s = '\x01\x04';
      const b = Buffer.from(s);
      const type = Type.forSchema({
        type: 'record',
        name: 'Object',
        fields: [
          {
            name: 'id',
            type: {type: 'fixed', size: 2, name: 'Id'},
            default: s,
          },
        ],
      });
      const obj = new (type.getRecordConstructor())();
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
              default: Buffer.from([0]),
            },
          ],
        });
      });
    });

    test('union invalid default', () => {
      assert.throws(() => {
        Type.forSchema({
          type: 'record',
          name: 'Person',
          fields: [{name: 'name', type: ['null', 'string'], default: ''}],
        });
      }, /incompatible.*first branch/);
    });

    test('record default', () => {
      const d = {street: null, zip: 123};
      const schema = {
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
                {name: 'zip', type: ['int', 'string']},
              ],
            },
            default: d,
          },
        ],
      };
      let Person, person;
      // Wrapped
      Person = Type.forSchema(schema, {
        wrapUnions: true,
      }).getRecordConstructor();
      person = new Person();
      assert.deepEqual(person.address, {street: null, zip: {int: 123}});
      // Unwrapped.
      Person = Type.forSchema(schema).getRecordConstructor();
      person = new Person();
      assert.deepEqual(person.address, {street: null, zip: 123});
    });

    test('record keyword field name', () => {
      const type = Type.forSchema({
        type: 'record',
        name: 'Person',
        fields: [{name: 'null', type: 'int'}],
      });
      const Person = type.getRecordConstructor();
      assert.deepEqual(new Person(2), {null: 2});
    });

    test('record isValid', () => {
      const type = Type.forSchema({
        type: 'record',
        name: 'Person',
        fields: [{name: 'age', type: 'int'}],
      });
      const Person = type.getRecordConstructor();
      assert(new Person(20).isValid());
      assert(!new Person().isValid());
      assert(!new Person('a').isValid());
    });

    test('record toBuffer', () => {
      const type = Type.forSchema({
        type: 'record',
        name: 'Person',
        fields: [{name: 'age', type: 'int'}],
      });
      const Person = type.getRecordConstructor();
      assert.deepEqual(new Person(48).toBuffer(), Buffer.from([96]));
      assert.throws(() => {
        new Person().toBuffer();
      });
    });

    test('record compare', () => {
      const P = Type.forSchema({
        type: 'record',
        name: 'Person',
        fields: [
          {name: 'data', type: {type: 'map', values: 'int'}, order: 'ignore'},
          {name: 'age', type: 'int'},
        ],
      }).getRecordConstructor();
      const p1 = new P({}, 1);
      const p2 = new P({}, 2);
      assert.equal(p1.compare(p2), -1);
      assert.equal(p2.compare(p2), 0);
      assert.equal(p2.compare(p1), 1);
    });

    test('Record type', () => {
      const type = Type.forSchema({
        type: 'record',
        name: 'Person',
        fields: [{name: 'age', type: 'int'}],
      });
      const Person = type.getRecordConstructor();
      assert.strictEqual(Person.getType(), type);
    });

    test('mutable defaults', () => {
      const Person = Type.forSchema({
        type: 'record',
        name: 'Person',
        fields: [
          {
            name: 'friends',
            type: {type: 'array', items: 'string'},
            default: [],
          },
        ],
      }).getRecordConstructor();
      const p1 = new Person(undefined);
      assert.deepEqual(p1.friends, []);
      p1.friends.push('ann');
      const p2 = new Person(undefined);
      assert.deepEqual(p2.friends, []);
    });

    test('resolve alias', () => {
      const v1 = Type.forSchema({
        type: 'record',
        name: 'Person',
        fields: [{name: 'name', type: 'string'}],
      });
      const p = v1.random();
      const buf = v1.toBuffer(p);
      const v2 = Type.forSchema({
        type: 'record',
        name: 'Human',
        aliases: ['Person'],
        fields: [{name: 'name', type: 'string'}],
      });
      const resolver = v2.createResolver(v1);
      assert.deepEqual(v2.fromBuffer(buf, resolver), p);
      const v3 = Type.forSchema({
        type: 'record',
        name: 'Human',
        fields: [{name: 'name', type: 'string'}],
      });
      assert.throws(() => {
        v3.createResolver(v1);
      });
    });

    test('resolve alias with namespace', () => {
      const v1 = Type.forSchema({
        type: 'record',
        name: 'Person',
        namespace: 'earth',
        fields: [{name: 'name', type: 'string'}],
      });
      const v2 = Type.forSchema({
        type: 'record',
        name: 'Human',
        aliases: ['Person'],
        fields: [{name: 'name', type: 'string'}],
      });
      assert.throws(() => {
        v2.createResolver(v1);
      });
      const v3 = Type.forSchema({
        type: 'record',
        name: 'Human',
        aliases: ['earth.Person'],
        fields: [{name: 'name', type: 'string'}],
      });
      assert.doesNotThrow(() => {
        v3.createResolver(v1);
      });
    });

    test('resolve skip field', () => {
      const v1 = Type.forSchema({
        type: 'record',
        name: 'Person',
        fields: [
          {name: 'age', type: 'int'},
          {name: 'name', type: 'string'},
        ],
      });
      const p = {age: 25, name: 'Ann'};
      const buf = v1.toBuffer(p);
      const v2 = Type.forSchema({
        type: 'record',
        name: 'Person',
        fields: [{name: 'name', type: 'string'}],
      });
      const resolver = v2.createResolver(v1);
      assert.deepEqual(v2.fromBuffer(buf, resolver), {name: 'Ann'});
    });

    test('resolve new field', () => {
      const v1 = Type.forSchema({
        type: 'record',
        name: 'Person',
        fields: [{name: 'name', type: 'string'}],
      });
      const p = {name: 'Ann'};
      const buf = v1.toBuffer(p);
      const v2 = Type.forSchema({
        type: 'record',
        name: 'Person',
        fields: [
          {name: 'age', type: 'int', default: 25},
          {name: 'name', type: 'string'},
        ],
      });
      const resolver = v2.createResolver(v1);
      assert.deepEqual(v2.fromBuffer(buf, resolver), {name: 'Ann', age: 25});
    });

    test('resolve field with javascript keyword as name', () => {
      const v1 = Type.forSchema({
        type: 'record',
        name: 'Person',
        fields: [{name: 'void', type: 'string'}],
      });
      const p = {void: 'Ann'};
      const buf = v1.toBuffer(p);
      const v2 = Type.forSchema({
        type: 'record',
        name: 'Person',
        fields: [{name: 'void', type: 'string'}],
      });
      const resolver = v2.createResolver(v1);
      assert.deepEqual(v2.fromBuffer(buf, resolver), {void: 'Ann'});
    });

    test('resolve new field no default', () => {
      const v1 = Type.forSchema({
        type: 'record',
        name: 'Person',
        fields: [{name: 'name', type: 'string'}],
      });
      const v2 = Type.forSchema({
        type: 'record',
        name: 'Person',
        fields: [
          {name: 'age', type: 'int'},
          {name: 'name', type: 'string'},
        ],
      });
      assert.throws(() => {
        v2.createResolver(v1);
      });
    });

    test('resolve from recursive schema', () => {
      const v1 = Type.forSchema({
        type: 'record',
        name: 'Person',
        fields: [{name: 'friends', type: {type: 'array', items: 'Person'}}],
      });
      const v2 = Type.forSchema({
        type: 'record',
        name: 'Person',
        fields: [{name: 'age', type: 'int', default: -1}],
      });
      const resolver = v2.createResolver(v1);
      const p1 = {friends: [{friends: []}]};
      const p2 = v2.fromBuffer(v1.toBuffer(p1), resolver);
      assert.deepEqual(p2, {age: -1});
    });

    test('resolve to recursive schema', () => {
      const v1 = Type.forSchema({
        type: 'record',
        name: 'Person',
        fields: [{name: 'age', type: 'int', default: -1}],
      });
      const v2 = Type.forSchema({
        type: 'record',
        name: 'Person',
        fields: [
          {
            name: 'friends',
            type: {type: 'array', items: 'Person'},
            default: [],
          },
        ],
      });
      const resolver = v2.createResolver(v1);
      const p1 = {age: 25};
      const p2 = v2.fromBuffer(v1.toBuffer(p1), resolver);
      assert.deepEqual(p2, {friends: []});
    });

    test('resolve from both recursive schema', () => {
      const v1 = Type.forSchema({
        type: 'record',
        name: 'Person',
        fields: [
          {name: 'friends', type: {type: 'array', items: 'Person'}},
          {name: 'age', type: 'int'},
        ],
      });
      const v2 = Type.forSchema({
        type: 'record',
        name: 'Person',
        fields: [{name: 'friends', type: {type: 'array', items: 'Person'}}],
      });
      const resolver = v2.createResolver(v1);
      const p1 = {friends: [{age: 1, friends: []}], age: 10};
      const p2 = v2.fromBuffer(v1.toBuffer(p1), resolver);
      assert.deepEqual(p2, {friends: [{friends: []}]});
    });

    test('resolve multiple matching aliases', () => {
      const v1 = Type.forSchema({
        type: 'record',
        name: 'Person',
        fields: [
          {name: 'phone', type: 'string'},
          {name: 'number', type: 'string'},
        ],
      });
      const v2 = Type.forSchema({
        type: 'record',
        name: 'Person',
        fields: [{name: 'number', type: 'string', aliases: ['phone']}],
      });
      assert.throws(() => {
        v2.createResolver(v1);
      });
    });

    test('resolve consolidated reads same type', () => {
      const t1 = Type.forSchema({
        type: 'record',
        name: 'Person',
        fields: [{name: 'phone', type: 'int'}],
      });
      const t2 = Type.forSchema({
        type: 'record',
        name: 'Person',
        fields: [
          {name: 'number1', type: 'int', aliases: ['phone']},
          {name: 'number2', type: 'int', aliases: ['phone']},
          {name: 'phone', type: 'int'},
        ],
      });
      const rsv = t2.createResolver(t1);
      const buf = t1.toBuffer({phone: 123});
      assert.deepEqual(t2.fromBuffer(buf, rsv), {
        number1: 123,
        number2: 123,
        phone: 123,
      });
    });

    test('resolve consolidated reads different types', () => {
      const t1 = Type.forSchema({
        type: 'record',
        name: 'Person',
        fields: [{name: 'phone', type: 'int'}],
      });
      const t2 = Type.forSchema({
        type: 'record',
        name: 'Person',
        fields: [
          {name: 'phoneLong', type: 'long', aliases: ['phone']},
          {name: 'phoneDouble', type: 'double', aliases: ['phone']},
          {name: 'phone', type: 'int'},
        ],
      });
      const rsv = t2.createResolver(t1);
      const buf = t1.toBuffer({phone: 123});
      assert.deepEqual(t2.fromBuffer(buf, rsv), {
        phoneLong: 123,
        phoneDouble: 123,
        phone: 123,
      });
    });

    test('getName', () => {
      const t = Type.forSchema({
        type: 'record',
        name: 'Person',
        doc: 'Hi!',
        namespace: 'earth',
        aliases: ['Human'],
        fields: [
          {name: 'friends', type: {type: 'array', items: 'string'}},
          {name: 'age', aliases: ['years'], type: {type: 'int'}},
        ],
      });
      assert.strictEqual(t.getName(), 'earth.Person');
      assert.strictEqual(t.getName(true), 'earth.Person');
      assert.equal(t.typeName, 'record');
    });

    test('getSchema', () => {
      const t = Type.forSchema({
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
            default: 0,
            order: 'descending',
          },
        ],
      });
      const schemaStr =
        '{"name":"earth.Person","type":"record","fields":[{"name":"friends","type":{"type":"array","items":"string"}},{"name":"age","type":"int"}]}';
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
            default: 0,
            order: 'descending',
          },
        ],
      });
      assert.equal(t.getSchema({noDeref: true}), 'earth.Person');
    });

    test('getSchema recursive schema', () => {
      const t = Type.forSchema({
        type: 'record',
        name: 'Person',
        namespace: 'earth',
        fields: [{name: 'friends', type: {type: 'array', items: 'Person'}}],
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
      const t = Type.forSchema({
        type: 'record',
        name: 'Person',
        fields: [
          {name: 'age', type: 'int'},
          {name: 'name', type: 'string', default: 'UNKNOWN'},
        ],
      });
      assert.deepEqual(t.fromString('{"age": 23}'), {age: 23, name: 'UNKNOWN'});
      assert.throws(() => {
        t.fromString('{}');
      });
    });

    test('toString record', () => {
      const T = Type.forSchema({
        type: 'record',
        name: 'Person',
        fields: [{name: 'pwd', type: 'bytes'}],
      }).getRecordConstructor();
      const r = new T(Buffer.from([1, 2]));
      assert.equal(r.toString(), T.getType().toString(r));
    });

    test('clone', () => {
      const t = Type.forSchema({
        type: 'record',
        name: 'Person',
        fields: [
          {name: 'age', type: 'int'},
          {name: 'name', type: 'string'},
        ],
      });
      const Person = t.getRecordConstructor();
      const o = {age: 25, name: 'Ann'};
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
      const t = Type.forSchema(
        {
          type: 'record',
          name: 'Person',
          fields: [
            {name: 'id', type: 'int'},
            {name: 'name', type: 'string', default: 'UNKNOWN'},
            {name: 'age', type: ['null', 'int'], default: null},
          ],
        },
        {wrapUnions: true}
      );
      assert.deepEqual(t.clone({id: 1, name: 'Ann'}), {
        id: 1,
        name: 'Ann',
        age: null,
      });
      assert.deepEqual(t.clone({id: 1, name: 'Ann', age: {int: 21}}), {
        id: 1,
        name: 'Ann',
        age: {int: 21},
      });
      assert.deepEqual(t.clone({id: 1, name: 'Ann', age: {int: 21}}, {}), {
        id: 1,
        name: 'Ann',
        age: {int: 21},
      });
      assert.deepEqual(
        t.clone({id: 1, name: 'Ann', age: 21}, {wrapUnions: true}),
        {id: 1, name: 'Ann', age: {int: 21}}
      );
      assert.throws(() => {
        t.clone({});
      });
    });

    test('clone field hook', () => {
      const t = Type.forSchema({
        type: 'record',
        name: 'Person',
        fields: [
          {name: 'age', type: 'int'},
          {name: 'name', type: 'string'},
        ],
      });
      const o = {name: 'Ann', age: 25};
      const c = t.clone(o, {
        fieldHook (f, o, r) {
          assert.strictEqual(r, t);
          return f.type instanceof builtins.StringType ? o.toUpperCase() : o;
        },
      });
      assert.deepEqual(c, {name: 'ANN', age: 25});
    });

    test('clone missing fields', () => {
      const t = Type.forSchema({
        type: 'record',
        name: 'Person',
        fields: [
          {name: 'id', type: 'int'},
          {name: 'name', type: ['null', 'string']},
          {name: 'age', type: ['null', 'int'], default: null},
        ],
      });
      assert.throws(() => {
        t.clone({id: 1});
      }, /invalid/);
      assert.deepEqual(t.clone({id: 1}, {skipMissingFields: true}), {
        id: 1,
        name: undefined,
        age: null,
      });
    });

    test('unwrapped union field default', () => {
      assert.throws(() => {
        Type.forSchema(
          {
            type: 'record',
            name: 'Person',
            fields: [{name: 'name', type: ['null', 'string'], default: 'Bob'}],
          },
          {wrapUnions: false}
        );
      });
      const schema = {
        type: 'record',
        name: 'Person',
        fields: [{name: 'name', type: ['string', 'null'], default: 'Bob'}],
      };
      const t = Type.forSchema(schema, {wrapUnions: false});
      const o = {name: 'Ann'};
      assert.deepEqual(t.clone(o), o);
      assert.deepEqual(t.clone({}), {name: 'Bob'});
      assert.deepEqual(t.toString({}), '{"name":{"string":"Bob"}}');
      assert.deepEqual(t.getSchema({exportAttrs: true}), schema);
    });

    test('wrapped union field default', () => {
      assert.throws(() => {
        Type.forSchema(
          {
            type: 'record',
            name: 'Person',
            fields: [{name: 'name', type: ['null', 'string'], default: 'Bob'}],
          },
          {wrapUnions: true}
        );
      });
      const schema = {
        type: 'record',
        name: 'Person',
        fields: [
          {name: 'name', type: ['string', 'null'], default: 'Bob', doc: ''},
        ],
      };
      const t = Type.forSchema(schema, {wrapUnions: true});
      const o = {name: {string: 'Ann'}};
      assert.deepEqual(t.clone(o), o);
      assert.deepEqual(t.clone({}), {name: {string: 'Bob'}});
      assert.deepEqual(t.getSchema({exportAttrs: true}), schema);
    });

    test('get full name & aliases', () => {
      const t = Type.forSchema({
        type: 'record',
        name: 'Person',
        namespace: 'a',
        fields: [
          {name: 'age', type: 'int'},
          {name: 'name', type: 'string'},
        ],
      });
      assert.equal(t.getName(), 'a.Person');
      assert.deepEqual(t.getAliases(), []);
    });

    test('field getters', () => {
      const t = Type.forSchema({
        type: 'record',
        name: 'Person',
        namespace: 'a',
        fields: [
          {name: 'age', type: 'int'},
          {name: 'name', type: 'string', aliases: ['word'], namespace: 'b'},
        ],
      });
      assert.equal(t.getField('age').getName(), 'age');
      assert.strictEqual(t.getField('foo'), undefined);
      const fields = t.getFields();
      assert.deepEqual(fields[0].getAliases(), []);
      assert.deepEqual(fields[1].getAliases(), ['word']);
      assert.equal(fields[1].getName(), 'name'); // Namespaces are ignored.
      assert(fields[1].getType().equals(Type.forSchema('string')));
    });

    test('field order', () => {
      const t = Type.forSchema({
        type: 'record',
        name: 'Person',
        fields: [{name: 'age', type: 'int'}],
      });
      const field = t.getFields()[0];
      assert.equal(field.order, 'ascending'); // Default.
      assert.equal(field.getOrder(), 'ascending'); // Default.
    });

    test('compare buffers default order', () => {
      const t = Type.forSchema({
        type: 'record',
        name: 'Person',
        fields: [
          {name: 'age', type: 'long'},
          {name: 'name', type: 'string'},
          {name: 'weight', type: 'float'},
        ],
      });
      const b1 = t.toBuffer({age: 20, name: 'Ann', weight: 0.5});
      assert.equal(t.compareBuffers(b1, b1), 0);
      const b2 = t.toBuffer({age: 20, name: 'Bob', weight: 0});
      assert.equal(t.compareBuffers(b1, b2), -1);
      const b3 = t.toBuffer({age: 19, name: 'Carrie', weight: 0});
      assert.equal(t.compareBuffers(b1, b3), 1);
    });

    test('compare buffers custom order', () => {
      const t = Type.forSchema({
        type: 'record',
        name: 'Person',
        fields: [
          {name: 'meta', type: {type: 'map', values: 'int'}, order: 'ignore'},
          {name: 'name', type: 'string', order: 'descending'},
        ],
      });
      const b1 = t.toBuffer({meta: {}, name: 'Ann'});
      assert.equal(t.compareBuffers(b1, b1), 0);
      const b2 = t.toBuffer({meta: {foo: 1}, name: 'Bob'});
      assert.equal(t.compareBuffers(b1, b2), 1);
      const b3 = t.toBuffer({meta: {foo: 0}, name: 'Alex'});
      assert.equal(t.compareBuffers(b1, b3), -1);
    });

    test('compare buffers invalid order', () => {
      assert.throws(() => {
        Type.forSchema({
          type: 'record',
          name: 'Person',
          fields: [{name: 'age', type: 'int', order: 'up'}],
        });
      });
    });

    test('error type', () => {
      const t = Type.forSchema({
        type: 'error',
        name: 'Ouch',
        fields: [{name: 'name', type: 'string'}],
      });
      const E = t.getRecordConstructor();
      const err = new E('MyError');
      assert(err instanceof Error);
    });

    test('error stack field not overwritten', () => {
      const t = Type.forSchema(
        {
          type: 'error',
          name: 'Ouch',
          fields: [
            {name: 'name', type: 'string'},
            {name: 'stack', type: 'string'},
          ],
        },
        {errorStackTraces: true}
      );
      const E = t.recordConstructor;
      const err = new E('MyError', 'my amazing stack');
      assert(err instanceof Error);
      assert(err.stack === 'my amazing stack');
    });

    test('error stack trace', () => {
      const t = Type.forSchema(
        {
          type: 'error',
          name: 'Ouch',
          fields: [
            {name: 'name', type: 'string'},
            {name: 'stack', type: 'string'},
          ],
        },
        {errorStackTraces: true}
      );
      const E = t.recordConstructor;
      const err = new E('MyError');
      assert(err instanceof Error);
      if (supportsErrorStacks()) {
        assert(typeof err.stack === 'string');
      }
    });

    test('no stack trace by default', () => {
      const t = Type.forSchema({
        type: 'error',
        name: 'Ouch',
        fields: [{name: 'name', type: 'string'}],
      });
      const E = t.recordConstructor;
      const err = new E('MyError');
      assert(err instanceof Error);
      assert(err.stack === undefined);
    });

    test('no stack when no matching field', () => {
      const t = Type.forSchema(
        {
          type: 'error',
          name: 'Ouch',
          fields: [{name: 'name', type: 'string'}],
        },
        {errorStackTraces: true}
      );
      const E = t.recordConstructor;
      const err = new E('MyError');
      assert(err instanceof Error);
      assert(err.stack === undefined);
    });

    test('no stack when non-string stack field', () => {
      const t = Type.forSchema(
        {
          type: 'error',
          name: 'Ouch',
          fields: [
            {name: 'name', type: 'string'},
            {name: 'stack', type: 'boolean'},
          ],
        },
        {errorStackTraces: true}
      );
      const E = t.recordConstructor;
      const err = new E('MyError');
      assert(err instanceof Error);
      assert(err.stack === undefined);
    });

    test('anonymous error type', () => {
      assert.doesNotThrow(() => {
        Type.forSchema({
          type: 'error',
          fields: [{name: 'name', type: 'string'}],
        });
      });
    });

    test('resolve error type', () => {
      const t1 = Type.forSchema({
        type: 'error',
        name: 'Ouch',
        fields: [{name: 'name', type: 'string'}],
      });
      const t2 = Type.forSchema({
        type: 'error',
        name: 'OuchAgain',
        aliases: ['Ouch'],
        fields: [{name: 'code', type: 'int', default: -1}],
      });
      const res = t2.createResolver(t1);
      const err1 = t1.random();
      const err2 = t2.fromBuffer(t1.toBuffer(err1), res);
      assert.deepEqual(err2, {code: -1});
    });

    test('isValid hook', () => {
      const t = Type.forSchema({
        type: 'record',
        name: 'Person',
        fields: [
          {name: 'age', type: 'int'},
          {name: 'names', type: {type: 'array', items: 'string'}},
        ],
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
      const t = Type.forSchema({type: 'record', name: 'Person', fields: []});
      assert(t.isValid({}));
    });

    test('isValid no undeclared fields', () => {
      const t = Type.forSchema({
        type: 'record',
        fields: [{name: 'foo', type: 'int'}],
      });
      const obj = {foo: 1, bar: 'bar'};
      assert(t.isValid(obj));
      assert(!t.isValid(obj, {noUndeclaredFields: true}));
      assert(t.isValid({foo: 23}, {noUndeclaredFields: true}));
    });

    test('qualified name namespacing', () => {
      const t = Type.forSchema(
        {
          type: 'record',
          name: '.Foo',
          fields: [
            {name: 'id', type: {type: 'record', name: 'Bar', fields: []}},
          ],
        },
        {namespace: 'bar'}
      );
      assert.equal(t.getField('id').getType().getName(), 'Bar');
    });

    test('omit record methods', () => {
      const t = Type.forSchema(
        {
          type: 'record',
          name: 'Foo',
          fields: [{name: 'id', type: 'string'}],
        },
        {omitRecordMethods: true}
      );
      const Foo = t.recordConstructor;
      assert.strictEqual(Foo.type, undefined);
      const v = t.clone({id: 'abc'});
      assert.strictEqual(v.toBuffer, undefined);
    });
  });

  suite('AbstractLongType', () => {
    const fastLongType = new builtins.LongType();

    suite('unpacked', () => {
      const slowLongType = builtins.LongType.__with({
        fromBuffer (buf) {
          const dv = new DataView(buf.buffer, buf.byteOffset, buf.byteLength);
          const neg = buf[7] >> 7;
          if (neg) {
            // Negative number.
            invert(buf);
          }
          let n = dv.getInt32(0, true) + Math.pow(2, 32) * dv.getInt32(4, true);
          if (neg) {
            invert(buf);
            n = -n - 1;
          }
          return n;
        },
        toBuffer (n) {
          const buf = new Uint8Array(8);
          const dv = new DataView(buf.buffer, buf.byteOffset, buf.byteLength);
          const neg = n < 0;
          if (neg) {
            invert(buf);
            n = -n - 1;
          }
          dv.setInt32(0, n | 0, true);
          const h = (n / Math.pow(2, 32)) | 0;
          dv.setInt32(4, h || (n >= 0 ? 0 : -1), true);
          if (neg) {
            invert(buf);
          }
          return buf;
        },
        isValid (n) {
          return typeof n == 'number' && n % 1 === 0;
        },
        fromJSON (n) {
          return n;
        },
        toJSON (n) {
          return n;
        },
        compare (n1, n2) {
          return n1 === n2 ? 0 : n1 < n2 ? -1 : 1;
        },
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
          const buf = fastLongType.toBuffer(n);
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
        const s = 'hi';
        const errs = [];
        assert(!slowLongType.isValid(s, {errorHook: hook}));
        assert.deepEqual(errs, [s]);
        assert.throws(() => {
          slowLongType.toBuffer(s);
        });

        function hook(path, obj, type) {
          assert.strictEqual(type, slowLongType);
          assert.equal(path.length, 0);
          errs.push(obj);
        }
      });

      test('resolve between long', () => {
        const b = fastLongType.toBuffer(123);
        const fastToSlow = slowLongType.createResolver(fastLongType);
        assert.equal(slowLongType.fromBuffer(b, fastToSlow), 123);
        const slowToFast = fastLongType.createResolver(slowLongType);
        assert.equal(fastLongType.fromBuffer(b, slowToFast), 123);
      });

      test('resolve from int', () => {
        const intType = Type.forSchema('int');
        const b = intType.toBuffer(123);
        const r = slowLongType.createResolver(intType);
        assert.equal(slowLongType.fromBuffer(b, r), 123);
      });

      test('resolve to double and float', () => {
        const b = slowLongType.toBuffer(123);
        const floatType = Type.forSchema('float');
        const doubleType = Type.forSchema('double');
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
      const slowLongType = builtins.LongType.__with(
        {
          fromBuffer (buf) {
            const tap = Tap.fromBuffer(buf);
            return tap.readLong();
          },
          toBuffer (n) {
            const buf = Buffer.alloc(10);
            const tap = Tap.fromBuffer(buf);
            tap.writeLong(n);
            return buf.subarray(0, tap.pos);
          },
          fromJSON (n) {
            return n;
          },
          toJSON (n) {
            return n;
          },
          isValid (n) {
            return typeof n == 'number' && n % 1 === 0;
          },
          compare (n1, n2) {
            return n1 === n2 ? 0 : n1 < n2 ? -1 : 1;
          },
        },
        true
      );

      test('encode', () => {
        [123, -1, 321414, 900719925474090].forEach((n) => {
          assert.deepEqual(slowLongType.toBuffer(n), fastLongType.toBuffer(n));
        });
      });

      test('decode', () => {
        [123, -1, 321414, 900719925474090].forEach((n) => {
          const buf = fastLongType.toBuffer(n);
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
        const t1 = Type.forSchema(
          {
            type: 'record',
            name: 'Foo',
            fields: [{name: 'foo', type: 'long'}],
          },
          {registry: {long: slowLongType}}
        );
        const t2 = Type.forSchema(
          {
            type: 'record',
            name: 'Foo',
            fields: [{name: 'bar', aliases: ['foo'], type: 'long'}],
          },
          {registry: {long: slowLongType}}
        );
        const rsv = t2.createResolver(t1);
        const buf = t1.toBuffer({foo: 2});
        assert.deepEqual(t2.fromBuffer(buf, rsv), {bar: 2});
      });
    });

    test('within unwrapped union', () => {
      const longType = builtins.LongType.__with(
        {
          fromBuffer (buf) {
            return {value: buf};
          },
          toBuffer (obj) {
            return obj.value;
          },
          fromJSON () {
            throw new Error();
          },
          toJSON () {
            throw new Error();
          },
          isValid (obj) {
            return obj && Buffer.isBuffer(obj.value);
          },
          compare () {
            throw new Error();
          },
        },
        true
      );
      const t = Type.forSchema(['null', 'long'], {registry: {long: longType}});
      const v = {value: Buffer.from([4])}; // Long encoding of 2.

      assert(t.isValid(null));
      assert(t.isValid(v));
      assert.deepEqual(t.fromBuffer(t.toBuffer(v)), v);
    });

    test('incomplete buffer', () => {
      // Check that `fromBuffer` doesn't get called.
      const slowLongType = builtins.LongType.__with({
        fromBuffer () {
          throw new Error('no');
        },
        toBuffer: null,
        fromJSON: null,
        toJSON: null,
        isValid: null,
        compare: null,
      });
      const buf = fastLongType.toBuffer(12314);
      assert.deepEqual(slowLongType.decode(buf.subarray(0, 1)), {
        value: undefined,
        offset: -1,
      });
    });
  });

  suite('LogicalType', () => {
    class DateType extends LogicalType {
      constructor(schema, opts) {
        super(schema, opts);
        if (!types.Type.isType(this.getUnderlyingType(), 'long', 'string')) {
          throw new Error('invalid underlying date type');
        }
      }

      _fromValue(val) {
        return new Date(val);
      }

      _toValue(date) {
        if (!(date instanceof Date)) {
          return undefined;
        }
        if (this.getUnderlyingType().typeName === 'long') {
          return +date;
        }
          // String.
          return '' + date;

      }

      _resolve(type) {
        if (types.Type.isType(type, 'long', 'string')) {
          return this._fromValue;
        }
      }
    }

    class AgeType extends LogicalType {
      _fromValue(val) {
        return val;
      }

      _toValue(any) {
        if (typeof any == 'number' && any >= 0) {
          return any;
        }
      }

      _resolve(type) {
        if (types.Type.isType(type, 'logical:age')) {
          return this._fromValue;
        }
      }
    }

    const logicalTypes = {age: AgeType, date: DateType};

    test('valid type', () => {
      const t = Type.forSchema(
        {
          type: 'long',
          logicalType: 'date',
        },
        {logicalTypes}
      );
      assert(t instanceof DateType);
      assert(/<(Date|Logical)Type {.+}>/.test(t.inspect())); // IE.
      assert(t.getUnderlyingType() instanceof builtins.LongType);
      assert(t.isValid(t.random()));
      const d = new Date(123);
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
      const schema = {
        type: 'int',
        logicalType: 'date',
      };
      let t;
      t = Type.forSchema(schema); // Missing.
      assert(t instanceof builtins.IntType);
      t = Type.forSchema(schema, {logicalTypes}); // Invalid.
      assert(t instanceof builtins.IntType);
      assert.throws(() => {
        Type.forSchema(schema, {
          logicalTypes,
          assertLogicalTypes: true,
        });
      });
    });

    test('missing type', () => {
      const t = Type.forSchema(
        {
          type: 'long',
          logicalType: 'date',
        },
        {logicalTypes: {}}
      );
      assert(t.typeName, 'long');
    });

    test('nested types', () => {
      const schema = {
        name: 'Person',
        type: 'record',
        fields: [
          {name: 'age', type: {type: 'int', logicalType: 'age'}},
          {name: 'time', type: {type: 'long', logicalType: 'date'}},
        ],
      };
      const base = Type.forSchema(schema);
      const derived = Type.forSchema(schema, {logicalTypes});
      const fields = derived.getFields();
      const ageType = fields[0].getType();
      ageType.constructor = undefined; // Mimic missing constructor name.
      assert(ageType instanceof AgeType);
      assert.equal(
        ageType.inspect(),
        '<LogicalType {"type":"int","logicalType":"age"}>'
      );
      assert(fields[1].getType() instanceof DateType);
      const date = new Date(Date.now());
      const buf = base.toBuffer({age: 12, time: +date});
      const person = derived.fromBuffer(buf);
      assert.deepEqual(person.age, 12);
      assert.deepEqual(person.time, date);

      const invalid = {age: -1, time: date};
      assert.throws(() => {
        derived.toBuffer(invalid);
      });
      let hasError = false;
      derived.isValid(invalid, {
        errorHook (path, any, type) {
          hasError = true;
          assert.deepEqual(path, ['age']);
          assert.equal(any, -1);
          assert(type instanceof AgeType);
        },
      });
      assert(hasError);
    });

    test('recursive', () => {
      function Person(friends) {
        this.friends = friends || [];
      }

      class PersonType extends LogicalType {
        _fromValue(val) {
          return new Person(val.friends);
        }

        _toValue(val) {
          return val;
        }
      }

      const schema = {
        type: 'record',
        name: 'Person',
        logicalType: 'person',
        fields: [{name: 'friends', type: {type: 'array', items: 'Person'}}],
      };
      const t = Type.forSchema(schema, {logicalTypes: {person: PersonType}});

      const p1 = new Person([new Person()]);
      const buf = t.toBuffer(p1);
      const p2 = t.fromBuffer(buf);
      assert(p2 instanceof Person);
      assert(p2.friends[0] instanceof Person);
      assert.deepEqual(p2, p1);
      assert.deepEqual(t.getSchema({exportAttrs: true}), schema);
    });

    test('recursive dereferencing name', () => {
      class BoxType extends LogicalType {
        _fromValue(val) {
          return val.unboxed;
        }
        _toValue(any) {
          return {unboxed: any};
        }
      }

      const t = Type.forSchema(
        {
          name: 'BoxedMap',
          type: 'record',
          logicalType: 'box',
          fields: [
            {
              name: 'unboxed',
              type: {type: 'map', values: ['string', 'BoxedMap']},
            },
          ],
        },
        {logicalTypes: {box: BoxType}}
      );

      const v = {foo: 'hi', bar: {baz: {}}};
      assert(t.isValid({}));
      assert(t.isValid(v));
      assert.deepEqual(t.fromBuffer(t.toBuffer(v)), v);
    });

    test('resolve underlying > logical', () => {
      const t1 = Type.forSchema({type: 'string'});
      const t2 = Type.forSchema(
        {
          type: 'long',
          logicalType: 'date',
        },
        {logicalTypes}
      );

      const d1 = new Date(Date.now());
      const buf = t1.toBuffer('' + d1);
      const res = t2.createResolver(t1);
      assert.throws(() => {
        t2.createResolver(Type.forSchema('float'));
      });
      const d2 = t2.fromBuffer(buf, res);
      assert.deepEqual('' + d2, '' + d1); // Rounding error on date objects.
    });

    test('resolve logical > underlying', () => {
      const t1 = Type.forSchema(
        {
          type: 'long',
          logicalType: 'date',
        },
        {logicalTypes}
      );
      const t2 = Type.forSchema({type: 'double'}); // Note long > double too.

      const d = new Date(Date.now());
      const buf = t1.toBuffer(d);
      const res = t2.createResolver(t1);
      assert.throws(() => {
        Type.forSchema('int').createResolver(t1);
      });
      assert.equal(t2.fromBuffer(buf, res), +d);
    });

    test('resolve logical type into a schema without the field', () => {
      const t1 = Type.forSchema(
        {
          name: 'Person',
          type: 'record',
          fields: [
            {name: 'age', type: {type: 'int', logicalType: 'age'}},
            {name: 'time', type: {type: 'long', logicalType: 'date'}},
          ],
        },
        {logicalTypes}
      );
      const t2 = Type.forSchema(
        {
          name: 'Person',
          type: 'record',
          fields: [{name: 'age', type: {type: 'int', logicalType: 'age'}}],
        },
        {logicalTypes}
      );

      const buf = t1.toBuffer({age: 12, time: new Date()});

      const res = t2.createResolver(t1);
      const decoded = t2.fromBuffer(buf, res);
      assert.equal(decoded.age, 12);
      assert.equal(decoded.time, undefined);
    });

    test('resolve union of logical > union of logical', () => {
      const t = types.Type.forSchema(
        ['null', {type: 'int', logicalType: 'age'}],
        {logicalTypes, wrapUnions: true}
      );
      const resolver = t.createResolver(t);
      const v = {int: 34};
      assert.deepEqual(t.fromBuffer(t.toBuffer(v), resolver), v);
    });

    test('even integer', () => {
      class EvenIntType extends LogicalType {
        _fromValue(val) {
          if (val !== (val | 0) || val % 2) {
            throw new Error('invalid');
          }
          return val;
        }

        _toValue(val) {
          return this._fromValue(val);
        }
      }

      const opts = {logicalTypes: {'even-integer': EvenIntType}};
      const t = Type.forSchema({type: 'long', logicalType: 'even-integer'}, opts);
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
      assert.throws(() => {
        t.clone(3);
      });
      assert.throws(() => {
        t.fromString('5');
      });
      assert.throws(() => {
        t.toBuffer(3);
      });
      assert.throws(() => {
        t.fromBuffer(Buffer.from([2]));
      });
    });

    test('inside unwrapped union', () => {
      const t = types.Type.forSchema(
        [
          'null',
          {type: 'long', logicalType: 'age'},
          {type: 'string', logicalType: 'date'},
        ],
        {logicalTypes}
      );
      assert(t.isValid(new Date()));
      assert(t.isValid(34));
      assert(t.isValid(null));
      assert(!t.isValid(-123));
    });

    test('inside unwrapped union ambiguous conversion', () => {
      const t = types.Type.forSchema(
        ['long', {type: 'int', logicalType: 'age'}],
        {logicalTypes}
      );
      assert(t.isValid(-34));
      assert.throws(() => {
        t.isValid(32);
      }, /ambiguous/);
    });

    test('inside unwrapped union with duplicate underlying type', () => {
      class FooType extends LogicalType {}
      assert.throws(() => {
        types.Type.forSchema(['int', {type: 'int', logicalType: 'foo'}], {
          logicalTypes: {foo: FooType},
          wrapUnions: false,
        });
      }, /duplicate/);
    });

    test('inside wrapped union', () => {
      class EvenIntType extends LogicalType {
        _fromValue(val) {
          if (val !== (val | 0) || val % 2) {
            throw new Error('invalid');
          }
          return val;
        }

        _toValue(val) {
          return this._fromValue(val);
        }
      }

      const t = types.Type.forSchema([{type: 'int', logicalType: 'even'}], {
        logicalTypes: {even: EvenIntType},
        wrapUnions: true,
      });
      assert(t.isValid({int: 2}));
      assert(!t.isValid({int: 3}));
    });

    test('of records inside wrapped union', () => {
      class PassThroughType extends LogicalType {
        _fromValue(val) {
          return val;
        }
        _toValue(val) {
          return val;
        }
      }

      const t = types.Type.forSchema(
        [
          {
            type: 'record',
            logicalType: 'pt',
            name: 'A',
            fields: [{name: 'a', type: 'int'}],
          },
          {
            type: 'record',
            logicalType: 'pt',
            name: 'B',
            fields: [{name: 'b', type: 'int'}],
          },
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
      const schema = [
        'null',
        {
          name: 'Person',
          type: 'record',
          fields: [
            {name: 'name', type: 'string'},
            {name: 'age', type: ['null', 'int'], default: null},
          ],
        },
      ];

      function createUnionTypeHook(Type) {
        const visited = [];
        return function (schema, opts) {
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
        _fromValue(val) {
          return val === null ? null : val[Object.keys(val)[0]];
        }

        _toValue(any) {
          return this.getUnderlyingType().clone(any, {wrapUnions: true});
        }
      }

      test('unwrapped', () => {
        const t1 = Type.forSchema(schema, {
          typeHook: createUnionTypeHook(UnwrappedUnionType),
          wrapUnions: true,
        });
        const obj = {name: 'Ann', age: 23};
        assert(t1.isValid(obj));
        const buf = t1.toBuffer(obj);
        const t2 = Type.forSchema(schema, {wrapUnions: true});
        assert.deepEqual(t2.fromBuffer(buf), {
          Person: {name: 'Ann', age: {int: 23}},
        });
      });

      test('unwrapped with nested logical types', () => {
        const schema = [
          'null',
          {
            type: 'record',
            name: 'Foo',
            fields: [
              {
                name: 'date',
                type: ['null', {type: 'long', logicalType: 'timestamp-millis'}],
              },
            ],
          },
        ];
        const t1 = Type.forSchema(schema, {
          logicalTypes: {'timestamp-millis': DateType},
          typeHook: createUnionTypeHook(UnwrappedUnionType),
          wrapUnions: true,
        });
        const obj = {date: new Date(1234)};
        assert(t1.isValid(obj));
        const buf = t1.toBuffer(obj);
        const t2 = Type.forSchema(schema, {wrapUnions: true});
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
          constructor(schema, opts) {
            super(schema, opts);
            const type = this.getUnderlyingType().getTypes()[1];
            this.name = type.getName(true);
          }

          _fromValue(val) {
            return val === null ? null : val[this.name];
          }

          _toValue(any) {
            if (any === null) {
              return null;
            }
            const obj = {};
            obj[this.name] = any;
            return obj;
          }
        }

        const t1 = Type.forSchema(schema, {
          typeHook: createUnionTypeHook(OptionalType),
          wrapUnions: true,
        });
        const obj = {name: 'Ann', age: 23};
        assert(t1.isValid(obj));
        const buf = t1.toBuffer(obj);
        const t2 = Type.forSchema(schema, {wrapUnions: true});
        assert.deepEqual(t2.fromBuffer(buf), {
          Person: {name: 'Ann', age: {int: 23}},
        });
      });
    });
  });

  suite('Type.forSchema', () => {
    test('null type', () => {
      assert.throws(() => {
        Type.forSchema(null);
      });
    });

    test('unknown types', () => {
      assert.throws(() => {
        Type.forSchema('a');
      });
      assert.throws(() => {
        Type.forSchema({type: 'b'});
      });
    });

    test('namespaced type', () => {
      const type = Type.forSchema({
        type: 'record',
        name: 'Human',
        namespace: 'earth',
        fields: [
          {
            name: 'id',
            type: {type: 'fixed', name: 'Id', size: 2, namespace: 'all'},
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
              ],
            },
          },
        ],
      });
      assert.equal(type.name, 'earth.Human');
      assert.equal(type.fields[0].type.name, 'all.Id');
      assert.equal(type.fields[1].type.name, 'all.Alien');
    });

    test('namespace scope', () => {
      const type = Type.forSchema({
        type: 'record',
        name: 'Human',
        namespace: 'earth',
        fields: [
          {
            name: 'id1',
            type: {type: 'fixed', name: 'Id', size: 2, namespace: 'all'},
          },
          {
            name: 'id2',
            type: {type: 'fixed', name: 'Id', size: 4},
          },
        ],
      });
      assert.equal(type.name, 'earth.Human');
      assert.equal(type.fields[0].type.name, 'all.Id');
      assert.equal(type.fields[1].type.name, 'earth.Id');
    });

    test('namespace reset', () => {
      const type = Type.forSchema({
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
            type: {type: 'fixed', name: 'Id', size: 4, namespace: ''},
          },
        ],
      });
      assert.equal(type.name, 'earth.Human');
      assert.equal(type.fields[0].type.name, 'earth.Id');
      assert.equal(type.fields[1].type.name, 'Id');
    });

    test('namespace reset with qualified name', () => {
      const type = Type.forSchema({
        type: 'record',
        name: 'earth.Human',
        namespace: '',
        fields: [{name: 'id', type: {type: 'fixed', name: 'Id', size: 2}}],
      });
      assert.equal(type.name, 'earth.Human');
      assert.equal(type.fields[0].type.name, 'Id');
    });

    test('absolute reference', () => {
      const type = Type.forSchema({
        type: 'record',
        namespace: 'earth',
        name: 'Human',
        fields: [
          {
            name: 'id1',
            type: {type: 'fixed', name: 'Id', namespace: '', size: 2},
          },
          {name: 'id2', type: '.Id'}, // Not `earth.Id`.
          {name: 'id3', type: '.string'}, // Also works with primitives.
        ],
      });
      assert.equal(type.name, 'earth.Human');
      assert.equal(type.fields[1].type.name, 'Id');
    });

    test('wrapped primitive', () => {
      const type = Type.forSchema({
        type: 'record',
        name: 'Person',
        fields: [{name: 'nothing', type: {type: 'null'}}],
      });
      assert.strictEqual(type.fields[0].type.constructor, builtins.NullType);
    });

    test('fromBuffer truncated', () => {
      const type = Type.forSchema('int');
      assert.throws(() => {
        type.fromBuffer(Buffer.from([128]));
      });
    });

    test('fromBuffer bad resolver', () => {
      const type = Type.forSchema('int');
      assert.throws(() => {
        type.fromBuffer(Buffer.from([0]), 123, {});
      });
    });

    test('fromBuffer trailing', () => {
      const type = Type.forSchema('int');
      assert.throws(() => {
        type.fromBuffer(Buffer.from([0, 2]));
      });
    });

    test('fromBuffer trailing with resolver', () => {
      const type = Type.forSchema('int');
      const resolver = type.createResolver(Type.forSchema(['int']));
      assert.equal(type.fromBuffer(Buffer.from([0, 2]), resolver), 1);
    });

    test('toBuffer', () => {
      const type = Type.forSchema('int');
      assert.throws(() => {
        type.toBuffer('abc');
      });
      assert.doesNotThrow(() => {
        type.toBuffer(123);
      });
    });

    test('toBuffer and resize', () => {
      const type = Type.forSchema('string');
      assert.deepEqual(type.toBuffer('\x01', 1), Buffer.from([2, 1]));
    });

    test('type hook', () => {
      const refs = [];
      const ts = [];
      const o = {
        type: 'record',
        name: 'Human',
        fields: [
          {name: 'age', type: 'int'},
          {name: 'name', type: {type: 'string'}},
        ],
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

        const type = Type.forSchema(schema, opts);
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

      function hook() {
        return 'int';
      }
    });

    test('type hook for aliases', () => {
      const a1 = {
        type: 'record',
        name: 'R1',
        fields: [{name: 'r2', type: 'R2'}],
      };
      const a2 = {
        type: 'record',
        name: 'R2',
        fields: [{name: 'r1', type: 'R1'}],
      };
      const opts = {typeHook: hook, registry: {}};
      Type.forSchema(a1, opts);
      assert.deepEqual(Object.keys(opts.registry), ['R1', 'R2']);

      function hook(name, opts) {
        if (name === 'R2') {
          return Type.forSchema(a2, opts);
        }
      }
    });

    test('fingerprint', () => {
      const t = Type.forSchema('int');
      const buf = Buffer.from('ef524ea1b91e73173d938ade36c1db32', 'hex');
      assert.deepEqual(t.fingerprint('md5'), buf);
      assert.deepEqual(t.fingerprint(), buf);
    });

    test('getSchema default', () => {
      const type = Type.forSchema({
        type: 'record',
        name: 'Human',
        fields: [
          {name: 'id1', type: ['string', 'null'], default: ''},
          {name: 'id2', type: ['null', 'string'], default: null},
        ],
      });
      assert.deepEqual(type.getSchema(), {
        type: 'record',
        name: 'Human',
        fields: [
          {name: 'id1', type: ['string', 'null']}, // Stripped defaults.
          {name: 'id2', type: ['null', 'string']},
        ],
      });
    });

    test('invalid unwrapped union default', () => {
      assert.throws(() => {
        Type.forSchema(
          {
            name: 'Person',
            type: 'record',
            fields: [{name: 'id', type: ['null', 'int'], default: 2}],
          },
          {wrapUnions: false}
        );
      }, /invalid "null"/);
    });

    test('anonymous types', () => {
      const t = Type.forSchema({
        type: 'record',
        fields: [{name: 'foo', type: 'int'}],
      });
      assert.strictEqual(t.name, undefined);
      assert.strictEqual(t.branchName, 'record');
      assert(t.isValid({foo: 3}));
      assert.throws(() => {
        Type.forSchema({name: '', type: 'record', fields: []});
      });
    });

    test('auto union wrapping', () => {
      const t = Type.forSchema(
        {
          type: 'record',
          fields: [
            {name: 'wrapped', type: ['int', 'double']}, // Ambiguous.
            {name: 'unwrapped', type: ['string', 'int']}, // Non-ambiguous.
          ],
        },
        {wrapUnions: 'AUTO'}
      );
      assert(Type.isType(t.field('wrapped').type, 'union:wrapped'));
      assert(Type.isType(t.field('unwrapped').type, 'union:unwrapped'));
    });

    test('union projection', () => {
      const Dog = {
        type: 'record',
        name: 'Dog',
        fields: [{type: 'string', name: 'bark'}],
      };
      const Cat = {
        type: 'record',
        name: 'Cat',
        fields: [{type: 'string', name: 'meow'}],
      };
      const animalTypes = [Dog, Cat];

      let callsToWrapUnions = 0;
      const wrapUnions = (types) => {
        callsToWrapUnions++;
        assert.deepEqual(
          types.map((t) => t.name),
          ['Dog', 'Cat']
        );
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
      const Animal = Type.forSchema(animalTypes, {wrapUnions});
      Animal.toBuffer({meow: '🐈'});
      assert.equal(callsToWrapUnions, 1);
      assert.throws(() => Animal.toBuffer({snap: '🐊'}), /Unknown animal/);
    });

    test('union projection with fallback', () => {
      const t = Type.forSchema(
        {
          type: 'record',
          fields: [
            {name: 'wrapped', type: ['int', 'double']}, // Ambiguous.
          ],
        },
        {wrapUnions: () => undefined}
      );
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
      const t = Type.forSchema('int');
      assert.equal(t.fromString('2'), 2);
      assert.throws(() => {
        t.fromString('"a"');
      });
    });

    test('string', () => {
      const t = Type.forSchema('string');
      assert.equal(t.fromString('"2"'), '2');
      assert.throws(() => {
        t.fromString('a');
      });
    });

    test('coerce buffers', () => {
      const t = Type.forSchema({
        name: 'Ids',
        type: 'record',
        fields: [{name: 'id1', type: {name: 'Id1', type: 'fixed', size: 2}}],
      });
      const o = {id1: Buffer.from([0, 1])};
      const s = '{"id1": "\\u0000\\u0001"}';
      const c = t.fromString(s);
      assert.deepEqual(c, o);
      assert(c instanceof t.getRecordConstructor());
    });
  });

  suite('toString', () => {
    test('int', () => {
      const t = Type.forSchema('int');
      assert.equal(t.toString(2), '2');
      assert.throws(() => {
        t.toString('a');
      });
    });
  });

  suite('resolve', () => {
    test('non type', () => {
      const t = Type.forSchema({type: 'map', values: 'int'});
      const obj = {type: 'map', values: 'int'};
      assert.throws(() => {
        t.createResolver(obj);
      });
    });

    test('union to valid wrapped union', () => {
      const t1 = Type.forSchema(['int', 'string']);
      const t2 = Type.forSchema(['null', 'string', 'long'], {wrapUnions: true});
      const resolver = t2.createResolver(t1);
      const buf = t1.toBuffer(12);
      assert.deepEqual(t2.fromBuffer(buf, resolver), {long: 12});
    });

    test('union to invalid union', () => {
      const t1 = Type.forSchema(['int', 'string']);
      const t2 = Type.forSchema(['null', 'long']);
      assert.throws(() => {
        t2.createResolver(t1);
      });
    });

    test('wrapped union to non union', () => {
      const t1 = Type.forSchema(['int', 'long'], {wrapUnions: true});
      const t2 = Type.forSchema('long');
      const resolver = t2.createResolver(t1);
      let buf = t1.toBuffer({int: 12});
      assert.equal(t2.fromBuffer(buf, resolver), 12);
      buf = Buffer.from([4, 0]);
      assert.throws(() => {
        t2.fromBuffer(buf, resolver);
      });
    });

    test('union to non union', () => {
      const t1 = Type.forSchema(['bytes', 'string']);
      const t2 = Type.forSchema('bytes');
      const resolver = t2.createResolver(t1);
      const buf = t1.toBuffer('\x01\x02');
      assert.deepEqual(t2.fromBuffer(buf, resolver), Buffer.from([1, 2]));
    });

    test('union to invalid non union', () => {
      const t1 = Type.forSchema(['int', 'long'], {wrapUnions: true});
      const t2 = Type.forSchema('int');
      assert.throws(() => {
        t2.createResolver(t1);
      });
    });

    test('anonymous types', () => {
      const t1 = Type.forSchema({type: 'fixed', size: 2});
      const t2 = Type.forSchema({
        type: 'fixed',
        size: 2,
        namespace: 'foo',
        aliases: ['Id'],
      });
      const t3 = Type.forSchema({type: 'fixed', size: 2, name: 'foo.Id'});
      assert.throws(() => {
        t1.createResolver(t3);
      });
      assert.doesNotThrow(() => {
        t2.createResolver(t3);
      });
      assert.doesNotThrow(() => {
        t3.createResolver(t1);
      });
    });

    test('ignore namespaces', () => {
      const t1 = Type.forSchema({type: 'fixed', name: 'foo.Two', size: 2});
      const t2 = Type.forSchema({
        type: 'fixed',
        size: 2,
        name: 'bar.Deux',
        aliases: ['bar.Two'],
      });
      assert.throws(() => {
        t1.createResolver(t2);
      });
      assert.doesNotThrow(() => {
        t2.createResolver(t1, {ignoreNamespaces: true});
      });
      const t3 = Type.forSchema({type: 'fixed', size: 2, name: 'Two'});
      assert.throws(() => {
        t3.createResolver(t1);
      });
      assert.doesNotThrow(() => {
        t3.createResolver(t1, {ignoreNamespaces: true});
      });
    });
  });

  suite('type references', () => {
    test('null', () => {
      assert.throws(() => {
        Type.forSchema(null);
      }, /did you mean/);
    });

    test('existing', () => {
      const type = Type.forSchema({
        type: 'record',
        name: 'Person',
        fields: [{name: 'so', type: 'Person'}],
      });
      assert.strictEqual(type, type.fields[0].type);
    });

    test('namespaced', () => {
      const type = Type.forSchema({
        type: 'record',
        name: 'Person',
        fields: [
          {
            name: 'so',
            type: {
              type: 'record',
              name: 'Person',
              fields: [{name: 'age', type: 'int'}],
              namespace: 'a',
            },
          },
        ],
      });
      assert.equal(type.name, 'Person');
      assert.equal(type.fields[0].type.name, 'a.Person');
    });

    test('namespaced global', () => {
      const type = Type.forSchema({
        type: 'record',
        name: '.Person',
        namespace: 'earth',
        fields: [
          {
            name: 'gender',
            type: {type: 'enum', name: 'Gender', symbols: ['F', 'M']},
          },
        ],
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
                fields: [{name: 'age', type: 'int'}],
              },
            },
          ],
        });
      });
    });

    test('missing', () => {
      assert.throws(() => {
        Type.forSchema({
          type: 'record',
          name: 'Person',
          fields: [{name: 'so', type: 'Friend'}],
        });
      });
    });

    test('redefining primitive', () => {
      assert.throws(
        // Unqualified.
        () => {
          Type.forSchema({type: 'fixed', name: 'int', size: 2});
        }
      );
      assert.throws(
        // Qualified.
        () => {
          Type.forSchema({type: 'fixed', name: 'int', size: 2, namespace: 'a'});
        }
      );
    });

    test('aliases', () => {
      const type = Type.forSchema({
        type: 'record',
        name: 'Person',
        namespace: 'a',
        aliases: ['Human', 'b.Being'],
        fields: [{name: 'age', type: 'int'}],
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
      const t = Type.forSchema([
        {type: 'enum', symbols: ['A']},
        'int',
        {type: 'record', fields: [{name: 'foo', type: 'string'}]},
      ]);
      assert.equal(
        JSON.stringify(t.getSchema()),
        '[{"type":"enum","symbols":["A"]},"int",{"type":"record","fields":[{"name":"foo","type":"string"}]}]'
      );
    });
  });

  suite('decode', () => {
    test('long valid', () => {
      const t = Type.forSchema('long');
      const buf = Buffer.from([0, 128, 2, 0]);
      const res = t.decode(buf, 1);
      assert.deepEqual(res, {value: 128, offset: 3});
    });

    test('bytes invalid', () => {
      const t = Type.forSchema('bytes');
      const buf = Buffer.from([4, 1]);
      const res = t.decode(buf, 0);
      assert.deepEqual(res, {value: undefined, offset: -1});
    });
  });

  suite('encode', () => {
    test('int valid', () => {
      const t = Type.forSchema('int');
      const buf = Buffer.alloc(2);
      buf.fill(0);
      const n = t.encode(5, buf, 1);
      assert.equal(n, 2);
      assert.deepEqual(buf, Buffer.from([0, 10]));
    });

    test('too short', () => {
      const t = Type.forSchema('string');
      const buf = Buffer.alloc(1);
      const n = t.encode('\x01\x02', buf, 0);
      assert.equal(n, -2);
    });

    test('invalid', () => {
      const t = Type.forSchema('float');
      const buf = Buffer.alloc(2);
      assert.throws(() => {
        t.encode('hi', buf, 0);
      });
    });

    test('invalid offset', () => {
      const t = Type.forSchema('string');
      const buf = Buffer.alloc(2);
      assert.throws(() => {
        t.encode('hi', buf, -1);
      });
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
      const t1 = Type.forSchema('int');
      const t2 = Type.forSchema('double');
      const resolver = t2.createResolver(t1);
      assert.equal(resolver.inspect(), '<Resolver>');
    });
  });

  test('equals', () => {
    const t1 = Type.forSchema('int');
    const t2 = Type.forSchema('int');
    assert(t1.equals(t2));
    assert(t2.equals(t1));
    assert(!t1.equals(Type.forSchema('long')));
    assert(!t1.equals(null));
  });

  test('equals strict', () => {
    const t1 = Type.forSchema({
      type: 'record',
      name: 'Foo',
      fields: [{name: 'foo', type: 'int', default: 0}],
    });
    const t2 = Type.forSchema({
      type: 'record',
      name: 'Foo',
      fields: [{name: 'foo', type: 'int', default: 1}],
    });
    assert(t1.equals(t2));
    assert(!t1.equals(t2, {strict: true}));
  });

  test('documentation', () => {
    assert.strictEqual(Type.forSchema('int').doc, undefined);
    const t1 = Type.forSchema({
      type: 'record',
      doc: 'A foo.',
      fields: [{name: 'bar', doc: 'Bar', type: 'int'}],
    });
    assert.equal(t1.doc, 'A foo.');
    assert.equal(t1.getField('bar').doc, 'Bar');
    const t2 = Type.forSchema({type: 'int', doc: 'A foo.'});
    assert.strictEqual(t2.doc, undefined);
  });

  test('isType', () => {
    const t = Type.forSchema('int');
    assert(types.Type.isType(t));
    assert(types.Type.isType(t, 'int'));
    assert(!types.Type.isType());
    assert(!types.Type.isType('int'));
    assert(!types.Type.isType(() => {}));
  });

  test('reset', () => {
    types.Type.__reset(0);
    const t = Type.forSchema('string');
    const buf = t.toBuffer('\x01');
    assert.deepEqual(buf, Buffer.from([2, 1]));
  });

  suite('forTypes', () => {
    const combine = Type.forTypes;

    test('empty', () => {
      assert.throws(() => {
        combine([]);
      });
    });

    test('numbers', () => {
      const t1 = Type.forSchema('int');
      const t2 = Type.forSchema('long');
      const t3 = Type.forSchema('float');
      const t4 = Type.forSchema('double');
      assert.strictEqual(combine([t1, t2]), t2);
      assert.strictEqual(combine([t1, t2, t3, t4]), t4);
      assert.strictEqual(combine([t3, t2]), t3);
      assert.strictEqual(combine([t2]), t2);
    });

    test('string & int', () => {
      const t1 = Type.forSchema('int');
      const t2 = Type.forSchema('string');
      assertUnionsEqual(combine([t1, t2]), Type.forSchema(['int', 'string']));
    });

    test('records & maps', () => {
      const t1 = Type.forSchema({
        type: 'record',
        fields: [{name: 'foo', type: 'int', default: 2}],
      });
      const t2 = Type.forSchema({type: 'map', values: 'string'});
      let t3;
      t3 = combine([t1, t2]);
      assertUnionsEqual(t3.getValuesType(), Type.forSchema(['int', 'string']));
      t3 = combine([t2, t1]);
      assertUnionsEqual(t3.getValuesType(), Type.forSchema(['int', 'string']));
    });

    test('arrays', () => {
      const t1 = Type.forSchema({type: 'array', items: 'null'});
      const t2 = Type.forSchema({type: 'array', items: 'int'});
      const t3 = combine([t1, t2]);
      assertUnionsEqual(t3.getItemsType(), Type.forSchema(['null', 'int']));
    });

    test('field single default', () => {
      const t1 = Type.forSchema({
        type: 'record',
        fields: [{name: 'foo', type: 'int', default: 2}],
      });
      const t2 = Type.forSchema({
        type: 'record',
        fields: [],
      });
      const t3 = combine([t1, t2], {strictDefaults: true});
      assert.deepEqual(t3.getSchema({exportAttrs: true}), {
        type: 'record',
        fields: [{name: 'foo', type: 'int', default: 2}],
      });
    });

    test('field multiple types default', () => {
      const t1 = Type.forSchema({
        type: 'record',
        fields: [{name: 'foo', type: 'string'}],
      });
      const t2 = Type.forSchema({
        type: 'record',
        fields: [{name: 'foo', type: 'int', default: 2}],
      });
      const t3 = combine([t1, t2], {strictDefaults: true});
      assert.deepEqual(t3.getSchema({exportAttrs: true}), {
        type: 'record',
        fields: [
          // Int should be first in the union.
          {name: 'foo', type: ['int', 'string'], default: 2},
        ],
      });
    });

    test('missing fields no null default', () => {
      const t1 = Type.forSchema({
        type: 'record',
        fields: [
          {name: 'foo', type: 'int'},
          {name: 'bar', type: 'string'},
        ],
      });
      const t2 = Type.forSchema({
        type: 'record',
        fields: [{name: 'bar', type: 'string'}],
      });
      let t3;
      t3 = combine([t1, t2]);
      assert.deepEqual(t3.getSchema({exportAttrs: true}), {
        type: 'record',
        fields: [
          // The null branch should always be first here.
          {name: 'foo', type: ['null', 'int'], default: null},
          {name: 'bar', type: 'string'},
        ],
      });
      t3 = combine([t1, t2], {strictDefaults: true});
      assertUnionsEqual(t3.getValuesType(), Type.forSchema(['int', 'string']));
    });

    test('logical types', () => {
      class EvenType extends LogicalType {
        _fromValue(val) {
          return 2 * val;
        }
        _toValue(any) {
          if (any === (any | 0) && any % 2 === 0) {
            return any / 2;
          }
        }
      }

      class OddType extends LogicalType {
        _fromValue(val) {
          return 2 * val + 1;
        }
        _toValue(any) {
          if (any === (any | 0) && any % 2 === 1) {
            return any / 2;
          }
        }
      }

      const opts = {logicalTypes: {even: EvenType, odd: OddType}};

      const t1 = Type.forSchema({type: 'int', logicalType: 'even'}, opts);
      const t2 = Type.forSchema({type: 'long', logicalType: 'odd'}, opts);
      assertUnionsEqual(combine([t1, t2]), Type.forSchema([t1, t2]));
      assert.throws(() => {
        combine([t1, t1]);
      });
    });

    test('invalid wrapped union', () => {
      const t1 = Type.forSchema(['int'], {wrapUnions: true});
      const t2 = Type.forSchema('string');
      assert.throws(() => {
        combine([t1, t2]);
      }, /cannot combine/);
    });

    test('error while creating wrapped union', () => {
      const opts = {typeHook: hook, wrapUnions: false};
      const t1 = Type.forSchema(['int'], {wrapUnions: true});
      const t2 = Type.forSchema(['string'], {wrapUnions: true});
      assert.throws(() => {
        combine([t1, t2], opts);
      }, /foo/);
      assert(!opts.wrapUnions);

      function hook() {
        throw new Error('foo');
      }
    });

    test('inconsistent wrapped union', () => {
      const t1 = Type.forSchema([{type: 'fixed', name: 'Id', size: 2}], {
        wrapUnions: true,
      });
      const t2 = Type.forSchema([{type: 'fixed', name: 'Id', size: 3}], {
        wrapUnions: true,
      });
      assert.throws(() => {
        combine([t1, t2]);
      }, /inconsistent/);
    });

    test('valid wrapped unions', () => {
      const opts = {wrapUnions: true};
      const t1 = Type.forSchema(['int', 'string', 'null'], opts);
      const t2 = Type.forSchema(['null', 'long'], opts);
      assertUnionsEqual(
        combine([t1, t2]),
        Type.forSchema(['int', 'long', 'string', 'null'], opts)
      );
    });

    test('valid unwrapped unions', () => {
      const t1 = Type.forSchema(['int', 'string', 'null']);
      const t2 = Type.forSchema(['null', 'long']);
      assertUnionsEqual(
        combine([t1, t2]),
        Type.forSchema(['long', 'string', 'null'])
      );
    });

    test('buffers', () => {
      const t1 = Type.forSchema({type: 'fixed', size: 2});
      const t2 = Type.forSchema({type: 'fixed', size: 4});
      const t3 = Type.forSchema('bytes');
      assert.strictEqual(combine([t1, t1]), t1);
      assert.strictEqual(combine([t1, t3]), t3);
      assert(combine([t1, t2]).equals(t3));
    });

    test('strings', () => {
      const t1 = Type.forSchema({type: 'enum', symbols: ['A', 'b']});
      const t2 = Type.forSchema({type: 'enum', symbols: ['A', 'B']});
      const t3 = Type.forSchema('string');
      let symbols;
      symbols = combine([t1, t1]).getSymbols().slice();
      assert.deepEqual(symbols.sort(), ['A', 'b']);
      assert.strictEqual(combine([t1, t3]), t3);
      assert.strictEqual(combine([t1, t2, t3]), t3);
      symbols = combine([t1, t2]).getSymbols().slice();
      assert.deepEqual(symbols.sort(), ['A', 'B', 'b']);
    });

    test('strings', () => {
      const opts = {wrapUnions: true};
      const t1 = Type.forSchema(['null', 'int'], opts);
      const t2 = Type.forSchema(['null', 'long', 'string'], opts);
      const t3 = Type.forSchema(['string'], opts);
      const t4 = combine([t1, t2, t3]);
      assert.deepEqual(t4.getSchema(), ['null', 'int', 'long', 'string']);
    });
  });

  suite('forValue', () => {
    const infer = Type.forValue;

    test('numbers', () => {
      assert.equal(infer(1).typeName, 'int');
      assert.equal(infer(1.2).typeName, 'float');
      assert.equal(infer(9007199254740991).typeName, 'double');
    });

    test('function', () => {
      assert.throws(() => {
        infer(() => {});
      });
    });

    test('record', () => {
      const t = infer({b: true, n: null, s: '', f: Buffer.alloc(0)});
      assert.deepEqual(t.getSchema(), {
        type: 'record',
        fields: [
          {name: 'b', type: 'boolean'},
          {name: 'n', type: 'null'},
          {name: 's', type: 'string'},
          {name: 'f', type: 'bytes'},
        ],
      });
    });

    test('empty array', () => {
      // Mostly check that the sentinel behaves correctly.
      const t1 = infer({0: [], 1: [true]});
      assert.equal(t1.getValuesType().getItemsType().typeName, 'boolean');
      const t2 = infer({0: [], 1: [true], 2: [null]});
      assertUnionsEqual(
        t2.getValuesType().getItemsType(),
        Type.forSchema(['boolean', 'null'])
      );
      const t3 = infer({0: [], 1: []});
      assert.equal(t3.getValuesType().getItemsType().typeName, 'null');
    });

    test('value hook', () => {
      const t = infer({foo: 23, bar: 'hi'}, {valueHook: hook});
      assert.equal(t.getField('foo').getType().typeName, 'long');
      assert.equal(t.getField('bar').getType().typeName, 'string');
      assert.throws(() => {
        infer({foo () {}}, {valueHook: hook});
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
      const t = infer([{foo: 2}, {foo: 3}], {typeHook: hook}).itemsType;
      assert.equal(t.name, 'Foo3');
      assert.equal(t.field('foo').type.typeName, 'int');

      function hook(schema) {
        if (schema.type !== 'record') {
          return;
        }
        schema.name = 'Foo' + i++;
      }
    });

    test('type hook nested array', () => {
      let i = 1;
      const outer = infer([[{foo: 2}], [{foo: 3}]], {typeHook: hook});
      const inner = outer.itemsType.itemsType;
      assert.equal(inner.name, 'Foo3');
      assert.equal(inner.field('foo').type.typeName, 'int');

      function hook(schema) {
        if (schema.type !== 'record') {
          return;
        }
        schema.name = 'Foo' + i++;
      }
    });
  });
});

function testType(Type, data, invalidSchemas) {
  data.forEach((elem) => {
    test('roundtrip', () => {
      const type = new Type(elem.schema);
      elem.valid.forEach((v) => {
        assert(type.isValid(v), '' + v);
        const fn = elem.check || assert.deepEqual;
        fn(type.fromBuffer(type.toBuffer(v)), v);
        fn(type.fromString(type.toString(v), {coerceBuffers: true}), v);
      });
      elem.invalid.forEach((v) => {
        assert(!type.isValid(v), '' + v);
        assert.throws(() => {
          type.isValid(v, {errorHook: hook});
        });
        assert.throws(() => {
          type.toBuffer(v);
        });

        function hook() {
          throw new Error();
        }
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
      const fn = elem.check || assert.deepEqual;
      const items = elem.valid;
      if (items.length > 1) {
        const type = new Type(elem.schema);
        const buf = Buffer.alloc(1024);
        const tap = Tap.fromBuffer(buf);
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
        assert.throws(() => {
          new Type(schema);
        });
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
  const b1 = utils.toMap(t1.types, (t) => {
    return t.branchName;
  });
  t2.types.forEach((t) => {
    assert(t.equals(b1[t.branchName]));
  });
}

function supportsErrorStacks() {
  return typeof Error.captureStackTrace == 'function' || Error().stack;
}
