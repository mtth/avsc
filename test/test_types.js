/* jshint node: true, mocha: true */

'use strict';

var types = require('../lib/types'),
    utils = require('../lib/utils'),
    assert = require('assert'),
    util = require('util');


var Tap = utils.Tap;
var createType = types.createType;
var builtins = types.builtins;


suite('types', function () {

  suite('BooleanType', function () {

    var data = [
      {
        valid: [true, false],
        invalid: [null, 'hi', undefined, 1.5, 1e28, 123124123123213]
      }
    ];

    testType(builtins.BooleanType, data);

    test('to JSON', function () {
      var t = new builtins.BooleanType();
      assert.equal(t.toJSON(), 'boolean');
    });

    test('compare buffers', function () {
      var t = new builtins.BooleanType();
      var bt = t.toBuffer(true);
      var bf = t.toBuffer(false);
      assert.equal(t.compareBuffers(bt, bf), 1);
      assert.equal(t.compareBuffers(bf, bt), -1);
      assert.equal(t.compareBuffers(bt, bt), 0);
    });

    test('get name', function () {
      var t = new builtins.BooleanType();
      assert.strictEqual(t.getName(), undefined);
      assert.equal(t.getName(true), 'boolean');
    });

  });

  suite('IntType', function () {

    var data = [
      {
        valid: [1, -3, 12314, 0, 1e9],
        invalid: [null, 'hi', undefined, 1.5, 1e28, 123124123123213]
      }
    ];

    testType(builtins.IntType, data);

    test('toBuffer int', function () {

      var type = createType('int');
      assert.equal(type.fromBuffer(new Buffer([0x80, 0x01])), 64);
      assert(new Buffer([0]).equals(type.toBuffer(0)));

    });

    test('resolve int > long', function () {
      var intType = createType('int');
      var longType = createType('long');
      var buf = intType.toBuffer(123);
      assert.equal(
        longType.fromBuffer(buf, longType.createResolver(intType)),
        123
      );
    });

    test('resolve int > U[null, int]', function () {
      var wt = createType('int');
      var rt = createType(['null', 'int']);
      var buf = wt.toBuffer(123);
      assert.deepEqual(rt.fromBuffer(buf, rt.createResolver(wt)), 123);
    });

    test('resolve int > W[null, int]', function () {
      var wt = createType('int');
      var rt = createType(['null', 'int'], {wrapUnions: true});
      var buf = wt.toBuffer(123);
      assert.deepEqual(
        rt.fromBuffer(buf, rt.createResolver(wt)),
        {'int': 123}
      );
    });

    test('resolve int > float', function () {
      var wt = createType('int');
      var rt = createType('float');
      var buf = wt.toBuffer(123);
      assert.deepEqual(rt.fromBuffer(buf, rt.createResolver(wt)), 123);
    });

    test('resolve int > double', function () {
      var wt = createType('int');
      var rt = createType('double');
      var n = Math.pow(2, 30) + 1;
      var buf = wt.toBuffer(n);
      assert.deepEqual(rt.fromBuffer(buf, rt.createResolver(wt)), n);
    });

    test('toString', function () {
      assert.equal(createType('int').toString(), '"int"');
    });

    test('clone', function () {
      var t = createType('int');
      assert.equal(t.clone(123), 123);
      assert.equal(t.clone(123, {}), 123);
      assert.throws(function () { t.clone(''); });
    });

    test('resolve invalid', function () {
      assert.throws(function () { getResolver('int', 'long'); });
    });

  });

  suite('LongType', function () {

    var data = [
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
      var t1 = createType('long');
      var t2 = createType('float');
      var n = 9007199254740990; // Number.MAX_SAFE_INTEGER - 1
      var buf = t1.toBuffer(n);
      var f = t2.fromBuffer(buf, t2.createResolver(t1));
      assert(Math.abs(f - n) / n < 1e-7);
      assert(t2.isValid(f));
    });

    test('precision loss', function () {
      var type = createType('long');
      var buf = new Buffer([0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x20]);
      assert.throws(function () { type.fromBuffer(buf); });
    });

    test('using missing methods', function () {
      assert.throws(function () { builtins.LongType.__with(); });
    });

  });

  suite('StringType', function () {

    var data = [
      {
        valid: ['', 'hi'],
        invalid: [null, undefined, 1, 0]
      }
    ];

    testType(builtins.StringType, data);

    test('fromBuffer string', function () {
      var type = createType('string');
      var buf = new Buffer([0x06, 0x68, 0x69, 0x21]);
      var s = 'hi!';
      assert.equal(type.fromBuffer(buf), s);
      assert(buf.equals(type.toBuffer(s)));
    });

    test('toBuffer string', function () {
      var type = createType('string');
      var buf = new Buffer([0x06, 0x68, 0x69, 0x21]);
      assert(buf.equals(type.toBuffer('hi!', 1)));
    });

    test('resolve string > bytes', function () {
      var stringT = createType('string');
      var bytesT = createType('bytes');
      var buf = stringT.toBuffer('\x00\x01');
      assert.deepEqual(
        bytesT.fromBuffer(buf, bytesT.createResolver(stringT)),
        new Buffer([0, 1])
      );
    });

    test('encode resize', function () {
      var t = createType('string');
      var s = 'hello';
      var b, pos;
      b = new Buffer(2);
      pos = t.encode(s, b);
      assert(pos < 0);
      b = new Buffer(b.length - pos);
      pos = t.encode(s, b);
      assert(pos >= 0);
      assert.equal(s, t.fromBuffer(b)); // Also checks exact length match.
    });

  });

  suite('NullType', function () {

    var data = [
      {
        schema: 'null',
        valid: [null],
        invalid: [0, 1, 'hi', undefined]
      }
    ];

    testType(builtins.NullType, data);

  });

  suite('FloatType', function () {

    var data = [
      {
        valid: [1, -3, 123e7],
        invalid: [null, 'hi', undefined],
        check: function (a, b) { assert(floatEquals(a, b)); }
      }
    ];

    testType(builtins.FloatType, data);

    test('compare buffer', function () {
      var t = createType('float');
      var b1 = t.toBuffer(0.5);
      assert.equal(t.compareBuffers(b1, b1), 0);
      var b2 = t.toBuffer(-0.75);
      assert.equal(t.compareBuffers(b1, b2), 1);
      var b3 = t.toBuffer(175);
      assert.equal(t.compareBuffers(b1, b3), -1);
    });

    test('resolver float > float', function () {
      assert.doesNotThrow(function () { getResolver('float', 'float'); });
    });

    test('resolver double > float', function () {
      assert.throws(function () { getResolver('float', 'double'); });
    });

    test('fromString', function () {
      var t = createType('float');
      var f = t.fromString('3.1');
      assert(t.isValid(f));
    });

    test('clone from double', function () {
      var t = createType('float');
      var d = 3.1;
      var f;
      f = t.clone(d);
      assert(t.isValid(f));
    });

  });

  suite('DoubleType', function () {

    var data = [
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
      var t = createType('double');
      var b1 = t.toBuffer(0.5);
      assert.equal(t.compareBuffers(b1, b1), 0);
      var b2 = t.toBuffer(-0.75);
      assert.equal(t.compareBuffers(b1, b2), 1);
      var b3 = t.toBuffer(175);
      assert.equal(t.compareBuffers(b1, b3), -1);
    });

  });

  suite('BytesType', function () {

    var data = [
      {
        valid: [new Buffer(1), new Buffer('abc')],
        invalid: [null, 'hi', undefined, 1, 0, -3.5]
      }
    ];

    testType(builtins.BytesType, data);

    test('resolve string > bytes', function () {
      var bytesT = createType('bytes');
      var stringT = createType('string');
      var buf = new Buffer([4, 0, 1]);
      assert.deepEqual(
        stringT.fromBuffer(buf, stringT.createResolver(bytesT)),
        '\x00\x01'
      );
    });

    test('clone', function () {
      var t = createType('bytes');
      var s = '\x01\x02';
      var buf = new Buffer(s);
      var clone;
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
      var t = createType('bytes');
      var s = '\x01\x02';
      var buf = new Buffer(s);
      var clone;
      clone = t.fromString(JSON.stringify(s));
      assert.deepEqual(clone, buf);
      clone = t.fromString(JSON.stringify(s), {});
      assert.deepEqual(clone, buf);
    });

    test('compare', function () {
      var t = createType('bytes');
      var b1 = t.toBuffer(new Buffer([0, 2]));
      assert.equal(t.compareBuffers(b1, b1), 0);
      var b2 = t.toBuffer(new Buffer([0, 2, 3]));
      assert.equal(t.compareBuffers(b1, b2), -1);
      var b3 = t.toBuffer(new Buffer([1]));
      assert.equal(t.compareBuffers(b3, b1), 1);
    });

  });

  suite('UnwrappedUnionType', function () {

    var data = [
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
        valid: [null, new Buffer(2)],
        invalid: [{'a.B': new Buffer(2)}],
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

    var schemas = [
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
      var t = new builtins.UnwrappedUnionType(['null', 'int']);
      assert.deepEqual(t.getTypes(), [createType('null'), createType('int')]);
    });

    test('getTypeName', function () {
      var t = new builtins.UnwrappedUnionType(['null', 'int']);
      assert.strictEqual(t.getName(), undefined);
      assert.strictEqual(t.getName(true), undefined);
      assert.equal(t.getTypeName(), 'union:unwrapped');
    });

    test('invalid read', function () {
      var type = new builtins.UnwrappedUnionType(['null', 'int']);
      assert.throws(function () { type.fromBuffer(new Buffer([4])); });
    });

    test('missing bucket write', function () {
      var type = new builtins.UnwrappedUnionType(['null', 'int']);
      assert.throws(function () { type.toBuffer('hi'); });
    });

    test('invalid bucket write', function () {
      var type = new builtins.UnwrappedUnionType(['null', 'int']);
      assert.throws(function () { type.toBuffer(2.5); });
    });

    test('fromString', function () {
      var type = new builtins.UnwrappedUnionType(['null', 'int']);
      assert.strictEqual(type.fromString('null'), null);
      assert.deepEqual(type.fromString('{"int": 48}'), 48);
      assert.throws(function () { type.fromString('48'); });
      assert.throws(function () { type.fromString('{"long": 48}'); });
    });

    test('toString', function () {
      var type = new builtins.UnwrappedUnionType(['null', 'int']);
      assert.strictEqual(type.toString(null), 'null');
      assert.deepEqual(type.toString(48), '{"int":48}');
      assert.throws(function () { type.toString(2.5); });
    });

    test('non wrapped write', function () {
      var type = new builtins.UnwrappedUnionType(['null', 'int']);
      assert.deepEqual(type.toBuffer(23), new Buffer([2, 46]));
      assert.deepEqual(type.toBuffer(null), new Buffer([0]));
    });

    test('coerce buffers', function () {
      var type = new builtins.UnwrappedUnionType(['null', 'bytes']);
      var obj = {type: 'Buffer', data: [1, 2]};
      assert.throws(function () { type.clone(obj); });
      assert.deepEqual(
        type.clone(obj, {coerceBuffers: true}),
        new Buffer([1, 2])
      );
      assert.deepEqual(type.clone(null, {coerceBuffers: true}), null);
    });

    test('wrapped write', function () {
      var type = new builtins.UnwrappedUnionType(['null', 'int']);
      assert.throws(function () { type.toBuffer({'int': 1}); });
    });

    test('to JSON', function () {
      var type = new builtins.UnwrappedUnionType(['null', 'int']);
      assert.equal(JSON.stringify(type), '["null","int"]');
      assert.equal(type.inspect(), '<UnwrappedUnionType ["null","int"]>');
    });

    test('resolve int to [string, long]', function () {
      var t1 = createType('int');
      var t2 = new builtins.UnwrappedUnionType(['string', 'long']);
      var a = t2.createResolver(t1);
      var buf = t1.toBuffer(23);
      assert.deepEqual(t2.fromBuffer(buf, a), 23);
    });

    test('resolve null to [null, int]', function () {
      var t1 = createType('null');
      var t2 = new builtins.UnwrappedUnionType(['null', 'int']);
      var a = t2.createResolver(t1);
      assert.deepEqual(t2.fromBuffer(new Buffer(0), a), null);
    });

    test('resolve [string, int] to unwrapped [float, bytes]', function () {
      var t1 = new builtins.WrappedUnionType(['string', 'int']);
      var t2 = new builtins.UnwrappedUnionType(['float', 'bytes']);
      var a = t2.createResolver(t1);
      var buf;
      buf = t1.toBuffer({string: 'hi'});
      assert.deepEqual(t2.fromBuffer(buf, a), new Buffer('hi'));
      buf = t1.toBuffer({'int': 1});
      assert.deepEqual(t2.fromBuffer(buf, a), 1);
    });

    test('clone', function () {
      var t = new builtins.UnwrappedUnionType(
        ['null', {type: 'map', values: 'int'}]
      );
      var o = {'int': 1};
      assert.strictEqual(t.clone(null), null);
      var c;
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
      var t = new builtins.UnwrappedUnionType(['string', 'int']);
      assert.throws(function () { t.fromString(null); }, /invalid/);
    });

    test('invalid multiple keys', function () {
      var t = new builtins.UnwrappedUnionType(['null', 'int']);
      var o = {'int': 2};
      assert.equal(t.fromString(JSON.stringify(o)), 2);
      o.foo = 3;
      assert.throws(function () { t.fromString(JSON.stringify(o)); });
    });

    test('clone named type', function () {
      var t = createType({
        name: 'Person',
        type: 'record',
        fields: [
          {name: 'id1', type: {name: 'an.Id', type: 'fixed', size: 1}},
          {name: 'id2', type: ['null', 'an.Id']}
        ]
      }, {wrapUnions: false});
      var b = new Buffer([0]);
      var o = {id1: b, id2: b};
      assert.deepEqual(t.clone(o), o);
    });

    test('compare buffers', function () {
      var t = new builtins.UnwrappedUnionType(['null', 'double']);
      var b1 = t.toBuffer(null);
      assert.equal(t.compareBuffers(b1, b1), 0);
      var b2 = t.toBuffer(4);
      assert.equal(t.compareBuffers(b2, b1), 1);
      assert.equal(t.compareBuffers(b1, b2), -1);
      var b3 = t.toBuffer(6);
      assert.equal(t.compareBuffers(b3, b2), 1);
    });

    test('compare', function () {
      var t;
      t = new builtins.UnwrappedUnionType(['null', 'int']);
      assert.equal(t.compare(null, 3), -1);
      assert.equal(t.compare(null, null), 0);
      assert.throws(function () { t.compare('hi', 2); });
      assert.throws(function () { t.compare(null, 'hey'); });
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

    var schemas = [
      {},
      [],
      ['null', 'null'],
      ['null', {type: 'map', values: 'int'}, {type: 'map', values: 'long'}],
      ['null', ['int', 'string']]
    ];

    testType(builtins.WrappedUnionType, data, schemas);

    test('getTypes', function () {
      var t = createType(['null', 'int']);
      assert.deepEqual(t.getTypes(), [createType('null'), createType('int')]);
    });

    test('get branch type', function () {
      var type = new builtins.WrappedUnionType(['null', 'int']);
      var buf = type.toBuffer({'int': 48});
      var branchType = type.fromBuffer(buf).constructor.getBranchType();
      assert(branchType instanceof builtins.IntType);
    });

    test('missing name write', function () {
      var type = new builtins.WrappedUnionType(['null', 'int']);
      assert.throws(function () {
        type.toBuffer({b: 'a'});
      });
    });

    test('read invalid index', function () {
      var type = new builtins.WrappedUnionType(['null', 'int']);
      var buf = new Buffer([1, 0]);
      assert.throws(function () { type.fromBuffer(buf); });
    });

    test('non wrapped write', function () {
      var type = new builtins.WrappedUnionType(['null', 'int']);
      assert.throws(function () {
        type.toBuffer(1, true);
      }, Error);
    });

    test('to JSON', function () {
      var type = new builtins.WrappedUnionType(['null', 'int']);
      assert.equal(JSON.stringify(type), '["null","int"]');
    });

    test('resolve int to [long, int]', function () {
      var t1 = createType('int');
      var t2 = new builtins.WrappedUnionType(['long', 'int']);
      var a = t2.createResolver(t1);
      var buf = t1.toBuffer(23);
      assert.deepEqual(t2.fromBuffer(buf, a), {'long': 23});
    });

    test('resolve null to [null, int]', function () {
      var t1 = createType('null');
      var t2 = new builtins.WrappedUnionType(['null', 'int']);
      var a = t2.createResolver(t1);
      assert.deepEqual(t2.fromBuffer(new Buffer(0), a), null);
    });

    test('resolve [string, int] to [long, bytes]', function () {
      var t1 = new builtins.WrappedUnionType(['string', 'int']);
      var t2 = new builtins.WrappedUnionType(['long', 'bytes']);
      var a = t2.createResolver(t1);
      var buf;
      buf = t1.toBuffer({string: 'hi'});
      assert.deepEqual(t2.fromBuffer(buf, a), {'bytes': new Buffer('hi')});
      buf = t1.toBuffer({'int': 1});
      assert.deepEqual(t2.fromBuffer(buf, a), {'long': 1});
    });

    test('resolve unwrapped [string, int] to [long, bytes]', function () {
      var t1 = new builtins.UnwrappedUnionType(['string', 'int']);
      var t2 = new builtins.WrappedUnionType(['long', 'bytes']);
      var a = t2.createResolver(t1);
      var buf;
      buf = t1.toBuffer('hi');
      assert.deepEqual(t2.fromBuffer(buf, a), {'bytes': new Buffer('hi')});
      buf = t1.toBuffer(1);
      assert.deepEqual(t2.fromBuffer(buf, a), {'long': 1});
    });

    test('clone', function () {
      var t = new builtins.WrappedUnionType(['null', 'int']);
      var o = {'int': 1};
      assert.strictEqual(t.clone(null), null);
      var c;
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
      var t = new builtins.WrappedUnionType(['string', 'int']);
      var o;
      o = t.clone('hi', {wrapUnions: true});
      assert.deepEqual(o, {'string': 'hi'});
      o = t.clone(3, {wrapUnions: true});
      assert.deepEqual(o, {'int': 3});
      assert.throws(function () { t.clone(null, {wrapUnions: 2}); });
    });

    test('invalid multiple keys', function () {
      var t = new builtins.WrappedUnionType(['null', 'int']);
      var o = {'int': 2};
      assert(t.isValid(o));
      o.foo = 3;
      assert(!t.isValid(o));
    });

    test('clone multiple keys', function () {
      var t = new builtins.WrappedUnionType(['null', 'int']);
      var o = {'int': 2, foo: 3};
      assert.throws(function () { t.clone(o); });
      assert.throws(function () { t.clone(o, {}); });
    });

    test('clone qualify names', function () {
      var t = createType({
        name: 'Person',
        type: 'record',
        fields: [
          {name: 'id1', type: {name: 'an.Id', type: 'fixed', size: 1}},
          {name: 'id2', type: ['null', 'an.Id']}
        ]
      }, {wrapUnions: true});
      var b = new Buffer([0]);
      var o = {id1: b, id2: {Id: b}};
      var c = {id1: b, id2: {'an.Id': b}};
      assert.throws(function () { t.clone(o, {}); });
      assert.deepEqual(t.clone(o, {qualifyNames: true}), c);
    });

    test('clone invalid qualified names', function () {
      var t = createType({
        name: 'Person',
        type: 'record',
        fields: [
          {name: 'id1', type: {name: 'Id', type: 'fixed', size: 1}},
          {name: 'id2', type: ['null', 'Id']}
        ]
      }, {wrapUnions: true});
      var b = new Buffer([0]);
      var o = {id1: b, id2: {'an.Id': b}};
      assert.throws(function () { t.clone(o); });
      assert.throws(function () { t.clone(o, {}); });
    });

    test('compare buffers', function () {
      var t = new builtins.WrappedUnionType(['null', 'double']);
      var b1 = t.toBuffer(null);
      assert.equal(t.compareBuffers(b1, b1), 0);
      var b2 = t.toBuffer({'double': 4});
      assert.equal(t.compareBuffers(b2, b1), 1);
      assert.equal(t.compareBuffers(b1, b2), -1);
      var b3 = t.toBuffer({'double': 6});
      assert.equal(t.compareBuffers(b3, b2), 1);
    });

    test('compare', function () {
      var t;
      t = new builtins.WrappedUnionType(['null', 'int']);
      assert.equal(t.compare(null, {'int': 3}), -1);
      assert.equal(t.compare(null, null), 0);
      t = new builtins.WrappedUnionType(['int', 'float']);
      assert.equal(t.compare({'int': 2}, {'float': 0.5}), -1);
      assert.equal(t.compare({'int': 20}, {'int': 5}), 1);
    });

    test('isValid hook', function () {
      var t = new builtins.WrappedUnionType(['null', 'int']);
      var paths = [];
      assert(t.isValid(null, {errorHook: hook}));
      assert(t.isValid({'int': 1}, {errorHook: hook}));
      assert(!paths.length);
      assert(!t.isValid({'int': 'hi'}, {errorHook: hook}));
      assert.deepEqual(paths, [['int']]);

      function hook(path) { paths.push(path); }
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
        schema: {name: 'Foo', symbols: ['HI', 'A0']},
        valid: ['HI', 'A0'],
        invalid: ['HEY', null, undefined, 0, 'a0']
      }
    ];

    var schemas = [
      {name: 'Foo', symbols: []},
      {name: 'Foo'},
      {name: 'G', symbols: ['0']}
    ];

    testType(builtins.EnumType, data, schemas);

    test('get full name', function () {
      var t = createType({
        type: 'enum',
        symbols: ['A', 'B'],
        name: 'Letter',
        namespace: 'latin'
      });
      assert.equal(t.getName(), 'latin.Letter');
      assert.equal(t.getName(true), 'latin.Letter');
    });

    test('get aliases', function () {
      var t = createType({
        type: 'enum',
        symbols: ['A', 'B'],
        name: 'Letter',
        namespace: 'latin',
        aliases: ['Character', 'alphabet.Letter']
      });
      var aliases = t.getAliases();
      assert.deepEqual(aliases, ['latin.Character', 'alphabet.Letter']);
      aliases.push('Char');
      assert.equal(t.getAliases().length, 3);
    });

    test('get symbols', function () {
      var t = createType({type: 'enum', symbols: ['A', 'B'], name: 'Letter'});
      var symbols = t.getSymbols();
      assert.deepEqual(symbols, ['A', 'B']);
      symbols.push('Char');
      assert.equal(t.getSymbols().length, 2);
    });

    test('duplicate symbol', function () {
      assert.throws(function () {
        createType({type: 'enum', symbols: ['A', 'B', 'A'], name: 'B'});
      });
    });

    test('missing name', function () {
      var attrs = {type: 'enum', symbols: ['A', 'B']};
      var t = createType(attrs);
      assert.strictEqual(t.getName(), undefined);
      assert.strictEqual(t.getName(true), 'enum');
      assert.throws(function () {
        createType(attrs, {noAnonymousTypes: true});
      });
    });

    test('write invalid', function () {
      var type = createType({type: 'enum', symbols: ['A'], name: 'a'});
      assert.throws(function () {
        type.toBuffer('B');
      });
    });

    test('read invalid index', function () {
      var type = new builtins.EnumType({
        type: 'enum', symbols: ['A'], name: 'a'
      });
      var buf = new Buffer([2]);
      assert.throws(function () { type.fromBuffer(buf); });
    });

    test('resolve', function () {
      var t1, t2, buf, resolver;
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
      t2 = newEnum('Foo', ['bar', 'bax']);
      assert.throws(function () { t1.createResolver(t2); });
      assert.throws(function () {
        t1.createResolver(createType('int'));
      });
      function newEnum(name, symbols, aliases, namespace) {
        var obj = {type: 'enum', name: name, symbols: symbols};
        if (aliases !== undefined) {
          obj.aliases = aliases;
        }
        if (namespace !== undefined) {
          obj.namespace = namespace;
        }
        return new builtins.EnumType(obj);
      }
    });

    test('clone', function () {
      var t = createType({type: 'enum', name: 'Foo', symbols: ['bar', 'baz']});
      assert.equal(t.clone('bar'), 'bar');
      assert.equal(t.clone('bar', {}), 'bar');
      assert.throws(function () { t.clone('BAR'); });
      assert.throws(function () { t.clone(null); });
    });

    test('compare buffers', function () {
      var t = createType({type: 'enum', name: 'Foo', symbols: ['bar', 'baz']});
      var b1 = t.toBuffer('bar');
      var b2 = t.toBuffer('baz');
      assert.equal(t.compareBuffers(b1, b1), 0);
      assert.equal(t.compareBuffers(b2, b1), 1);
    });

    test('compare', function () {
      var t = createType({type: 'enum', name: 'Foo', symbols: ['b', 'a']});
      assert.equal(t.compare('b', 'a'), -1);
      assert.equal(t.compare('a', 'a'), 0);
    });

  });

  suite('FixedType', function () {

    var data = [
      {
        name: 'size 1',
        schema: {name: 'Foo', size: 2},
        valid: [new Buffer([1, 2]), new Buffer([2, 3])],
        invalid: ['HEY', null, undefined, 0, new Buffer(1), new Buffer(3)],
        check: function (a, b) { assert(a.equals(b)); }
      }
    ];

    var schemas = [
      {name: 'Foo', size: 0},
      {name: 'Foo', size: -2},
      {name: 'Foo'},
      {}
    ];

    testType(builtins.FixedType, data, schemas);

    test('get full name', function () {
      var t = createType({
        type: 'fixed',
        size: 2,
        name: 'Id',
        namespace: 'id'
      });
      assert.equal(t.getName(), 'id.Id');
      assert.equal(t.getName(true), 'id.Id');
    });

    test('get aliases', function () {
      var t = createType({
        type: 'fixed',
        size: 3,
        name: 'Id'
      });
      var aliases = t.getAliases();
      assert.deepEqual(aliases, []);
      aliases.push('ID');
      assert.equal(t.getAliases().length, 1);
    });

    test('get size', function () {
      var t = createType({type: 'fixed', size: 5, name: 'Id'});
      assert.equal(t.getSize(), 5);
    });

    test('resolve', function () {
      var t1 = new builtins.FixedType({name: 'Id', size: 4});
      var t2 = new builtins.FixedType({name: 'Id', size: 4});
      assert.doesNotThrow(function () { t2.createResolver(t1); });
      t2 = new builtins.FixedType({name: 'Id2', size: 4});
      assert.throws(function () { t2.createResolver(t1); });
      t2 = new builtins.FixedType({name: 'Id2', size: 4, aliases: ['Id']});
      assert.doesNotThrow(function () { t2.createResolver(t1); });
      t2 = new builtins.FixedType({name: 'Id2', size: 5, aliases: ['Id']});
      assert.throws(function () { t2.createResolver(t1); });
    });

    test('clone', function () {
      var t = new builtins.FixedType({name: 'Id', size: 2});
      var s = '\x01\x02';
      var buf = new Buffer(s);
      var clone;
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
      assert.throws(function () { t.clone(new Buffer([2])); });
    });

    test('getSchema with extra fields', function () {
      var t = createType({type: 'fixed', name: 'Id', size: 2, three: 3});
      t.one = 1;
      assert.equal(t.getSchema(), '{"name":"Id","type":"fixed","size":2}');
      assert.equal(t.getSchema({noDeref: true}), '"Id"');
    });

    test('fromString', function () {
      var t = new builtins.FixedType({name: 'Id', size: 2});
      var s = '\x01\x02';
      var buf = new Buffer(s);
      var clone = t.fromString(JSON.stringify(s));
      assert.deepEqual(clone, buf);
    });

    test('compare buffers', function () {
      var t = createType({type: 'fixed', name: 'Id', size: 2});
      var b1 = new Buffer([1, 2]);
      assert.equal(t.compareBuffers(b1, b1), 0);
      var b2 = new Buffer([2, 2]);
      assert.equal(t.compareBuffers(b1, b2), -1);
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

    testType(builtins.MapType, data, schemas);

    test('get values type', function () {
      var t = new builtins.MapType({type: 'map', values: 'int'});
      assert.deepEqual(t.getValuesType(), createType('int'));
    });

    test('write int', function () {
      var t = new builtins.MapType({type: 'map', values: 'int'});
      var buf = t.toBuffer({'\x01': 3, '\x02': 4});
      assert.deepEqual(buf, new Buffer([4, 2, 1, 6, 2, 2, 8, 0]));
    });

    test('read long', function () {
      var t = new builtins.MapType({type: 'map', values: 'long'});
      var buf = new Buffer([4, 2, 1, 6, 2, 2, 8, 0]);
      assert.deepEqual(t.fromBuffer(buf), {'\x01': 3, '\x02': 4});
    });

    test('read with sizes', function () {
      var t = new builtins.MapType({type: 'map', values: 'int'});
      var buf = new Buffer([1,6,2,97,2,0]);
      assert.deepEqual(t.fromBuffer(buf), {a: 1});
    });

    test('skip', function () {
      var v1 = createType({
        name: 'Foo',
        type: 'record',
        fields: [
          {name: 'map', type: {type: 'map', values: 'int'}},
          {name: 'val', type: 'int'}
        ]
      });
      var v2 = createType({
        name: 'Foo',
        type: 'record',
        fields: [{name: 'val', type: 'int'}]
      });
      var b1 = new Buffer([2,2,97,2,0,6]); // Without sizes.
      var b2 = new Buffer([1,6,2,97,2,0,6]); // With sizes.
      var resolver = v2.createResolver(v1);
      assert.deepEqual(v2.fromBuffer(b1, resolver), {val: 3});
      assert.deepEqual(v2.fromBuffer(b2, resolver), {val: 3});
    });

    test('resolve int > long', function () {
      var t1 = new builtins.MapType({type: 'map', values: 'int'});
      var t2 = new builtins.MapType({type: 'map', values: 'long'});
      var resolver = t2.createResolver(t1);
      var obj = {one: 1, two: 2};
      var buf = t1.toBuffer(obj);
      assert.deepEqual(t2.fromBuffer(buf, resolver), obj);
    });

    test('resolve double > double', function () {
      var t = new builtins.MapType({type: 'map', values: 'double'});
      var resolver = t.createResolver(t);
      var obj = {one: 1, two: 2};
      var buf = t.toBuffer(obj);
      assert.deepEqual(t.fromBuffer(buf, resolver), obj);
    });

    test('resolve invalid', function () {
      var t1 = new builtins.MapType({type: 'map', values: 'int'});
      var t2 = new builtins.MapType({type: 'map', values: 'string'});
      assert.throws(function () { t2.createResolver(t1); });
      t2 = new builtins.ArrayType({type: 'array', items: 'string'});
      assert.throws(function () { t2.createResolver(t1); });
    });

    test('resolve fixed', function () {
      var t1 = createType({
        type: 'map', values: {name: 'Id', type: 'fixed', size: 2}
      });
      var t2 = createType({
        type: 'map', values: {
          name: 'Id2', aliases: ['Id'], type: 'fixed', size: 2
        }
      });
      var resolver = t2.createResolver(t1);
      var obj = {one: new Buffer([1, 2])};
      var buf = t1.toBuffer(obj);
      assert.deepEqual(t2.fromBuffer(buf, resolver), obj);
    });

    test('clone', function () {
      var t = new builtins.MapType({type: 'map', values: 'int'});
      var o = {one: 1, two: 2};
      var c;
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
      var t = new builtins.MapType({type: 'map', values: 'bytes'});
      var o = {one: {type: 'Buffer', data: [1]}};
      assert.throws(function () { t.clone(o, {}); });
      assert.throws(function () { t.clone(o); });
      var c = t.clone(o, {coerceBuffers: true});
      assert.deepEqual(c, {one: new Buffer([1])});
    });

    test('compare buffers', function () {
      var t = new builtins.MapType({type: 'map', values: 'bytes'});
      var b1 = t.toBuffer({});
      assert.throws(function () { t.compareBuffers(b1, b1); });
    });

    test('isValid hook', function () {
      var t = new builtins.MapType({type: 'map', values: 'int'});
      var o = {one: 1, two: 'deux', three: null, four: 4};
      var errs = {};
      assert(!t.isValid(o, {errorHook: hook}));
      assert.deepEqual(errs, {two: 'deux', three: null});

      function hook(path, obj, type) {
        assert.strictEqual(type, t.getValuesType());
        assert.equal(path.length, 1);
        errs[path[0]] = obj;
      }
    });

    test('getName', function () {
      var t = new builtins.MapType({type: 'map', values: 'int'});
      assert.strictEqual(t.getName(), undefined);
      assert.strictEqual(t.getName(true), 'map');
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

    testType(builtins.ArrayType, data, schemas);

    test('get items type', function () {
      var t = new builtins.ArrayType({type: 'array', items: 'int'});
      assert.deepEqual(t.getItemsType(), createType('int'));
    });

    test('read with sizes', function () {
      var t = new builtins.ArrayType({type: 'array', items: 'int'});
      var buf = new Buffer([1,2,2,0]);
      assert.deepEqual(t.fromBuffer(buf), [1]);
    });

    test('skip', function () {
      var v1 = createType({
        name: 'Foo',
        type: 'record',
        fields: [
          {name: 'array', type: {type: 'array', items: 'int'}},
          {name: 'val', type: 'int'}
        ]
      });
      var v2 = createType({
        name: 'Foo',
        type: 'record',
        fields: [{name: 'val', type: 'int'}]
      });
      var b1 = new Buffer([2,2,0,6]); // Without sizes.
      var b2 = new Buffer([1,2,2,0,6]); // With sizes.
      var resolver = v2.createResolver(v1);
      assert.deepEqual(v2.fromBuffer(b1, resolver), {val: 3});
      assert.deepEqual(v2.fromBuffer(b2, resolver), {val: 3});
    });

    test('resolve string items to bytes items', function () {
      var t1 = new builtins.ArrayType({type: 'array', items: 'string'});
      var t2 = new builtins.ArrayType({type: 'array', items: 'bytes'});
      var resolver = t2.createResolver(t1);
      var obj = ['\x01\x02'];
      var buf = t1.toBuffer(obj);
      assert.deepEqual(t2.fromBuffer(buf, resolver), [new Buffer([1, 2])]);
    });

    test('resolve invalid', function () {
      var t1 = new builtins.ArrayType({type: 'array', items: 'string'});
      var t2 = new builtins.ArrayType({type: 'array', items: 'long'});
      assert.throws(function () { t2.createResolver(t1); });
      t2 = new builtins.MapType({type: 'map', values: 'string'});
      assert.throws(function () { t2.createResolver(t1); });
    });

    test('clone', function () {
      var t = new builtins.ArrayType({type: 'array', items: 'int'});
      var o = [1, 2];
      var c;
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
      var t = createType({
        type: 'array',
        items: {type: 'fixed', name: 'Id', size: 2}
      });
      var o = [{type: 'Buffer', data: [1, 2]}];
      assert.throws(function () { t.clone(o); });
      assert.throws(function () { t.clone(o, {}); });
      var c = t.clone(o, {coerceBuffers: true});
      assert.deepEqual(c, [new Buffer([1, 2])]);
    });

    test('compare buffers', function () {
      var t = createType({type: 'array', items: 'int'});
      assert.equal(t.compareBuffers(t.toBuffer([]), t.toBuffer([])), 0);
      assert.equal(t.compareBuffers(t.toBuffer([1, 2]), t.toBuffer([])), 1);
      assert.equal(t.compareBuffers(t.toBuffer([1]), t.toBuffer([1, -1])), -1);
      assert.equal(t.compareBuffers(t.toBuffer([1]), t.toBuffer([2])), -1);
      assert.equal(t.compareBuffers(t.toBuffer([1, 2]), t.toBuffer([1])), 1);
    });

    test('compare', function () {
      var t = createType({type: 'array', items: 'int'});
      assert.equal(t.compare([], []), 0);
      assert.equal(t.compare([], [-1]), -1);
      assert.equal(t.compare([1], [1]), 0);
      assert.equal(t.compare([2], [1, 2]), 1);
    });

    test('isValid hook invalid array', function () {
      var t = createType({type: 'array', items: 'int'});
      var hookCalled = false;
      assert(!t.isValid({}, {errorHook: hook}));
      assert(hookCalled);

      function hook(path, obj, type) {
        assert.strictEqual(type, t);
        assert.deepEqual(path, []);
        hookCalled = true;
      }
    });

    test('isValid hook invalid elems', function () {
      var t = createType({type: 'array', items: 'int'});
      var paths = [];
      assert(!t.isValid([0, 3, 'hi', 5, 'hey'], {errorHook: hook}));
      assert.deepEqual(paths, [['2'], ['4']]);

      function hook(path, obj, type) {
        assert.strictEqual(type, t.getItemsType());
        assert.equal(typeof obj, 'string');
        paths.push(path);
      }
    });

    test('isValid hook reentrant', function () {
      var t = new builtins.ArrayType({
        items: new builtins.ArrayType({items: 'int'})
      });
      var a1 = [[1, 3], ['a', 2, 'c'], [3, 'b']];
      var a2 = [[1, 3]];
      var paths = [];
      assert(!t.isValid(a1, {errorHook: hook}));
      assert.deepEqual(paths, [['1', '0'], ['1', '2'], ['2', '1']]);

      function hook(path, any, type, val) {
        paths.push(path);
        assert.strictEqual(val, a1);
        assert(t.isValid(a2, {errorHook: hook}));
      }
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

    testType(builtins.RecordType, data, schemas);

    test('duplicate field names', function () {
      assert.throws(function () {
        createType({
          type: 'record',
          name: 'Person',
          fields: [{name: 'age', type: 'int'}, {name: 'age', type: 'float'}]
        });
      });
    });

    test('default constructor', function () {
      var type = createType({
        type: 'record',
        name: 'Person',
        fields: [{name: 'age', type: 'int', 'default': 25}]
      });
      var Person = type.getRecordConstructor();
      var p = new Person();
      assert.equal(p.age, 25);
      assert.strictEqual(p.constructor, Person);
    });

    test('default check & write', function () {
      var type = createType({
        type: 'record',
        name: 'Person',
        fields: [
          {name: 'age', type: 'int', 'default': 25},
          {name: 'name', type: 'string', 'default': '\x01'}
        ]
      });
      assert.deepEqual(type.toBuffer({}), new Buffer([50, 2, 1]));
    });

    test('fixed string default', function () {
      var s = '\x01\x04';
      var b = new Buffer(s);
      var type = createType({
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
      var obj = new (type.getRecordConstructor())();
      assert.deepEqual(obj.id, new Buffer([1, 4]));
      assert.deepEqual(type.toBuffer({}), b);
    });

    test('fixed buffer invalid default', function () {
      assert.throws(function () {
        createType({
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
      });
    });

    test('union invalid default', function () {
      assert.throws(function () {
        createType({
          type: 'record',
          name: 'Person',
          fields: [{name: 'name', type: ['null', 'string'], 'default': ''}]
        });
      });
    });

    test('record default', function () {
      var d = {street: null, zip: 123};
      var attrs = {
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
      var Person, person;
      // Wrapped
      Person = createType(attrs, {wrapUnions: true}).getRecordConstructor();
      person = new Person();
      assert.deepEqual(person.address, {street: null, zip: {'int': 123}});
      // Unwrapped.
      Person = createType(attrs).getRecordConstructor();
      person = new Person();
      assert.deepEqual(person.address, {street: null, zip: 123});
    });

    test('record keyword field name', function () {
      var type = createType({
        type: 'record',
        name: 'Person',
        fields: [{name: 'null', type: 'int'}]
      });
      var Person = type.getRecordConstructor();
      assert.deepEqual(new Person(2), {'null': 2});
    });

    test('record isValid', function () {
      var type = createType({
        type: 'record',
        name: 'Person',
        fields: [{name: 'age', type: 'int'}]
      });
      var Person = type.getRecordConstructor();
      assert((new Person(20)).isValid());
      assert(!(new Person()).isValid());
      assert(!(new Person('a')).isValid());
    });

    test('record toBuffer', function () {
      var type = createType({
        type: 'record',
        name: 'Person',
        fields: [{name: 'age', type: 'int'}]
      });
      var Person = type.getRecordConstructor();
      assert.deepEqual((new Person(48)).toBuffer(), new Buffer([96]));
      assert.throws(function () { (new Person()).toBuffer(); });
    });

    test('record compare', function () {
      var P = createType({
        type: 'record',
        name: 'Person',
        fields: [
          {name: 'data', type: {type: 'map', values: 'int'}, order:'ignore'},
          {name: 'age', type: 'int'}
        ]
      }).getRecordConstructor();
      var p1 = new P({}, 1);
      var p2 = new P({}, 2);
      assert.equal(p1.compare(p2), -1);
      assert.equal(p2.compare(p2), 0);
      assert.equal(p2.compare(p1), 1);
    });

    test('Record type', function () {
      var type = createType({
        type: 'record',
        name: 'Person',
        fields: [{name: 'age', type: 'int'}]
      });
      var Person = type.getRecordConstructor();
      assert.strictEqual(Person.getType(), type);
    });

    test('mutable defaults', function () {
      var Person = createType({
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
      var p1 = new Person(undefined);
      assert.deepEqual(p1.friends, []);
      p1.friends.push('ann');
      var p2 = new Person(undefined);
      assert.deepEqual(p2.friends, []);
    });

    test('resolve alias', function () {
      var v1 = createType({
        type: 'record',
        name: 'Person',
        fields: [{name: 'name', type: 'string'}]
      });
      var p = v1.random();
      var buf = v1.toBuffer(p);
      var v2 = createType({
        type: 'record',
        name: 'Human',
        aliases: ['Person'],
        fields: [{name: 'name', type: 'string'}]
      });
      var resolver = v2.createResolver(v1);
      assert.deepEqual(v2.fromBuffer(buf, resolver), p);
      var v3 = createType({
        type: 'record',
        name: 'Human',
        fields: [{name: 'name', type: 'string'}]
      });
      assert.throws(function () { v3.createResolver(v1); });
    });

    test('resolve alias with namespace', function () {
      var v1 = createType({
        type: 'record',
        name: 'Person',
        namespace: 'earth',
        fields: [{name: 'name', type: 'string'}]
      });
      var v2 = createType({
        type: 'record',
        name: 'Human',
        aliases: ['Person'],
        fields: [{name: 'name', type: 'string'}]
      });
      assert.throws(function () { v2.createResolver(v1); });
      var v3 = createType({
        type: 'record',
        name: 'Human',
        aliases: ['earth.Person'],
        fields: [{name: 'name', type: 'string'}]
      });
      assert.doesNotThrow(function () { v3.createResolver(v1); });
    });

    test('resolve skip field', function () {
      var v1 = createType({
        type: 'record',
        name: 'Person',
        fields: [
          {name: 'age', type: 'int'},
          {name: 'name', type: 'string'}
        ]
      });
      var p = {age: 25, name: 'Ann'};
      var buf = v1.toBuffer(p);
      var v2 = createType({
        type: 'record',
        name: 'Person',
        fields: [{name: 'name', type: 'string'}]
      });
      var resolver = v2.createResolver(v1);
      assert.deepEqual(v2.fromBuffer(buf, resolver), {name: 'Ann'});
    });

    test('resolve new field', function () {
      var v1 = createType({
        type: 'record',
        name: 'Person',
        fields: [{name: 'name', type: 'string'}]
      });
      var p = {name: 'Ann'};
      var buf = v1.toBuffer(p);
      var v2 = createType({
        type: 'record',
        name: 'Person',
        fields: [
          {name: 'age', type: 'int', 'default': 25},
          {name: 'name', type: 'string'}
        ]
      });
      var resolver = v2.createResolver(v1);
      assert.deepEqual(v2.fromBuffer(buf, resolver), {name: 'Ann', age: 25});
    });

    test('resolve new field no default', function () {
      var v1 = createType({
        type: 'record',
        name: 'Person',
        fields: [{name: 'name', type: 'string'}]
      });
      var v2 = createType({
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
      var v1 = createType({
        type: 'record',
        name: 'Person',
        fields: [{name: 'friends', type: {type: 'array', items: 'Person'}}]
      });
      var v2 = createType({
        type: 'record',
        name: 'Person',
        fields: [{name: 'age', type: 'int', 'default': -1}]
      });
      var resolver = v2.createResolver(v1);
      var p1 = {friends: [{friends: []}]};
      var p2 = v2.fromBuffer(v1.toBuffer(p1), resolver);
      assert.deepEqual(p2, {age: -1});
    });

    test('resolve to recursive schema', function () {
      var v1 = createType({
        type: 'record',
        name: 'Person',
        fields: [{name: 'age', type: 'int', 'default': -1}]
      });
      var v2 = createType({
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
      var resolver = v2.createResolver(v1);
      var p1 = {age: 25};
      var p2 = v2.fromBuffer(v1.toBuffer(p1), resolver);
      assert.deepEqual(p2, {friends: []});
    });

    test('resolve from both recursive schema', function () {
      var v1 = createType({
        type: 'record',
        name: 'Person',
        fields: [
          {name: 'friends', type: {type: 'array', items: 'Person'}},
          {name: 'age', type: 'int'}
        ]
      });
      var v2 = createType({
        type: 'record',
        name: 'Person',
        fields: [{name: 'friends', type: {type: 'array', items: 'Person'}}]
      });
      var resolver = v2.createResolver(v1);
      var p1 = {friends: [{age: 1, friends: []}], age: 10};
      var p2 = v2.fromBuffer(v1.toBuffer(p1), resolver);
      assert.deepEqual(p2, {friends: [{friends: []}]});
    });

    test('resolve multiple matching aliases', function () {
      var v1 = createType({
        type: 'record',
        name: 'Person',
        fields: [
          {name: 'phone', type: 'string'},
          {name: 'number', type: 'string'}
        ]
      });
      var v2 = createType({
        type: 'record',
        name: 'Person',
        fields: [{name: 'number', type: 'string', aliases: ['phone']}]
      });
      assert.throws(function () { v2.createResolver(v1); });
    });

    test('getName', function () {
      var t = createType({
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
      assert.equal(t.getTypeName(), 'record');
    });

    test('getSchema', function () {
      var t = createType({
        type: 'record',
        name: 'Person',
        doc: 'Hi!',
        namespace: 'earth',
        aliases: ['Human'],
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
      assert.equal(
        t.getSchema(),
        '{"name":"earth.Person","type":"record","fields":[{"name":"friends","type":{"type":"array","items":"string"}},{"name":"age","type":"int"}]}'
      );
      assert.deepEqual(JSON.parse(t.getSchema({exportAttrs: true})), {
        type: 'record',
        name: 'earth.Person',
        aliases: ['earth.Human'],
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
      assert.equal(t.getSchema({noDeref: true}), '"earth.Person"');
    });

    test('getSchema recursive schema', function () {
      var t = createType({
        type: 'record',
        name: 'Person',
        namespace: 'earth',
        fields: [
          {name: 'friends', type: {type: 'array', items: 'Person'}},
        ]
      });
      assert.equal(
        t.getSchema(),
        '{"name":"earth.Person","type":"record","fields":[{"name":"friends","type":{"type":"array","items":"earth.Person"}}]}'
      );
      assert.equal(t.getSchema({noDeref: true}), '"earth.Person"');
    });

    test('fromString', function () {
      var t = createType({
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
      var T = createType({
        type: 'record',
        name: 'Person',
        fields: [{name: 'pwd', type: 'bytes'}]
      }).getRecordConstructor();
      var r = new T(new Buffer([1, 2]));
      assert.equal(r.toString(), T.getType().toString(r));
    });

    test('clone', function () {
      var t = createType({
        type: 'record',
        name: 'Person',
        fields: [{name: 'age', type: 'int'}, {name: 'name', type: 'string'}]
      });
      var Person = t.getRecordConstructor();
      var o = {age: 25, name: 'Ann'};
      var c;
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
      var t = createType({
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
      var t = createType({
        type: 'record',
        name: 'Person',
        fields: [{name: 'age', type: 'int'}, {name: 'name', type: 'string'}]
      });
      var o = {name: 'Ann', age: 25};
      var c = t.clone(o, {fieldHook: function (f, o, r) {
        assert.strictEqual(r, t);
        return f._type instanceof builtins.StringType ? o.toUpperCase() : o;
      }});
      assert.deepEqual(c, {name: 'ANN', age: 25});
    });

    test('clone missing fields', function () {
      var t = createType({
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
        createType({
          type: 'record',
          name: 'Person',
          fields: [
            {name: 'name', type: ['null', 'string'], 'default': 'Bob'}
          ]
        }, {wrapUnions: false});
      });
      var attrs = {
        type: 'record',
        name: 'Person',
        fields: [
          {name: 'name', type: ['string', 'null'], 'default': 'Bob'}
        ]
      };
      var t = createType(attrs, {wrapUnions: false});
      var o = {name: 'Ann'};
      assert.deepEqual(t.clone(o), o);
      assert.deepEqual(t.clone({}), {name: 'Bob'});
      assert.deepEqual(JSON.parse(t.getSchema({exportAttrs: true})), attrs);
    });

    test('wrapped union field default', function () {
      assert.throws(function () {
        createType({
          type: 'record',
          name: 'Person',
          fields: [
            {name: 'name', type: ['null', 'string'], 'default': 'Bob'}
          ]
        }, {wrapUnions: true});
      });
      var attrs = {
        type: 'record',
        name: 'Person',
        fields: [
          {name: 'name', type: ['string', 'null'], 'default': 'Bob'}
        ]
      };
      var t = createType(attrs, {wrapUnions: true});
      var o = {name: {string: 'Ann'}};
      assert.deepEqual(t.clone(o), o);
      assert.deepEqual(t.clone({}), {name: {string: 'Bob'}});
      assert.deepEqual(JSON.parse(t.getSchema({exportAttrs: true})), attrs);
    });

    test('get full name & aliases', function () {
      var t = createType({
        type: 'record',
        name: 'Person',
        namespace: 'a',
        fields: [{name: 'age', type: 'int'}, {name: 'name', type: 'string'}]
      });
      assert.equal(t.getName(), 'a.Person');
      assert.deepEqual(t.getAliases(), []);
    });

    test('field getters', function () {
      var t = createType({
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
      var fields = t.getFields();
      assert.deepEqual(fields[0].getAliases(), []);
      assert.deepEqual(fields[1].getAliases(), ['word']);
      assert.equal(fields[1].getName(), 'name'); // Namespaces are ignored.
      assert.deepEqual(fields[1].getType(), createType('string'));
      fields.push('null');
      assert.equal(t.getFields().length, 2); // No change.
    });

    test('field order', function () {
      var t = createType({
        type: 'record',
        name: 'Person',
        fields: [{name: 'age', type: 'int'}]
      });
      var field = t.getFields()[0];
      assert.equal(field.getOrder(), 'ascending'); // Default.
    });

    test('compare buffers default order', function () {
      var t = createType({
        type: 'record',
        name: 'Person',
        fields: [
          {name: 'age', type: 'long'},
          {name: 'name', type: 'string'},
          {name: 'weight', type: 'float'},
        ]
      });
      var b1 = t.toBuffer({age: 20, name: 'Ann', weight: 0.5});
      assert.equal(t.compareBuffers(b1, b1), 0);
      var b2 = t.toBuffer({age: 20, name: 'Bob', weight: 0});
      assert.equal(t.compareBuffers(b1, b2), -1);
      var b3 = t.toBuffer({age: 19, name: 'Carrie', weight: 0});
      assert.equal(t.compareBuffers(b1, b3), 1);
    });

    test('compare buffers custom order', function () {
      var t = createType({
        type: 'record',
        name: 'Person',
        fields: [
          {name: 'meta', type: {type: 'map', values: 'int'}, order: 'ignore'},
          {name: 'name', type: 'string', order: 'descending'}
        ]
      });
      var b1 = t.toBuffer({meta: {}, name: 'Ann'});
      assert.equal(t.compareBuffers(b1, b1), 0);
      var b2 = t.toBuffer({meta: {foo: 1}, name: 'Bob'});
      assert.equal(t.compareBuffers(b1, b2), 1);
      var b3 = t.toBuffer({meta: {foo: 0}, name: 'Alex'});
      assert.equal(t.compareBuffers(b1, b3), -1);
    });

    test('compare buffers invalid order', function () {
      assert.throws(function () { createType({
        type: 'record',
        name: 'Person',
        fields: [{name: 'age', type: 'int', order: 'up'}]
      }); });
    });

    test('error type', function () {
      var t = createType({
        type: 'error',
        name: 'Ouch',
        fields: [{name: 'name', type: 'string'}]
      });
      var E = t.getRecordConstructor();
      var err = new E('MyError');
      assert(err instanceof Error);
    });

    test('anonymous error type', function () {
      assert.doesNotThrow(function () { createType({
        type: 'error',
        fields: [{name: 'name', type: 'string'}]
      }); });
    });

    test('resolve error type', function () {
      var t1 = createType({
        type: 'error',
        name: 'Ouch',
        fields: [{name: 'name', type: 'string'}]
      });
      var t2 = createType({
        type: 'error',
        name: 'OuchAgain',
        aliases: ['Ouch'],
        fields: [{name: 'code', type: 'int', 'default': -1}]
      });
      var res = t2.createResolver(t1);
      var err1 = t1.random();
      var err2 = t2.fromBuffer(t1.toBuffer(err1), res);
      assert.deepEqual(err2, {code: -1});
    });

    test('isValid hook', function () {
      var t = createType({
        type: 'record',
        name: 'Person',
        fields: [
          {name: 'age', type: 'int'},
          {name: 'names', type: {type: 'array', items: 'string'}}
        ]
      });
      var hasErr = false;
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
      var t = createType({type: 'record', name: 'Person', fields: []});
      assert(t.isValid({}));
    });

    test('isValid no undeclared fields', function () {
      var t = createType({
        type: 'record',
        fields: [{name: 'foo', type: 'int'}]
      });
      var obj = {foo: 1, bar: 'bar'};
      assert(t.isValid(obj));
      assert(!t.isValid(obj, {noUndeclaredFields: true}));
      assert(t.isValid({foo: 23}, {noUndeclaredFields: true}));
    });

    test('qualified name namespacing', function () {
      var t = createType({
        type: 'record',
        name: '.Foo',
        fields: [
          {name: 'id', type: {type: 'record', name: 'Bar', fields: []}}
        ]
      }, {namespace: 'bar'});
      assert.equal(t.getField('id').getType().getName(), 'Bar');
    });

  });

  suite('AbstractLongType', function () {

    var fastLongType = new builtins.LongType();

    suite('unpacked', function () {

      var slowLongType = builtins.LongType.__with({
        fromBuffer: function (buf) {
          var neg = buf[7] >> 7;
          if (neg) { // Negative number.
            invert(buf);
          }
          var n = buf.readInt32LE(0) + Math.pow(2, 32) * buf.readInt32LE(4);
          if (neg) {
            invert(buf);
            n = -n - 1;
          }
          return n;
        },
        toBuffer: function (n) {
          var buf = new Buffer(8);
          var neg = n < 0;
          if (neg) {
            invert(buf);
            n = -n - 1;
          }
          buf.writeInt32LE(n | 0);
          var h = n / Math.pow(2, 32) | 0;
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

      test('encode', function () {
        [123, -1, 321414, 900719925474090].forEach(function (n) {
          assert.deepEqual(slowLongType.toBuffer(n), fastLongType.toBuffer(n));
        });
      });

      test('decode', function () {
        [123, -1, 321414, 900719925474090].forEach(function (n) {
          var buf = fastLongType.toBuffer(n);
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
        var s = 'hi';
        var errs = [];
        assert(!slowLongType.isValid(s, {errorHook: hook}));
        assert.deepEqual(errs, [s]);
        assert.throws(function () { slowLongType.toBuffer(s); });

        function hook(path, obj, type) {
          assert.strictEqual(type, slowLongType);
          assert.equal(path.length, 0);
          errs.push(obj);
        }
      });

    });

    suite('packed', function () {

      var slowLongType = builtins.LongType.__with({
        fromBuffer: function (buf) {
          var tap = new Tap(buf);
          return tap.readLong();
        },
        toBuffer: function (n) {
          var buf = new Buffer(10);
          var tap = new Tap(buf);
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
          var buf = fastLongType.toBuffer(n);
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

    });

    test('incomplete buffer', function () {
      // Check that `fromBuffer` doesn't get called.
      var slowLongType = new builtins.LongType.__with({
        fromBuffer: function () { throw new Error('no'); },
        toBuffer: null,
        fromJSON: null,
        toJSON: null,
        isValid: null,
        compare: null
      });
      var buf = fastLongType.toBuffer(12314);
      assert.deepEqual(
        slowLongType.decode(buf.slice(0, 1)),
        {value: undefined, offset: -1}
      );
    });

  });

  suite('LogicalType', function () {

    function DateType(attrs, opts) {
      types.builtins.LogicalType.call(this, attrs, opts);
      if (!types.Type.isType(this.getUnderlyingType(), 'long', 'string')) {
        throw new Error('invalid underlying date type');
      }
    }
    util.inherits(DateType, types.builtins.LogicalType);

    DateType.prototype._fromValue = function (val) { return new Date(val); };

    DateType.prototype._toValue = function (date) {
      if (!(date instanceof Date)) {
        return undefined;
      }
      if (this.getUnderlyingType().getTypeName() === 'long') {
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

    function AgeType(attrs, opts) {
      types.builtins.LogicalType.call(this, attrs, opts);
    }
    util.inherits(AgeType, types.builtins.LogicalType);

    AgeType.prototype._fromValue = function (val) { return val; };

    AgeType.prototype._toValue = function (any) {
      if (typeof any == 'number' && any >= 0) {
        return any;
      }
    };

    var logicalTypes = {age: AgeType, date: DateType};

    test('valid type', function () {
      var t = createType({
        type: 'long',
        logicalType: 'date'
      }, {logicalTypes: logicalTypes});
      assert(t instanceof DateType);
      assert(/<(Date|Logical)Type {.+}>/.test(t.inspect())); // IE.
      assert(t.getUnderlyingType() instanceof builtins.LongType);
      assert(t.isValid(t.random()));
      var d = new Date(123);
      assert.equal(t.toString(d), '123');
      assert.strictEqual(t.getName(), undefined);
      assert.equal(t.getName(true), 'long');
      assert.equal(t.getTypeName(), 'logical:date');
      assert.deepEqual(t.fromString('123'), d);
      assert.deepEqual(t.clone(d), d);
      assert.equal(t.compare(d, d), 0);
      assert.equal(t.getSchema(), '"long"');
    });

    test('invalid type', function () {
      var attrs = {
        type: 'int',
        logicalType: 'date'
      };
      var t;
      t = createType(attrs); // Missing.
      assert(t instanceof builtins.IntType);
      t = createType(attrs, {logicalTypes: logicalTypes}); // Invalid.
      assert(t instanceof builtins.IntType);
      assert.throws(function () {
        createType(attrs, {
          logicalTypes: logicalTypes,
          assertLogicalTypes: true
        });
      });
    });

    test('missing type', function () {
      var t = createType({
        type: 'long',
        logicalType: 'date'
      }, {logicalTypes: {}});
      assert(t.getTypeName(), 'long');
    });

    test('nested types', function () {
      var attrs = {
        name: 'Person',
        type: 'record',
        fields: [
          {name: 'age', type: {type: 'int', logicalType: 'age'}},
          {name: 'time', type: {type: 'long', logicalType: 'date'}}
        ]
      };
      var base = createType(attrs);
      var derived = createType(attrs, {logicalTypes: logicalTypes});
      var fields = derived.getFields();
      var ageType = fields[0].getType();
      ageType.constructor = undefined; // Mimic missing constructor name.
      assert(ageType instanceof AgeType);
      assert.equal(
        ageType.inspect(),
        '<LogicalType {"type":"int","logicalType":"age"}>'
      );
      assert(fields[1].getType() instanceof DateType);
      var date = new Date(Date.now());
      var buf = base.toBuffer({age: 12, time: +date});
      var person = derived.fromBuffer(buf);
      assert.deepEqual(person.age, 12);
      assert.deepEqual(person.time, date);

      var invalid = {age: -1, time: date};
      assert.throws(function () { derived.toBuffer(invalid); });
      var hasError = false;
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

      function PersonType(attrs, opts) {
        types.builtins.LogicalType.call(this, attrs, opts);
      }
      util.inherits(PersonType, types.builtins.LogicalType);

      PersonType.prototype._fromValue = function (val) {
        return new Person(val.friends);
      };

      PersonType.prototype._toValue = function (val) { return val; };

      var attrs = {
        type: 'record',
        name: 'Person',
        logicalType: 'person',
        fields: [
          {name: 'friends', type: {type: 'array', items: 'Person'}},
        ]
      };
      var t = createType(attrs, {logicalTypes: {'person': PersonType}});

      var p1 = new Person([new Person()]);
      var buf = t.toBuffer(p1);
      var p2 = t.fromBuffer(buf);
      assert(p2 instanceof Person);
      assert(p2.friends[0] instanceof Person);
      assert.deepEqual(p2, p1);
      assert.deepEqual(
        JSON.parse(t.getSchema({exportAttrs: true})),
        attrs
      );
    });

    test('resolve underlying > logical', function () {
      var t1 = createType({type: 'string'});
      var t2 = createType({
        type: 'long',
        logicalType: 'date'
      }, {logicalTypes: logicalTypes});

      var d1 = new Date(Date.now());
      var buf = t1.toBuffer('' + d1);
      var res = t2.createResolver(t1);
      assert.throws(function () { t2.createResolver(createType('float')); });
      var d2 = t2.fromBuffer(buf, res);
      assert.deepEqual('' + d2, '' + d1); // Rounding error on date objects.
    });

    test('resolve logical > underlying', function () {
      var t1 = createType({
        type: 'long',
        logicalType: 'date'
      }, {logicalTypes: logicalTypes});
      var t2 = createType({type: 'double'}); // Note long > double too.

      var d = new Date(Date.now());
      var buf = t1.toBuffer(d);
      var res = t2.createResolver(t1);
      assert.throws(function () { createType('int').createResolver(t1); });
      assert.equal(t2.fromBuffer(buf, res), +d);
    });

    test('even integer', function () {
      function EvenIntType(attrs, opts) {
        types.builtins.LogicalType.call(this, attrs, opts);
      }
      util.inherits(EvenIntType, types.builtins.LogicalType);
      EvenIntType.prototype._fromValue = function (val) {
        if (val !== (val | 0) || val % 2) {
          throw new Error('invalid');
        }
        return val;
      };
      EvenIntType.prototype._toValue = EvenIntType.prototype._fromValue;

      var opts = {logicalTypes: {'even-integer': EvenIntType}};
      var t = createType({type: 'long', logicalType: 'even-integer'}, opts);
      assert(t.isValid(2));
      assert(!t.isValid(3));
      assert(!t.isValid('abc'));
      assert.equal(t.fromBuffer(new Buffer([4])), 2);
      assert.equal(t.clone(4), 4);
      assert.equal(t.fromString('6'), 6);
      assert.equal(t.getSchema(), '"long"');
      assert.deepEqual(
        t.getSchema({exportAttrs: true}),
        '{"type":"long","logicalType":"even-integer"}'
      );
      assert(types.Type.isType(t));
      assert(!types.Type.isType(t, 'int'));
      assert(types.Type.isType(t, 'logical'));
      assert.throws(function () { t.clone(3); });
      assert.throws(function () { t.fromString('5'); });
      assert.throws(function () { t.toBuffer(3); });
      assert.throws(function () { t.fromBuffer(new Buffer([2])); });
    });

    test('inside unwrapped union', function () {
      var t = types.createType(
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
      var t = types.createType(
        ['long', {type: 'int', logicalType: 'age'}],
        {logicalTypes: logicalTypes}
      );
      assert(t.isValid(-34));
      assert.throws(function () { t.isValid(32); }, /ambiguous/);
    });

    test('inside unwrapped union with duplicate underlying type', function () {
      function FooType(attrs, opts) {
        types.builtins.LogicalType.call(this, attrs, opts);
      }
      util.inherits(FooType, types.builtins.LogicalType);
      assert.throws(function () {
        types.createType([
          'int',
          {type: 'int', logicalType: 'foo'}
        ], {logicalTypes: {foo: FooType}, wrapUnions: false});
      }, /duplicate/);
    });

    test('inside wrapped union', function () {
      function EvenIntType(attrs, opts) {
        types.builtins.LogicalType.call(this, attrs, opts);
      }
      util.inherits(EvenIntType, types.builtins.LogicalType);
      EvenIntType.prototype._fromValue = function (val) {
        if (val !== (val | 0) || val % 2) {
          throw new Error('invalid');
        }
        return val;
      };
      EvenIntType.prototype._toValue = EvenIntType.prototype._fromValue;

      var t = types.createType(
        [{type: 'int', logicalType: 'even'}],
        {logicalTypes: {even: EvenIntType}, wrapUnions: true}
      );
      assert(t.isValid({int: 2}));
      assert(!t.isValid({int: 3}));
    });

    // Unions are slightly tricky to override with logical types since their
    // schemas aren't represented as objects.
    suite('union logical types', function () {

      var schema = [
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
        var visited = [];
        return function(attrs, opts) {
          if (Array.isArray(attrs) && !~visited.indexOf(attrs)) {
            visited.push(attrs);
            return new Type(attrs, opts);
          }
        };
      }

      test('unwrapped', function () {

        /**
        * A generic union type which exposes its values directly.
        *
        * This implementation predates the existence of the
        * `UnwrappedUnionType` currently in the built-in types. It can still be
        * used as an example to implement custom unwrapped unions (which would
        * be able to cover ambiguous unions).
        *
        */
        function UnwrappedUnionType(attrs, opts) {
          types.builtins.LogicalType.call(this, attrs, opts);
        }
        util.inherits(UnwrappedUnionType, types.builtins.LogicalType);

        UnwrappedUnionType.prototype._fromValue = function (val) {
          return val === null ? null : val[Object.keys(val)[0]];
        };

        UnwrappedUnionType.prototype._toValue = function (any) {
          return this.getUnderlyingType().clone(any, {wrapUnions: true});
        };

        var t1 = createType(
          schema,
          {typeHook: createUnionTypeHook(UnwrappedUnionType), wrapUnions: true}
        );
        var obj = {name: 'Ann', age: 23};
        assert(t1.isValid(obj));
        var buf = t1.toBuffer(obj);
        var t2 = createType(schema, {wrapUnions: true});
        assert.deepEqual(
          t2.fromBuffer(buf),
          {Person: {name: 'Ann', age: {'int': 23}}}
        );

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
        function OptionalType(attrs, opts) {
          types.builtins.LogicalType.call(this, attrs, opts);
          var type = this.getUnderlyingType().getTypes()[1];
          this._name = type.getName(true);
        }
        util.inherits(OptionalType, types.builtins.LogicalType);

        OptionalType.prototype._fromValue = function (val) {
          return val === null ? null : val[this._name];
        };

        OptionalType.prototype._toValue = function (any) {
          if (any === null) {
            return null;
          }
          var obj = {};
          obj[this._name] = any;
          return obj;
        };

        var t1 = createType(
          schema,
          {typeHook: createUnionTypeHook(OptionalType), wrapUnions: true}
        );
        var obj = {name: 'Ann', age: 23};
        assert(t1.isValid(obj));
        var buf = t1.toBuffer(obj);
        var t2 = createType(schema, {wrapUnions: true});
        assert.deepEqual(
          t2.fromBuffer(buf),
          {Person: {name: 'Ann', age: {'int': 23}}}
        );

      });

    });

  });

  suite('createType', function  () {

    test('null type', function () {
      assert.throws(function () { createType(null); });
    });

    test('unknown types', function () {
      assert.throws(function () { createType('a'); });
      assert.throws(function () { createType({type: 'b'}); });
    });

    test('namespaced type', function () {
      var type = createType({
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
      assert.equal(type._name, 'earth.Human');
      assert.equal(type._fields[0]._type._name, 'all.Id');
      assert.equal(type._fields[1]._type._name, 'all.Alien');
    });

    test('namespace scope', function () {
      var type = createType({
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
      assert.equal(type._name, 'earth.Human');
      assert.equal(type._fields[0]._type._name, 'all.Id');
      assert.equal(type._fields[1]._type._name, 'earth.Id');
    });

    test('namespace reset', function () {
      var type = createType({
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
      assert.equal(type._name, 'earth.Human');
      assert.equal(type._fields[0]._type._name, 'earth.Id');
      assert.equal(type._fields[1]._type._name, 'Id');
    });

    test('namespace reset with qualified name', function () {
      var type = createType({
        type: 'record',
        name: 'earth.Human',
        namespace: '',
        fields: [{name: 'id', type: {type: 'fixed', name: 'Id', size: 2}}]
      });
      assert.equal(type._name, 'earth.Human');
      assert.equal(type._fields[0]._type._name, 'Id');
    });

    test('absolute reference', function () {
      var type = createType({
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
      assert.equal(type._name, 'earth.Human');
      assert.equal(type._fields[1]._type._name, 'Id');
    });

    test('wrapped primitive', function () {
      var type = createType({
        type: 'record',
        name: 'Person',
        fields: [{name: 'nothing', type: {type: 'null'}}]
      });
      assert.strictEqual(type._fields[0]._type.constructor, builtins.NullType);
    });

    test('fromBuffer truncated', function () {
      var type = createType('int');
      assert.throws(function () {
        type.fromBuffer(new Buffer([128]));
      });
    });

    test('fromBuffer bad resolver', function () {
      var type = createType('int');
      assert.throws(function () {
        type.fromBuffer(new Buffer([0]), 123, {});
      });
    });

    test('fromBuffer trailing', function () {
      var type = createType('int');
      assert.throws(function () {
        type.fromBuffer(new Buffer([0, 2]));
      });
    });

    test('fromBuffer trailing with resolver', function () {
      var type = createType('int');
      var resolver = type.createResolver(createType(['int']));
      assert.equal(type.fromBuffer(new Buffer([0, 2]), resolver), 1);
    });

    test('toBuffer', function () {
      var type = createType('int');
      assert.throws(function () { type.toBuffer('abc'); });
      assert.doesNotThrow(function () { type.toBuffer(123); });
    });

    test('toBuffer and resize', function () {
      var type = createType('string');
      assert.deepEqual(type.toBuffer('\x01', 1), new Buffer([2, 1]));
    });

    test('type hook', function () {
      var refs = [];
      var ts = [];
      var o = {
        type: 'record',
        name: 'Human',
        fields: [
          {name: 'age', type: 'int'},
          {name: 'name', type: {type: 'string'}}
        ]
      };
      createType(o, {typeHook: hook});
      assert.equal(ts.length, 1);
      assert.equal(ts[0].getName(), 'Human');

      function hook(schema, opts) {
        if (~refs.indexOf(schema)) {
          // Already seen this schema.
          return;
        }
        refs.push(schema);

        var type = createType(schema, opts);
        if (type instanceof builtins.RecordType) {
          ts.push(type);
        }
        return type;
      }
    });

    test('type hook invalid return value', function () {
      assert.throws(function () {
        createType({type: 'int'}, {typeHook: hook});
      });

      function hook() { return 'int'; }
    });

    test('type hook for aliases', function () {
      var a1 = {
        type: 'record',
        name: 'R1',
        fields: [
          {name: 'r2', type: 'R2'},
        ]
      };
      var a2 = {
        type: 'record',
        name: 'R2',
        fields: [
          {name: 'r1', type: 'R1'},
        ]
      };
      var opts = {typeHook: hook};
      createType(a1, opts);
      assert.deepEqual(Object.keys(opts.registry), ['R1', 'R2']);

      function hook(name, opts) {
        if (name === 'R2') {
          return createType(a2, opts);
        }
      }
    });

    test('fingerprint', function () {
      var t = createType('int');
      var buf = new Buffer('ef524ea1b91e73173d938ade36c1db32', 'hex');
      assert.deepEqual(t.getFingerprint('md5'), buf);
      assert.deepEqual(t.getFingerprint(), buf);
    });

    test('getSchema default', function () {
      var type = createType({
        type: 'record',
        name: 'Human',
        fields: [
          {name: 'id1', type: ['string', 'null'], 'default': ''},
          {name: 'id2', type: ['null', 'string'], 'default': null}
        ]
      });
      assert.deepEqual(
        JSON.parse(type.getSchema()),
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
        createType({
          name: 'Person',
          type: 'record',
          fields: [
            {name: 'id', type: ['null', 'int'], 'default': 2}
          ]
        }, {wrapUnions: false});
      }, /invalid "null"/);
    });

    test('anonymous types', function () {
      var t = createType({
        type: 'record',
        fields: [{name: 'foo', type: 'int'}]
      });
      assert.strictEqual(t.getName(), undefined);
      assert.strictEqual(t.getName(true), 'record');
      assert(t.isValid({foo: 3}));
      assert.throws(function () {
        createType({name: '', type: 'record', fields: []});
      });
    });

  });

  suite('fromString', function () {

    test('int', function () {
      var t = createType('int');
      assert.equal(t.fromString('2'), 2);
      assert.throws(function () { t.fromString('"a"'); });
    });

    test('string', function () {
      var t = createType('string');
      assert.equal(t.fromString('"2"'), '2');
      assert.throws(function () { t.fromString('a'); });
    });

    test('coerce buffers', function () {
      var t = createType({
        name: 'Ids',
        type: 'record',
        fields: [{name: 'id1', type: {name: 'Id1', type: 'fixed', size: 2}}]
      });
      var o = {id1: new Buffer([0, 1])};
      var s = '{"id1": "\\u0000\\u0001"}';
      var c = t.fromString(s);
      assert.deepEqual(c, o);
      assert(c instanceof t.getRecordConstructor());
    });

  });

  suite('toString', function () {

    test('int', function () {
      var t = createType('int');
      assert.equal(t.toString(2), '2');
      assert.throws(function () { t.toString('a'); });
    });

  });

  suite('resolve', function () {

    test('non type', function () {
      var t = createType({type: 'map', values: 'int'});
      var obj = {type: 'map', values: 'int'};
      assert.throws(function () { t.createResolver(obj); });
    });

    test('union to valid wrapped union', function () {
      var t1 = createType(['int', 'string']);
      var t2 = createType(['null', 'string', 'long'], {wrapUnions: true});
      var resolver = t2.createResolver(t1);
      var buf = t1.toBuffer(12);
      assert.deepEqual(t2.fromBuffer(buf, resolver), {'long': 12});
    });

    test('union to invalid union', function () {
      var t1 = createType(['int', 'string']);
      var t2 = createType(['null', 'long']);
      assert.throws(function () { t2.createResolver(t1); });
    });

    test('wrapped union to non union', function () {
      var t1 = createType(['int', 'long'], {wrapUnions: true});
      var t2 = createType('long');
      var resolver = t2.createResolver(t1);
      var buf = t1.toBuffer({'int': 12});
      assert.equal(t2.fromBuffer(buf, resolver), 12);
      buf = new Buffer([4, 0]);
      assert.throws(function () { t2.fromBuffer(buf, resolver); });
    });

    test('union to non union', function () {
      var t1 = createType(['bytes', 'string']);
      var t2 = createType('bytes');
      var resolver = t2.createResolver(t1);
      var buf = t1.toBuffer('\x01\x02');
      assert.deepEqual(t2.fromBuffer(buf, resolver), new Buffer([1, 2]));
    });

    test('union to invalid non union', function () {
      var t1 = createType(['int', 'long'], {wrapUnions: true});
      var t2 = createType('int');
      assert.throws(function() { t2.createResolver(t1); });
    });

    test('anonymous types', function () {
      var t1 = createType({type: 'fixed', size: 2});
      var t2 = createType(
        {type: 'fixed', size: 2, namespace: 'foo', aliases: ['Id']}
      );
      var t3 = createType({type: 'fixed', size: 2, name: 'foo.Id'});
      assert.throws(function () { t1.createResolver(t3); });
      assert.doesNotThrow(function () { t2.createResolver(t3); });
      assert.doesNotThrow(function () { t3.createResolver(t1); });
    });

  });

  suite('type references', function () {

    test('null', function () {
      assert.throws(function () { createType(null); }, /did you mean/);
    });

    test('existing', function () {
      var type = createType({
        type: 'record',
        name: 'Person',
        fields: [{name: 'so', type: 'Person'}]
      });
      assert.strictEqual(type, type._fields[0]._type);
    });

    test('namespaced', function () {
      var type = createType({
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
      assert.equal(type._name, 'Person');
      assert.equal(type._fields[0]._type._name, 'a.Person');
    });

    test('namespaced global', function () {
      var type = createType({
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
      assert.equal(type._fields[0]._type.getName(), 'earth.Gender');
    });

    test('redefining', function () {
      assert.throws(function () {
        createType({
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
        createType({
          type: 'record',
          name: 'Person',
          fields: [{name: 'so', type: 'Friend'}]
        });
      });
    });

    test('redefining primitive', function () {
      assert.throws( // Unqualified.
        function () { createType({type: 'fixed', name: 'int', size: 2}); }
      );
      assert.throws( // Qualified.
        function () {
          createType({type: 'fixed', name: 'int', size: 2, namespace: 'a'});
        }
      );
    });

    test('aliases', function () {
      var type = createType({
        type: 'record',
        name: 'Person',
        namespace: 'a',
        aliases: ['Human', 'b.Being'],
        fields: [{name: 'age', type: 'int'}]
      });
      assert.deepEqual(type._aliases, ['a.Human', 'b.Being']);
    });

    test('invalid', function () {
      // Name.
      assert.throws(function () {
        createType({type: 'fixed', name: 'ID$', size: 3});
      });
      // Namespace.
      assert.throws(function () {
        createType({type: 'fixed', name: 'ID', size: 3, namespace: '1a'});
      });
      // Qualified.
      assert.throws(function () {
        createType({type: 'fixed', name: 'a.2.ID', size: 3});
      });
    });

    test('anonymous types', function () {
      var t = createType([
        {type: 'enum', symbols: ['A']},
        'int',
        {type: 'record', fields: [{name: 'foo', type: 'string'}]}
      ]);
      assert.equal(
        t.getSchema(),
        '[{"type":"enum","symbols":["A"]},"int",{"type":"record","fields":[{"name":"foo","type":"string"}]}]'
      );
    });

  });

  suite('decode', function () {

    test('long valid', function () {
      var t = createType('long');
      var buf = new Buffer([0, 128, 2, 0]);
      var res = t.decode(buf, 1);
      assert.deepEqual(res, {value: 128, offset: 3});
    });

    test('bytes invalid', function () {
      var t = createType('bytes');
      var buf = new Buffer([4, 1]);
      var res = t.decode(buf, 0);
      assert.deepEqual(res, {value: undefined, offset: -1});
    });

  });

  suite('encode', function () {

    test('int valid', function () {
      var t = createType('int');
      var buf = new Buffer(2);
      buf.fill(0);
      var n = t.encode(5, buf, 1);
      assert.equal(n, 2);
      assert.deepEqual(buf, new Buffer([0, 10]));
    });

    test('too short', function () {
      var t = createType('string');
      var buf = new Buffer(1);
      var n = t.encode('\x01\x02', buf, 0);
      assert.equal(n, -2);
    });

    test('invalid', function () {
      var t = createType('float');
      var buf = new Buffer(2);
      assert.throws(function () { t.encode('hi', buf, 0); });
    });

    test('invalid offset', function () {
      var t = createType('string');
      var buf = new Buffer(2);
      assert.throws(function () { t.encode('hi', buf, -1); });
    });

  });

  suite('inspect', function () {

    test('type', function () {
      assert.equal(createType('int').inspect(), '<IntType>');
      assert.equal(
        createType({type: 'map', values: 'string'}).inspect(),
        '<MapType {"values":"string"}>'
      );
      assert.equal(
        createType({type: 'fixed', name: 'Id', size: 2}).inspect(),
        '<FixedType "Id">'
      );
    });

    test('field', function () {
      var type = createType({
        type: 'record',
        name: 'Person',
        fields: [{name: 'age', type: 'int'}]
      });
      var field = type.getFields()[0];
      assert.equal(field.inspect().name, 'age');
    });

    test('resolver', function () {
      var t1 = createType('int');
      var t2 = createType('double');
      var resolver = t2.createResolver(t1);
      assert.equal(resolver.inspect(), '<Resolver>');
    });

  });

  test('equals', function () {
    var t1 = createType('int');
    var t2 = createType('int');
    assert(t1.equals(t2));
    assert(t2.equals(t1));
    assert(!t1.equals(createType('long')));
    assert(!t1.equals(null));
  });

  test('isType', function () {
    var t = createType('int');
    assert(types.Type.isType(t));
    assert(types.Type.isType(t, 'int'));
    assert(!types.Type.isType());
    assert(!types.Type.isType('int'));
    assert(!types.Type.isType(function () {}));
  });

  test('reset', function () {
    types.Type.__reset(0);
    var t = createType('string');
    var buf = t.toBuffer('\x01');
    assert.deepEqual(buf, new Buffer([2, 1]));
  });

});

function testType(Type, data, invalidSchemas) {

  data.forEach(function (elem) {
    test('roundtrip', function () {
      var type = new Type(elem.schema);
      elem.valid.forEach(function (v) {
        assert(type.isValid(v), '' + v);
        var fn = elem.check || assert.deepEqual;
        fn(type.fromBuffer(type.toBuffer(v)), v);
        fn(type.fromString(type.toString(v), {coerceBuffers: true}), v);
      });
      elem.invalid.forEach(function (v) {
        assert(!type.isValid(v), '' + v);
        assert.throws(function () { type.isValid(v, {errorHook: hook}); });
        assert.throws(function () { type.toBuffer(v); });

        function hook() { throw new Error(); }
      });
      var n = 50;
      while (n--) {
        // Run a few times to make sure we cover any branches.
        assert(type.isValid(type.random()));
      }
    });
  });

  test('skip', function () {
    data.forEach(function (elem) {
      var fn = elem.check || assert.deepEqual;
      var items = elem.valid;
      if (items.length > 1) {
        var type = new Type(elem.schema);
        var buf = new Buffer(1024);
        var tap = new Tap(buf);
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
  return createType(reader).createResolver(createType(writer));
}

function floatEquals(a, b) {
  return Math.abs((a - b) / Math.min(a, b)) < 1e-7;
}

function invert(buf) {
  var len = buf.length;
  while (len--) {
    buf[len] = ~buf[len];
  }
}
