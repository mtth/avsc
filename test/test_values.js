/* jshint node: true, mocha: true */

'use strict';

var types = require('../lib/types'),
    values = require('../lib/values'),
    assert = require('assert');


var createType = types.createType;

suite('values', function () {

  suite('combine', function () {

    var combine = values.combine;

    test('empty', function () {
      assert.throws(function () { combine([]); });
    });

    test('unwrapped', function () {
      var t = createType(['int'], {wrapUnions: true});
      assert.throws(function () { combine([t, t]); });
    });

    test('numbers', function () {
      var t1 = createType('int');
      var t2 = createType('long');
      var t3 = createType('float');
      var t4 = createType('double');
      assert.strictEqual(combine([t1, t2]), t2);
      assert.strictEqual(combine([t1, t2, t3, t4]), t4);
      assert.strictEqual(combine([t3, t2]), t3);
      assert.strictEqual(combine([t2]), t2);
    });

    test('string & int', function () {
      var t1 = createType('int');
      var t2 = createType('string');
      assertUnionsEqual(combine([t1, t2]), createType(['int', 'string']));
    });

    test('field default', function () {
      var t1 = createType({
        type: 'record',
        fields: [{name: 'foo', type: 'int', 'default': 2}]
      });
      var t2 = createType({
        type: 'record',
        fields: []
      });
      var t3 = combine([t1, t2], {noNullDefaults: true});
      assert.deepEqual(
        JSON.parse(t3.getSchema({exportAttrs: true})),
        {
          type: 'record',
          fields: [
            {name: 'foo', type: 'int', 'default': 2}
          ]
        }
      );
    });

    test('missing fields no null default', function () {
      var t1 = createType({
        type: 'record',
        fields: [{name: 'foo', type: 'int'}, {name: 'bar', type: 'string'}]
      });
      var t2 = createType({
        type: 'record',
        fields: [{name: 'bar', type: 'string'}]
      });
      var t3;
      t3 = combine([t1, t2]);
      assert.deepEqual(
        JSON.parse(t3.getSchema({exportAttrs: true})),
        {
          type: 'record',
          fields: [
            // The null branch should always be first here.
            {name: 'foo', type: ['null', 'int'], 'default': null},
            {name: 'bar', type: 'string'}
          ]
        }
      );
      t3 = combine([t1, t2], {noNullDefaults: true});
      assertUnionsEqual(t3.getValuesType(), createType(['int', 'string']));
    });

  });

  suite('infer', function () {

    var infer = values.infer;

    test('numbers', function () {
      assert.equal(infer(1).getTypeName(), 'int');
    });

    test('function', function () {
      assert.throws(function () { infer(function () {}); });
    });

    test('record', function () {
      var t = infer({b: true, n: null, s: '', f: new Buffer(0)});
      assert.deepEqual(
        JSON.parse(t.getSchema()),
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

  });

});

function assertUnionsEqual(t1, t2) {
  // The order of branches in combined unions is undefined, this function
  // allows a safe equality check.
  var b1 = {};
  var b2 = {};
  t1.getTypes().forEach(function (t) { b1[t.getName(true)] = t; });
  t2.getTypes().forEach(function (t) { b2[t.getName(true)] = t; });
  assert.deepEqual(b1, b2);
}
