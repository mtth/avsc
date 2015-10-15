/* jshint node: true, mocha: true */

'use strict';


var avsc = require('../lib'),
    assert = require('assert'),
    path = require('path');


suite('index', function () {

  suite('parse', function () {

    test('object', function () {
      var obj = {
        type: 'record',
        name: 'Person',
        fields: [{name: 'so', type: 'Person'}]
      };
      assert(avsc.parse(obj) instanceof avsc.types.RecordType);
    });

    test('schema instance', function () {
      var type = avsc.parse({
        type: 'record',
        name: 'Person',
        fields: [{name: 'so', type: 'Person'}]
      });
      assert.strictEqual(avsc.parse(type), type);
    });

    test('stringified schema', function () {
      assert(avsc.parse('"int"') instanceof avsc.types.IntType);
    });

    test('type name', function () {
      assert(avsc.parse('double') instanceof avsc.types.DoubleType);
    });

    test('file', function () {
      var t1 = avsc.parse({type: 'fixed', name: 'id.Id', size: 64});
      var t2 = avsc.parse(path.join(__dirname, 'dat', 'Id.avsc'));
      assert.deepEqual(JSON.stringify(t1), JSON.stringify(t2));
    });

  });

});
