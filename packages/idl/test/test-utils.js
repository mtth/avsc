/* jshint node: true, mocha: true */

'use strict';

var utils = require('../lib/utils'),
    assert = require('assert');

suite('utils', function () {
  test('copy own properties', function () {
    function Obj() { this.a = 1; this.b = 2; }
    Obj.prototype.c = 2;
    var obj1 = new Obj();

    var obj2 = {b: 3};
    utils.copyOwnProperties(obj1, obj2);
    assert.deepEqual(obj2, {a: 1, b: 3});

    var obj3 = {b: 3};
    utils.copyOwnProperties(obj1, obj3, true);
    assert.deepEqual(obj3, {a: 1, b: 2});
  });

  test('jsonEnd', function () {
    assert.equal(utils.jsonEnd(''), -1);
    assert.equal(utils.jsonEnd('{}a'), 2);
    assert.equal(utils.jsonEnd('{a'), -1);
    assert.equal(utils.jsonEnd('123'), 3);
    assert.equal(utils.jsonEnd('[1,2]'), 5);
    assert.equal(utils.jsonEnd('true'), 4);
    assert.equal(utils.jsonEnd('null'), 4);
    assert.equal(utils.jsonEnd('falseish'), 5);
    assert.equal(utils.jsonEnd('"false"'), 7);
  });
});
