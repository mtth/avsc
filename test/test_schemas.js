/* jshint node: true, mocha: true */

'use strict';

if (process.browser) {
  return;
}

var schemas = require('../lib/schemas'),
    assert = require('assert');


suite('schemas', function () {

  suite('assemble', function () {

    test('missing file', function () {
    });

  });

  suite('JSON boundary', function () {

    var endOfJson = schemas.endOfJson;

    test('number', function () {
      assert.equal(endOfJson('324,'), 3);
      assert.equal(endOfJson('3,'), 1);
      assert.equal(endOfJson('-3.123,'), 6);
    });

    test('string', function () {
      assert.equal(endOfJson('"324,",'), 6);
      assert.equal(endOfJson('"hello \\"you\\""r'), 15);
      assert.equal(endOfJson('"\\\\"a'), 4);
    });

    test('object', function () {
      assert.equal(endOfJson('{},'), 2);
      assert.equal(endOfJson('{"a":"}"},'), 9);
    });

    test('array', function () {
      assert.equal(endOfJson(' [],'), 3);
    });

  });

  suite('Tokenizer', function () {

    var Tokenizer = schemas.Tokenizer;

    test('simple', function () {
      assert.deepEqual(
        getTokens(new Tokenizer('hello; "you"')),
        [
          {id: 'literal', val: 'hello'},
          {id: 'operator', val: ';'},
          {id: 'string', val: '"you"'}
        ]
      );
    });

    function getTokens(tokenizer) {
      var tokens = [];
      var token;
      while ((token = tokenizer.advance())) {
        tokens.push(token);
      }
      return tokens;
    }

  });

});
