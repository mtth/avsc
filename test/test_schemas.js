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

  });

  suite('Tokenizer', function () {

    var Tokenizer = schemas.Tokenizer;

    test('next', function () {
      assert.deepEqual(
        getTokens('hello; "you"'),
        [
          {id: 'name', val: 'hello'},
          {id: 'operator', val: ';'},
          {id: 'string', val: '"you"'}
        ]
      );
    });

    test('prev', function () {
      var t = new Tokenizer('fee 1');
      assert.equal(t.next().val, 'fee');
      assert.equal(t.next().val, '1');
      t.prev();
      assert.equal(t.next().val, '1');
      t.prev();
      t.prev();
      assert.equal(t.next().val, 'fee');
      assert.equal(t.next().val, '1');
    });

    test('JSON', function () {
      [
        {str: '324,', val: '324'},
        {str: '3,', val: '3'},
        {str: '-54,', val: '-54'},
        {str: '-5.4)', val: '-5.4'},
        {str: '"324",', val: '"324"'},
        {str: '"hello \\"you\\""r', val: '"hello \\"you\\""'},
        {str: '{}o', val: '{}'},
        {str: '{"a": 1},', val: '{"a": 1}'},
        {str: '[]', val: '[]'},
      ].forEach(function (el) {
        assert.equal(getToken(el.str, 'json').val, el.val);
      });

    });

    function getToken(str, id) {
      var tokenizer = new Tokenizer(str);
      return tokenizer.next({id: id});
    }

    function getTokens(str) {
      var tokenizer = new Tokenizer(str);
      var tokens = [];
      var token;
      try {
        while ((token = tokenizer.next())) {
          tokens.push(token);
        }
      } catch (err) {
        assert(/end of input/.test(err.message));
        return tokens;
      }
    }

  });

});
