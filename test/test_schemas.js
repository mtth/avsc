/* jshint node: true, mocha: true */

'use strict';

if (process.browser) {
  return;
}

var schemas = require('../lib/schemas'),
    assert = require('assert'),
    path = require('path');


var DPATH = path.join(__dirname, 'dat');


suite('schemas', function () {

  suite('assemble', function () {

    var assemble = schemas.assemble;

    test('missing file', function (done) {
      assemble('./dat/foo', function (err) {
        assert(err);
        done();
      });
    });

    test('simple file', function (done) {
      assemble(path.join(DPATH, 'hello.avdl'), function (err, attrs) {
        assert.strictEqual(err, null);
        assert.deepEqual(attrs, {
          namespace: 'org.apache.avro.test',
          protocol: 'Simple',
          types: [
            {
              aliases: ['org.foo.KindOf'],
              type: 'enum',
              name: 'Kind',
              symbols: ['FOO', 'BAR', 'BAZ']
            },
            {type: 'fixed', name: 'MD5', size: 16},
            {
              type: 'record',
              name: 'TestRecord',
              fields: [
                {type: 'string', order: 'ignore', name: 'name'},
                {type: 'Kind', order: 'descending', name: 'kind'},
                {type: 'MD5', name: 'hash'},
                {
                  type: ['MD5', 'null'],
                  aliases: ['hash'],
                  name: 'nullableHash'
                },
                {type: {type: 'array', items: 'long'}, name: 'arrayOfLongs'}
              ]
            },
            {
              type: 'error',
              name: 'TestError',
              fields: [{type: 'string', name: 'message'}]
            }
          ],
          messages: {
            hello: {
              response: 'string',
              request: [{ type: 'string', name: 'greeting' }]
            },
            echo: {
              response: 'TestRecord',
              request: [{type: 'TestRecord', name: 'record'}]
            },
            add: {
              response: 'int',
              request: [
                {type: 'int', name: 'arg1'},
                {type: 'int', name: 'arg2'}
              ]
            },
            echoBytes: {
              response: 'bytes',
              request: [{type: 'bytes', name: 'data'}]
            },
            error: {response: 'null', request: [], errors: ['TestError']},
            ping: {response: 'null', request: [], 'one-way': true}
          }
        });
        done();
      });
    });

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

    test('invalid comment', function () {
      assert.throws(function () { getToken('/** rew'); });
    });

    test('invalid string', function () {
      assert.throws(function () { getToken('"rewr\\"re'); });
    });

    test('valid JSON', function () {
      [
        {str: '324,', val: 324},
        {str: '3,', val: 3},
        {str: '-54,', val: -54},
        {str: '-5.4)', val: -5.4},
        {str: '"324",', val: '324'},
        {str: '"hello \\"you\\""r', val: 'hello "you"'},
        {str: '{}o', val: {}},
        {str: '{"a": 1},', val: {a: 1}},
        {str: '[]', val: []},
        {str: 'true+1', val: true},
        {str: 'null.1', val: null},
        {str: 'false::', val: false},
        {str: '["[", {"}": null}, true]', val: ['[', {'}': null}, true]},
      ].forEach(function (el) {
        assert.deepEqual(getToken(el.str, 'json').val, el.val);
      });
    });

    test('invalid JSON', function () {
      assert.throws(function () { getToken('{"rew": "3}"', 'json'); });
    });

    test('name', function () {
      [
        {str: 'hi', val: 'hi'},
        {str: '`i3i`', val: 'i3i'}
      ].forEach(function (el) {
        assert.deepEqual(getToken(el.str).val, el.val);
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
