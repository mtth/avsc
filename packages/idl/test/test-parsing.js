/* jshint node: true, mocha: true */

'use strict';

if (process.browser) {
  return;
}

var parsing = require('../lib/parsing'),
    assert = require('assert');

suite('parsing', function () {
  suite('readSchema', function () {
    var readSchema = parsing.readSchema;

    test('anonymous record', function () {
      assert.deepEqual(
        readSchema('/** A foo. */ record { int foo; }'),
        {
          doc: 'A foo.',
          type: 'record',
          fields: [{type: 'int', name: 'foo'}]
        }
      );
    });

    test('fixed', function () {
      assert.deepEqual(
        readSchema('@logicalType("address") @live(true) fixed Address(6)'),
        {
          type: 'fixed',
          size: 6,
          live: true,
          name: 'Address',
          logicalType: 'address'
        }
      );
    });

    test('no implicit collection tags', function () {
      assert.throws(
        function () {
          readSchema(
            'record { array int bars; }',
            {delimitedCollections: true}
          );
        },
        /</
      );
    });

    test('mismatched implicit collection tags', function () {
      assert.throws(
        function () { readSchema('array < int'); },
        />/
      );
    });

    test('implicit collection tags', function () {
      assert.deepEqual(
        readSchema('record { array int bars; }'),
        {
          type: 'record',
          fields: [{type: {type: 'array', items: 'int'}, name: 'bars'}]
        }
      );
    });

    test('mismatched implicit collection tags', function () {
      assert.throws(function () {
        readSchema('record { array < int bars; }');
      }, />/);
    });

    test('default type ref', function () {
      assert.deepEqual(
        readSchema('@precision(4) @scale(2) decimal'),
        {type: 'bytes', logicalType: 'decimal', precision: 4, scale: 2}
      );
    });

    test('custom type ref', function () {
      var typeRefs = {foo: {logicalType: 'foo', type: 'long'}};
      assert.deepEqual(
        readSchema('record { foo bar; }', {typeRefs: typeRefs}),
        {
          type: 'record',
          fields: [
            {
              name: 'bar',
              type: {type: 'long', logicalType: 'foo'}
            }
          ]
        }
      );
    });

    test('type ref overwrite attributes', function () {
      var typeRefs = {ip: {logicalType: 'ip', type: 'fixed', size: 4}};
      assert.deepEqual(
        readSchema('record { @size(16) ip ipV6; }', {typeRefs: typeRefs}),
        {
          type: 'record',
          fields: [
            {
              name: 'ipV6',
              type: {type: 'fixed', size: 16, logicalType: 'ip'}
            }
          ]
        }
      );
    });
  });

  suite('readProtocol', function () {
    var readProtocol = parsing.readProtocol;

    test('anonymous protocol with javadoced type', function () {
      assert.deepEqual(
        readProtocol('protocol { /** Foo. */ int; }'),
        {imports: [], protocol: {types: [{doc: 'Foo.', type: 'int'}]}}
      );
    });

    test('invalid message suffix', function () {
      assert.throws(function () {
        readProtocol('protocol { void foo() repeated; }');
      }, /suffix/);
    });
  });

  suite('Tokenizer', function () {
    var Tokenizer = parsing.Tokenizer;

    test('next', function () {
      assert.deepEqual(
        getTokens('hello; "you"'),
        [
          {id: 'name', pos: 0, val: 'hello'},
          {id: 'operator', pos: 5, val: ';'},
          {id: 'string', pos: 6, val: '"you"'}
        ]
      );
    });

    test('next silent', function () {
      var t = new Tokenizer('fee 1');
      assert.equal(t.next().val, 'fee');
      assert.strictEqual(t.next({val: '2', silent: true}), undefined);
      assert.equal(t.next().val, '1');
    });

    test('invalid comment', function () {
      assert.throws(function () { getToken('/** rew'); });
    });

    test('invalid string', function () {
      assert.throws(function () { getToken('"rewr\\"re'); }, /unterminated/);
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
      assert.throws(function () { getToken('{"rew": "3}"]', 'json'); });
    });

    test('name', function () {
      [
        {str: 'hi', val: 'hi'},
        {str: '`i3i`', val: 'i3i'}
      ].forEach(function (el) {
        assert.deepEqual(getToken(el.str).val, el.val);
      });
    });

    test('non-matching', function () {
      assert.throws(function () { getToken('\n1', 'name'); });
      assert.throws(function () { getToken('{', undefined, '}'); });
    });

    function getToken(str, id, val) {
      var tokenizer = new Tokenizer(str);
      return tokenizer.next({id: id, val: val});
    }

    function getTokens(str) {
      var tokenizer = new Tokenizer(str);
      var tokens = [];
      var token;
      while ((token = tokenizer.next()).id !== '(eof)') {
        tokens.push(token);
      }
      return tokens;
    }
  });
});
