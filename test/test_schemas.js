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

    test('single file', function (done) {
      assemble(path.join(DPATH, 'Hello.avdl'), function (err, attrs) {
        assert.strictEqual(err, null);
        assert.deepEqual(attrs, {
          namespace: 'org.apache.avro.test',
          protocol: 'Simple',
          doc: 'An example protocol in Avro IDL.\n\nInspired by the Avro specification IDL page:\nhttps://avro.apache.org/docs/current/idl.html#example',
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
                {
                  type: {type: 'string', foo: 'first and last'},
                  order: 'ignore',
                  name: 'name'
                },
                {type: 'Kind', order: 'descending', name: 'kind'},
                {type: 'MD5', name: 'hash', doc: 'MD5 field!'},
                {
                  type: ['MD5', 'null'],
                  aliases: ['hash'],
                  name: 'nullableHash'
                },
                {
                  type: {
                    type: 'array',
                    items: {type: 'long', logicalType: 'date'}
                  },
                  name: 'arrayOfDates'
                },
                {
                  type: {type: 'map', values: 'boolean'},
                  name: 'someMap',
                  'default': {'true': true}
                }
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
              doc: 'greet',
              response: 'string',
              request: [{ type: 'string', name: 'greeting', 'default': 'hi'}]
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

    test('custom import hook', function (done) {
      var hook = createImportHook({'foo.avdl': 'protocol Foo {}'});
      assemble('foo.avdl', {importHook: hook}, function (err, attrs) {
        assert.strictEqual(err, null);
        assert.deepEqual(attrs, {protocol: 'Foo', messages: {}, types: []});
        done();
      });
    });

    test('duplicate message', function (done) {
      var hook = createImportHook({
        '1.avdl': 'protocol First { double one(); int one(); }'
      });
      assemble('1.avdl', {importHook: hook}, function (err) {
        assert(/duplicate message/.test(err.message));
        done();
      });
    });

    test('import idl', function (done) {
      var hook = createImportHook({
        '1.avdl': 'import idl "2.avdl"; protocol First {}',
        '2.avdl': 'protocol Second { int one(); }'
      });
      assemble('1.avdl', {importHook: hook}, function (err, attrs) {
        assert.strictEqual(err, null);
        assert.deepEqual(attrs, {
          protocol: 'First',
          messages: {one: {request: [], response: 'int'}},
          types: []
        });
        done();
      });
    });

    test('import idl inside protocol', function (done) {
      var hook = createImportHook({
        '1.avdl': 'protocol First {int two(); import idl "2.avdl";}',
        '2.avdl': 'protocol Second { fixed Foo(1); }'
      });
      assemble('1.avdl', {importHook: hook}, function (err, attrs) {
        assert.strictEqual(err, null);
        assert.deepEqual(attrs, {
          protocol: 'First',
          messages: {two: {request: [], response: 'int'}},
          types: [{name: 'Foo', type: 'fixed', size: 1, namespace: ''}]
        });
        done();
      });
    });

    test('duplicate message from import', function (done) {
      var hook = createImportHook({
        '1.avdl': 'import idl "2.avdl";\nprotocol First { double one(); }',
        '2.avdl': 'protocol Second { int one(); }'
      });
      assemble('1.avdl', {importHook: hook}, function (err) {
        assert(/duplicate message/.test(err.message));
        done();
      });
    });

    test('repeated import', function (done) {
      var hook = createImportHook({
        '1.avdl': 'import idl "2.avdl";import idl "3.avdl";protocol A {}',
        '2.avdl': 'import idl "3.avdl";protocol B { enum Number { ONE } }',
        '3.avdl': 'protocol C { enum Letter { A } }'
      });
      assemble('1.avdl', {importHook: hook}, function (err, attrs) {
        assert.deepEqual(attrs, {
          protocol: 'A',
          messages: {},
          types: [
            {name: 'Letter', type: 'enum', symbols: ['A'], namespace: ''},
            {name: 'Number', type: 'enum', symbols: ['ONE'], namespace: ''}
          ]
        });
        done();
      });
    });

    test('import protocol', function (done) {
      var hook = createImportHook({
        '1': 'import protocol "2";import protocol "3.avpr"; protocol A {}',
        '2': JSON.stringify({
          protocol: 'B',
          types: [{name: 'Letter', type: 'enum', symbols: ['A']}],
          messages: {ping: {request: [], response: 'boolean'}}
        }),
        '3.avpr': '{"protocol": "C"}'
      });
      assemble('1', {importHook: hook}, function (err, attrs) {
        assert.strictEqual(err, null);
        assert.deepEqual(attrs, {
          protocol: 'A',
          messages: {ping: {request: [], response: 'boolean'}},
          types: [
            {name: 'Letter', type: 'enum', symbols: ['A'], namespace: ''}
          ]
        });
        done();
      });
    });

    test('import protocol with namespace', function (done) {
      var hook = createImportHook({
        'A': 'import protocol "B";import protocol "C";protocol A {}',
        'B': JSON.stringify({
          protocol: 'B',
          namespace: 'b',
          types: [{name: 'Letter', type: 'enum', symbols: ['A']}]
        }),
        'C': JSON.stringify({
          protocol: 'C',
          namespace: 'c',
          types: [{name: 'Letter', type: 'enum', symbols: ['A']}]
        })
      });
      assemble('A', {importHook: hook}, function (err, attrs) {
        assert.strictEqual(err, null);
        assert.deepEqual(attrs, {
          protocol: 'A',
          messages: {},
          types: [
            {namespace: 'b', name: 'Letter', type: 'enum', symbols: ['A']},
            {namespace: 'c', name: 'Letter', type: 'enum', symbols: ['A']}
          ]
        });
        done();
      });
    });

    test('import protocol with duplicate message', function (done) {
      var hook = createImportHook({
        'A': 'import protocol "B";import protocol "C";protocol A {}',
        'B': JSON.stringify({
          protocol: 'B',
          messages: {ping: {request: [], response: 'boolean'}}
        }),
        'C': JSON.stringify({
          protocol: 'C',
          messages: {ping: {request: [], response: 'boolean'}}
        })
      });
      assemble('A', {importHook: hook}, function (err) {
        assert(/duplicate message/.test(err.message));
        done();
      });
    });

    test('import schema', function (done) {
      var hook = createImportHook({
        '1': 'import schema "2"; protocol A {}',
        '2': JSON.stringify({name: 'Number', type: 'enum', symbols: ['1']})
      });
      assemble('1', {importHook: hook}, function (err, attrs) {
        assert.strictEqual(err, null);
        assert.deepEqual(attrs, {
          protocol: 'A',
          messages: {},
          types: [
            {name: 'Number', type: 'enum', symbols: ['1'], namespace: ''}
          ]
        });
        done();
      });
    });

    test('import hook error', function (done) {
      var hook = function (fpath, kind, cb) {
        if (path.basename(fpath) === 'A.avdl') {
          cb(null, 'import schema "hi"; protocol A {}');
        } else {
          cb(new Error('foo'));
        }
      };
      assemble('A.avdl', {importHook: hook}, function (err) {
        assert(/foo/.test(err.message));
        done();
      });
    });

    test('import invalid kind', function (done) {
      var hook = createImportHook({'A.avdl': 'import foo "2";protocol A {}'});
      assemble('A.avdl', {importHook: hook}, function (err) {
        assert(/invalid import/.test(err.message));
        done();
      });
    });

    test('import invalid JSON', function (done) {
      var hook = createImportHook({
        '1': 'import schema "2"; protocol A {}',
        '2': '{'
      });
      assemble('1', {importHook: hook}, function (err) {
        assert(err);
        assert.equal(err.path, '2');
        done();
      });
    });

    test('annotated union', function (done) {
      var hook = createImportHook({
        '1': 'protocol A { @doc("") union { null, int } }'
      });
      assemble('1', {importHook: hook}, function (err) {
        assert(/unions cannot be annotated/.test(err.message));
        done();
      });
    });

    test('commented import', function (done) {
      var hook = createImportHook({
        '1': '/* import idl "2"; */ // import idl "3"\nprotocol A {}',
        '2': 'foo', // Invalid IDL.
        '3': 'bar'  // Same.
      });
      assemble('1', {importHook: hook}, function (err) {
        assert.strictEqual(err, null);
        done();
      });
    });

    test('qualified name', function (done) {
      var hook = createImportHook({
        '1': 'protocol A { fixed one.One(1); }',
      });
      assemble('1', {importHook: hook}, function (err, attrs) {
        assert.strictEqual(err, null);
        assert.deepEqual(attrs, {
          protocol: 'A',
          types: [{name: 'one.One', type: 'fixed', size: 1}],
          messages: {}
        });
        done();
      });
    });

    test('inline fixed', function (done) {
      var hook = createImportHook({
        '1': 'protocol A { record Two { fixed One(1) one; } }',
      });
      assemble('1', {importHook: hook}, function (err, attrs) {
        assert.strictEqual(err, null);
        assert.deepEqual(attrs, {
          protocol: 'A',
          types: [{
            name: 'Two',
            type: 'record',
            fields: [
              {name: 'one', type: {name: 'One', type: 'fixed', size: 1}}
            ]
          }],
          messages: {}
        });
        done();
      });
    });

    test('one way void', function (done) {
      var hook = createImportHook({
        '1': 'protocol A { void ping(); @foo(true) void pong(); }',
      });
      var opts = {importHook: hook, oneWayVoid: true};
      assemble('1', opts, function (err, attrs) {
        assert.strictEqual(err, null);
        assert.deepEqual(attrs, {
          protocol: 'A',
          types: [],
          messages: {
            ping: {response: 'null', request: [], 'one-way': true},
            pong: {
              response: {foo: true, type: 'null'},
              request: [],
              'one-way': true
            }
          }
        });
        done();
      });
    });

    test('javadoc', function (done) {
      var hook = createImportHook({
        '1': 'protocol A { /**\n * 1 */ fixed One(1); /** 2 */ void pong(); }'
      });
      assemble('1', {importHook: hook}, function (err, attrs) {
        assert.strictEqual(err, null);
        assert.deepEqual(attrs, {
          protocol: 'A',
          types: [
            {name: 'One', type: 'fixed', size: 1, doc: '1'}
          ],
          messages: {
            pong: {
              doc: '2',
              response: 'null',
              request: [],
            }
          }
        });
        done();
      });
    });

    test('reset namespace', function (done) {
      var hook = createImportHook({
        '1': 'protocol A { import idl "2"; }',
        '2': '@namespace("b") protocol B { @namespace("") fixed One(1); }'
      });
      assemble('1', {importHook: hook}, function (err, attrs) {
        assert.strictEqual(err, null);
        assert.deepEqual(attrs, {
          protocol: 'A',
          types: [{name: 'One', type: 'fixed', size: 1, namespace: ''}],
          messages: {}
        });
        done();
      });
    });

    test('reset nested namespace', function (done) {
      var hook = createImportHook({
        '1': 'protocol A { import idl "2"; }',
        '2': 'import idl "3"; @namespace("b") protocol B {}',
        '3': 'protocol C { fixed Two(1); }'
      });
      assemble('1', {importHook: hook}, function (err, attrs) {
        assert.strictEqual(err, null);
        assert.deepEqual(attrs, {
          protocol: 'A',
          types: [{name: 'Two', type: 'fixed', size: 1, namespace: ''}],
          messages: {}
        });
        done();
      });
    });

    // Import hook from strings.
    function createImportHook(imports) {
      return function (fpath, kind, cb) {
        var fname = path.basename(fpath);
        var str = imports[fname];
        delete imports[fname];
        process.nextTick(function () { cb(null, str); });
      };
    }

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
      assert.throws(function () { t.prev(); });
      assert.equal(t.next().val, 'fee');
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
