/* jshint node: true, mocha: true */

'use strict';

if (process.browser) {
  return;
}

var index = require('../lib'),
    assert = require('assert'),
    path = require('path');

var DPATH = path.join(__dirname, 'data');

suite('index', function () {
  suite('assembleProtocol', function () {
    var assembleProtocol = index.assembleProtocol;

    test('missing file', function (done) {
      assembleProtocol('./dat/foo', function (err) {
        assert(err);
        done();
      });
    });

    test('single file', function (done) {
      var fpath = path.join(DPATH, 'Hello.avdl');
      assembleProtocol(fpath, function (err, schema) {
        assert.strictEqual(err, null);
        assert.deepEqual(schema, {
          namespace: 'org.apache.avro.test',
          protocol: 'Simple',
          doc: 'An example protocol in Avro IDL.\n\nInspired by the Avro specification IDL page:\nhttps://avro.apache.org/docs/current/idl.html#example',
          types: [
            {
              aliases: ['org.foo.KindOf'],
              doc: 'An enum.',
              type: 'enum',
              name: 'Kind',
              symbols: ['FOO', 'BAR', 'BAZ']
            },
            {type: 'fixed', doc: 'A fixed.', name: 'MD5', size: 16},
            {
              type: 'record',
              name: 'TestRecord',
              doc: 'A record.',
              fields: [
                {
                  type: {type: 'string', foo: 'first and last'},
                  order: 'ignore',
                  name: 'name'
                },
                {type: 'Kind', order: 'descending', name: 'kind'},
                {type: 'MD5', name: 'hash'},
                {
                  doc: 'A field.',
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
              doc: 'An error.',
              fields: [{type: 'string', name: 'message'}]
            },
            {type: 'error', name: 'EmptyError', fields: []}
          ],
          messages: {
            hello: {
              doc: 'Greeting.',
              response: 'string',
              request: [{ type: 'string', name: 'greeting', 'default': 'hi'}]
            },
            echo: {
              response: 'TestRecord',
              request: [{type: 'TestRecord', name: 'record'}]
            },
            add: {
              doc: 'Adding.',
              response: 'int',
              request: [
                {type: 'int', name: 'arg1'},
                {type: 'int', name: 'arg2'}
              ]
            },
            echoBytes: {
              doc: 'Echoing.',
              response: 'bytes',
              request: [{type: 'bytes', name: 'data'}]
            },
            error: {response: 'null', request: [], errors: ['TestError']},
            errors: {
              response: 'string',
              request: [],
              errors: ['TestError', 'EmptyError']
            },
            ping: {response: 'null', request: [], 'one-way': true},
            pong: {response: 'null', request: [], 'one-way': true}
          }
        });
        done();
      });
    });

    test('custom file', function (done) {
      var fpath = path.join(DPATH, 'Custom.avdl');
      assembleProtocol(fpath, function (err, schema) {
        assert.strictEqual(err, null);
        assert.deepEqual(schema, {
          doc: 'A protocol using advanced features.',
          namespace: 'org.apache.avro.test',
          messages: {
            ok: {
              response: {type: 'enum', symbols: ['SUCCESS', 'FAILURE']},
              request: []
            },
            hash: {
              response: 'int',
              request: [
                {
                  name: 'fixed',
                  type: {type: 'fixed', size: 2},
                  'default': 'aa'
                },
                {type: 'long', name: 'length'}
              ]
            },
            import: {
              response: 'null',
              request: [],
              'one-way': true
            }
          }
        });
        done();
      });
    });

    test('custom import hook', function (done) {
      var opts = {
        importHook: createImportHook({'foo.avdl': 'protocol Foo {}'})
      };
      assembleProtocol('foo.avdl', opts, function (err, schema) {
        assert.strictEqual(err, null);
        assert.deepEqual(schema, {protocol: 'Foo'});
        done();
      });
    });

    test('empty file', function (done) {
      var opts = {
        importHook: createImportHook({'foo.avdl': ''})
      };
      assembleProtocol('foo.avdl', opts, function (err) {
        assert(/eof/.test(err.message));
        done();
      });
    });

    test('duplicate message', function (done) {
      var hook = createImportHook({
        '1.avdl': 'protocol First { double one(); int one(); }'
      });
      assembleProtocol('1.avdl', {importHook: hook}, function (err) {
        assert(/duplicate message/.test(err.message));
        done();
      });
    });

    test('import idl', function (done) {
      var opts = {
        importHook: createImportHook({
          '1.avdl': 'import idl "2.avdl"; protocol First {}',
          '2.avdl': 'protocol Second { int one(); }'
        })
      };
      assembleProtocol('1.avdl', opts, function (err, schema) {
        assert.strictEqual(err, null);
        assert.deepEqual(schema, {
          protocol: 'First',
          messages: {one: {request: [], response: 'int'}}
        });
        done();
      });
    });

    test('import idl inside protocol', function (done) {
      var opts = {
        importHook: createImportHook({
          '1.avdl': 'protocol First {int two(); import idl "2.avdl";}',
          '2.avdl': 'protocol Second { fixed Foo(1); }'
        })
      };
      assembleProtocol('1.avdl', opts, function (err, schema) {
        assert.strictEqual(err, null);
        assert.deepEqual(schema, {
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
      assembleProtocol('1.avdl', {importHook: hook}, function (err) {
        assert(/duplicate message/.test(err.message));
        done();
      });
    });

    test('repeated import', function (done) {
      var opts = {
        importHook: createImportHook({
          '1.avdl': 'import idl "2.avdl";import idl "3.avdl";protocol A {}',
          '2.avdl': 'import idl "3.avdl";protocol B { enum Number { ONE } }',
          '3.avdl': 'protocol C { enum Letter { A } }'
        })
      };
      assembleProtocol('1.avdl', opts, function (err, schema) {
        assert.deepEqual(schema, {
          protocol: 'A',
          types: [
            {name: 'Letter', type: 'enum', symbols: ['A'], namespace: ''},
            {name: 'Number', type: 'enum', symbols: ['ONE'], namespace: ''}
          ]
        });
        done();
      });
    });

    test('import protocol', function (done) {
      var opts = {
        importHook: createImportHook({
          '1': 'import protocol "2";import protocol "3.avpr"; protocol A {}',
          '2': JSON.stringify({
            protocol: 'B',
            types: [{name: 'Letter', type: 'enum', symbols: ['A']}],
            messages: {ping: {request: [], response: 'boolean'}}
          }),
          '3.avpr': '{"protocol": "C"}'
        })
      };
      assembleProtocol('1', opts, function (err, schema) {
        assert.strictEqual(err, null);
        assert.deepEqual(schema, {
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
          protocol: 'bb.B',
          namespace: 'b', // Takes precedence.
          types: [{name: 'Letter', type: 'enum', symbols: ['A']}]
        }),
        'C': JSON.stringify({
          protocol: 'C',
          namespace: 'c',
          types: [{name: 'Letter', type: 'enum', symbols: ['A']}]
        })
      });
      assembleProtocol('A', {importHook: hook}, function (err, schema) {
        assert.strictEqual(err, null);
        assert.deepEqual(schema, {
          protocol: 'A',
          types: [
            {namespace: 'b', name: 'Letter', type: 'enum', symbols: ['A']},
            {namespace: 'c', name: 'Letter', type: 'enum', symbols: ['A']}
          ]
        });
        done();
      });
    });

    test('import protocol with namespaced name', function (done) {
      var hook = createImportHook({
        'A': 'import protocol "B";protocol A {}',
        'B': JSON.stringify({
          protocol: 'b.B',
          types: [{name: 'Letter', type: 'enum', symbols: ['A']}]
        })
      });
      assembleProtocol('A', {importHook: hook}, function (err, schema) {
        assert.strictEqual(err, null);
        assert.deepEqual(schema, {
          protocol: 'A',
          types: [
            {namespace: 'b', name: 'Letter', type: 'enum', symbols: ['A']}
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
      assembleProtocol('A', {importHook: hook}, function (err) {
        assert(/duplicate message/.test(err.message));
        done();
      });
    });

    test('import schema', function (done) {
      var hook = createImportHook({
        '1': 'import schema "2"; protocol A {}',
        '2': JSON.stringify({name: 'Number', type: 'enum', symbols: ['1']})
      });
      assembleProtocol('1', {importHook: hook}, function (err, schema) {
        assert.strictEqual(err, null);
        assert.deepEqual(schema, {
          protocol: 'A',
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
      assembleProtocol('A.avdl', {importHook: hook}, function (err) {
        assert(/foo/.test(err.message));
        done();
      });
    });

    test('import hook idl error', function (done) {
      var hook = function (fpath, kind, cb) {
        if (path.basename(fpath) === 'A.avdl') {
          cb(null, 'import idl "hi"; protocol A {}');
        } else {
          cb(new Error('bar'));
        }
      };
      assembleProtocol('A.avdl', {importHook: hook}, function (err) {
        assert(/bar/.test(err.message));
        done();
      });
    });

    test('import invalid kind', function (done) {
      var hook = createImportHook({'A.avdl': 'import foo "2";protocol A {}'});
      assembleProtocol('A.avdl', {importHook: hook}, function (err) {
        assert(/invalid import/.test(err.message));
        done();
      });
    });

    test('import invalid JSON', function (done) {
      var hook = createImportHook({
        '1': 'import schema "2"; protocol A {}',
        '2': '{'
      });
      assembleProtocol('1', {importHook: hook}, function (err) {
        assert(err);
        assert.equal(err.path, '2');
        done();
      });
    });

    test('annotated union', function (done) {
      var hook = createImportHook({
        '1': 'protocol A { /** 1 */ @bar(true) union { null, int } foo(); }'
      });
      assembleProtocol('1', {importHook: hook}, function (err) {
        assert(/union annotations/.test(err.message));
        done();
      });
    });

    test('commented import', function (done) {
      var hook = createImportHook({
        '1': '/* import idl "2"; */ // import idl "3"\nprotocol A {}',
        '2': 'foo', // Invalid IDL.
        '3': 'bar'  // Same.
      });
      assembleProtocol('1', {importHook: hook}, function (err) {
        assert.strictEqual(err, null);
        done();
      });
    });

    test('qualified name', function (done) {
      var hook = createImportHook({
        '1': 'protocol A { fixed one.One(1); }',
      });
      assembleProtocol('1', {importHook: hook}, function (err, schema) {
        assert.strictEqual(err, null);
        assert.deepEqual(schema, {
          protocol: 'A',
          types: [{name: 'one.One', type: 'fixed', size: 1}]
        });
        done();
      });
    });

    test('inline fixed', function (done) {
      var hook = createImportHook({
        '1': 'protocol A { record Two { fixed One(1) one; } }',
      });
      assembleProtocol('1', {importHook: hook}, function (err, schema) {
        assert.strictEqual(err, null);
        assert.deepEqual(schema, {
          protocol: 'A',
          types: [{
            name: 'Two',
            type: 'record',
            fields: [
              {name: 'one', type: {name: 'One', type: 'fixed', size: 1}}
            ]
          }]
        });
        done();
      });
    });

    test('one way void', function (done) {
      var hook = createImportHook({
        '1': 'protocol A { void ping(); @foo(true) void pong(); }',
      });
      var opts = {importHook: hook, oneWayVoid: true};
      assembleProtocol('1', opts, function (err, schema) {
        assert.strictEqual(err, null);
        assert.deepEqual(schema, {
          protocol: 'A',
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

    test('javadoc precedence', function (done) {
      var hook = createImportHook({
        '1': 'protocol A {/**1*/ @doc(2) fixed One(1);}',
      });
      var opts = {importHook: hook, reassignJavadoc: true};
      assembleProtocol('1', opts, function (err, schema) {
        assert.strictEqual(err, null);
        assert.deepEqual(schema, {
          protocol: 'A',
          types: [
            {name: 'One', type: 'fixed', size: 1, doc: 2}
          ]
        });
        done();
      });
    });

    test('reset namespace', function (done) {
      var hook = createImportHook({
        '1': 'protocol A { import idl "2"; }',
        '2': '@namespace("b") protocol B { @namespace("") fixed One(1); }'
      });
      assembleProtocol('1', {importHook: hook}, function (err, schema) {
        assert.strictEqual(err, null);
        assert.deepEqual(schema, {
          protocol: 'A',
          types: [{name: 'One', type: 'fixed', size: 1, namespace: ''}]
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
      assembleProtocol('1', {importHook: hook}, function (err, schema) {
        assert.strictEqual(err, null);
        assert.deepEqual(schema, {
          protocol: 'A',
          types: [{name: 'Two', type: 'fixed', size: 1, namespace: ''}]
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

  suite('assembleProtocolSync', function () {
    var assembleProtocolSync = index.assembleProtocolSync;

    test('custom file', function () {
      var fpath = path.join(DPATH, 'Custom.avdl');
      var schema = assembleProtocolSync(fpath);
      assert.deepEqual(schema, {
        doc: 'A protocol using advanced features.',
        namespace: 'org.apache.avro.test',
        messages: {
          ok: {
            response: {type: 'enum', symbols: ['SUCCESS', 'FAILURE']},
            request: []
          },
          hash: {
            response: 'int',
            request: [
              {
                name: 'fixed',
                type: {type: 'fixed', size: 2},
                'default': 'aa'
              },
              {type: 'long', name: 'length'}
            ]
          },
          import: {
            response: 'null',
            request: [],
            'one-way': true
          }
        }
      });
    });

    test('custom import hook', function () {
      var opts = {
        importHook: createImportHook({
          '1.avdl': 'import idl "2.avdl"; protocol First {}',
          '2.avdl': 'protocol Second { int one(); }'
        })
      };
      var schema = assembleProtocolSync('1.avdl', opts);
      assert.deepEqual(schema, {
        protocol: 'First',
        messages: {one: {request: [], response: 'int'}}
      });
    });

    // Import hook from strings.
    function createImportHook(imports) {
      return function (fpath) {
        var fname = path.basename(fpath);
        var str = imports[fname];
        delete imports[fname];
        return str;
      };
    }
  });
});
