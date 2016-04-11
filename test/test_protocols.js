/* jshint node: true, mocha: true */

'use strict';

var types = require('../lib/types'),
    protocols = require('../lib/protocols'),
    utils = require('../lib/utils'),
    assert = require('assert'),
    stream = require('stream'),
    util = require('util');


var HANDSHAKE_REQUEST_TYPE = protocols.HANDSHAKE_REQUEST_TYPE;
var HANDSHAKE_RESPONSE_TYPE = protocols.HANDSHAKE_RESPONSE_TYPE;
var createProtocol = protocols.createProtocol;


suite('protocols', function () {

  suite('Protocol', function () {

    test('get name and types', function () {
      var p = createProtocol({
        namespace: 'foo',
        protocol: 'HelloWorld',
        types: [
          {
            name: 'Greeting',
            type: 'record',
            fields: [{name: 'message', type: 'string'}]
          },
          {
            name: 'Curse',
            type: 'error',
            fields: [{name: 'message', type: 'string'}]
          }
        ],
        messages: {
          hello: {
            request: [{name: 'greeting', type: 'Greeting'}],
            response: 'Greeting',
            errors: ['Curse']
          },
          hi: {
          request: [{name: 'hey', type: 'string'}],
          response: 'null',
          'one-way': true
          }
        }
      });
      assert.equal(p.getName(), 'foo.HelloWorld');
      assert.equal(p.getType('foo.Greeting').getTypeName(), 'record');
    });

    test('missing message', function () {
      var ptcl = createProtocol({namespace: 'com.acme', protocol: 'Hello'});
      assert.throws(function () {
        ptcl.on('add', function () {});
      }, /unknown/);
    });

    test('missing name', function () {
      assert.throws(function () {
        createProtocol({namespace: 'com.acme', messages: {}});
      });
    });

    test('missing type', function () {
      assert.throws(function () {
        createProtocol({
          namespace: 'com.acme',
          protocol: 'HelloWorld',
          messages: {
            hello: {
              request: [{name: 'greeting', type: 'Greeting'}],
              response: 'Greeting'
            }
          }
        });
      });
    });

    test('special character in name', function () {
      var ptcl = createProtocol({
        protocol: 'Ping',
        messages: {
          'ping/1': {
            request: [],
            response: 'string'
          }
        }
      });
      var message = ptcl.getMessage('ping/1');
      assert.equal(message.getResponseType().getName(true), 'string');
    });

    test('get messages', function () {
      var ptcl;
      ptcl = createProtocol({protocol: 'Empty'});
      assert.deepEqual(ptcl.getMessages(), {});
      ptcl = createProtocol({
        protocol: 'Ping',
        messages: {
          ping: {
            request: [],
            response: 'string'
          }
        }
      });
      var messages = ptcl.getMessages();
      assert.deepEqual(messages, [ptcl.getMessage('ping')]);
    });

    test('subprotocol', function () {
      var ptcl = createProtocol({
        namespace: 'com.acme',
        protocol: 'Hello',
        types: [{name: 'Id', type: 'fixed', size: 2}],
        messages: {ping: {request: [], response: 'null'}}
      });
      var subptcl = ptcl.subprotocol();
      assert(subptcl.getFingerprint().equals(ptcl.getFingerprint()));
      assert.strictEqual(subptcl._emitterResolvers, ptcl._emitterResolvers);
      assert.strictEqual(subptcl._listenerResolvers, ptcl._listenerResolvers);
    });

    test('invalid emitter', function (done) {
      var ptcl = createProtocol({protocol: 'Hey'});
      var ee = createProtocol({protocol: 'Hi'}).createEmitter(function () {});
      assert.throws(
        function () { ptcl.emit('hi', {}, ee); },
        /invalid emitter/
      );
      done();
    });

    test('getFingerprint', function () {
      var p = createProtocol({
        namespace: 'hello',
        protocol: 'World',
      });
      assert.deepEqual(p.getFingerprint('md5'), p.getFingerprint());
    });

    test('toString', function () {
      var p = createProtocol({
        namespace: 'hello',
        protocol: 'World',
      });
      assert.equal(p.toString(), '{"protocol":"hello.World"}');
    });

    test('inspect', function () {
      var p = createProtocol({
        namespace: 'hello',
        protocol: 'World',
      });
      assert.equal(p.inspect(), '<Protocol "hello.World">');
    });

  });

  suite('Message', function () {

    var Message = protocols.Message;

    test('empty errors', function () {
      var m = new Message('Hi', {
        request: [{name: 'greeting', type: 'string'}],
        response: 'int'
      });
      assert.deepEqual(m.getErrorType().toString(), '["string"]');
    });

    test('missing response', function () {
      assert.throws(function () {
        new Message('Hi', {
          request: [{name: 'greeting', type: 'string'}]
        });
      });
    });

    test('invalid one-way', function () {
      // Non-null response.
      assert.throws(function () {
        new Message('Hi', {
          request: [{name: 'greeting', type: 'string'}],
          response: 'string',
          'one-way': true
        });
      });
      // Non-empty errors.
      assert.throws(function () {
        new Message('Hi', {
          request: [{name: 'greeting', type: 'string'}],
          response: 'null',
          errors: ['int'],
          'one-way': true
        });
      });
    });

    test('getters', function () {
      var m = new Message('Ping', {
        request: [{name: 'ping', type: 'string'}],
        response: 'null'
      });
      assert.equal(m.getName(), 'Ping');
      assert.equal(m.getRequestType().getFields()[0].getName(), 'ping');
      assert.equal(m.getResponseType().getName(true), 'null');
      assert.strictEqual(m.isOneWay(), false);
    });

    test('inspect', function () {
      var m = new Message('Ping', {
        request: [{name: 'ping', type: 'string'}],
        response: 'null',
        'one-way': true
      });
      assert(m.inspect()['one-way']);
    });

  });

  suite('FrameDecoder & FrameEncoder', function () {

    var FrameDecoder = protocols.streams.FrameDecoder;
    var FrameEncoder = protocols.streams.FrameEncoder;

    test('decode', function (done) {
      var frames = [
        new Buffer([0, 1]),
        new Buffer([2]),
        new Buffer([]),
        new Buffer([3, 4]),
        new Buffer([])
      ].map(frame);
      var messages = [];
      createReadableStream(frames)
        .pipe(new FrameDecoder())
        .on('finish', function () {
          assert.deepEqual(
            messages,
            [
              {id: null, payload: [new Buffer([0, 1]), new Buffer([2])]},
              {id: null, payload: [new Buffer([3, 4])]}
            ]
          );
          done();
        })
        .pipe(createWritableStream(messages));
    });

    test('decode with trailing data', function (done) {
      var frames = [
        new Buffer([0, 1]),
        new Buffer([2]),
        new Buffer([]),
        new Buffer([3])
      ].map(frame);
      var messages = [];
      createReadableStream(frames)
        .pipe(new FrameDecoder())
        .on('error', function () {
          assert.deepEqual(
            messages,
            [{id: null, payload: [new Buffer([0, 1]), new Buffer([2])]}]
          );
          done();
        })
        .pipe(createWritableStream(messages));
    });

    test('decode empty', function (done) {
      createReadableStream([])
        .pipe(new FrameDecoder())
        .on('end', function () {
          done();
        })
        .pipe(createWritableStream([]));
    });

    test('encode empty', function (done) {
      var frames = [];
      createReadableStream([])
        .pipe(new FrameEncoder())
        .pipe(createWritableStream(frames))
        .on('finish', function () {
          assert.deepEqual(frames, []);
          done();
        });
    });

    test('encode', function (done) {
      var messages = [
        {id: 1, payload: [new Buffer([1, 3, 5]), new Buffer([6, 8])]},
        {id: 4, payload: [new Buffer([123, 23])]}
      ];
      var frames = [];
      createReadableStream(messages)
        .pipe(new FrameEncoder())
        .pipe(createWritableStream(frames))
        .on('finish', function () {
          assert.deepEqual(
            frames,
            [
              new Buffer([0, 0, 0, 3]),
              new Buffer([1, 3, 5]),
              new Buffer([0, 0, 0, 2]),
              new Buffer([6, 8]),
              new Buffer([0, 0, 0, 0]),
              new Buffer([0, 0, 0, 2]),
              new Buffer([123, 23]),
              new Buffer([0, 0, 0, 0])
            ]
          );
          done();
        });
    });

    test('roundtrip', function (done) {
      var type = types.createType({
        type: 'record',
        name: 'Record',
        fields: [
          {name: 'id', type: 'null'},
          {name: 'payload', type: {type: 'array', items: 'bytes'}}
        ]
      });
      var n = 100;
      var src = [];
      while (n--) {
        var record = type.random();
        record.payload = record.payload.filter(function (arr) {
          return arr.length;
        });
        src.push(record);
      }
      var dst = [];
      var encoder = new FrameEncoder();
      var decoder = new FrameDecoder();
      createReadableStream(src)
        .pipe(encoder)
        .pipe(decoder)
        .pipe(createWritableStream(dst))
        .on('finish', function () {
          assert.deepEqual(dst, src);
          done();
        });
    });

  });

  suite('NettyDecoder & NettyEncoder', function () {

    var NettyDecoder = protocols.streams.NettyDecoder;
    var NettyEncoder = protocols.streams.NettyEncoder;

    test('decode with trailing data', function (done) {
      var src = [
        new Buffer([0, 0, 0, 2, 0, 0, 0]),
        new Buffer([1, 0, 0, 0, 5, 1, 3, 4, 2, 5, 1])
      ];
      var dst = [];
      createReadableStream(src)
        .pipe(new NettyDecoder())
        .on('error', function () {
          assert.deepEqual(
            dst,
            [{id: 2, payload: [new Buffer([1, 3, 4, 2, 5])]}]
          );
          done();
        })
        .pipe(createWritableStream(dst));
    });

    test('roundtrip', function (done) {
      var type = types.createType({
        type: 'record',
        name: 'Record',
        fields: [
          {name: 'id', type: 'int'},
          {name: 'payload', type: {type: 'array', items: 'bytes'}}
        ]
      });
      var n = 200;
      var src = [];
      while (n--) {
        var record = type.random();
        record.payload = record.payload.filter(function (arr) {
          return arr.length;
        });
        src.push(record);
      }
      var dst = [];
      var encoder = new NettyEncoder();
      var decoder = new NettyDecoder();
      createReadableStream(src)
        .pipe(encoder)
        .pipe(decoder)
        .pipe(createWritableStream(dst))
        .on('finish', function () {
          assert.deepEqual(dst, src);
          done();
        });
    });

  });

  suite('Registry', function () {

    var Registry = protocols.Registry;

    test('get', function (done) {
      var ctx = {one: 1};
      var reg = new Registry(ctx);
      var id = reg.add(200, function (err, two) {
        assert.strictEqual(this, ctx);
        assert.strictEqual(err, null);
        assert.equal(two, 2);
        assert.strictEqual(reg.get(id), undefined);
        done();
      });
      setTimeout(function () { reg.get(id)(null, 2); }, 50);
    });

    test('timeout', function (done) {
      var ctx = {one: 1};
      var reg = new Registry(ctx);
      var id = reg.add(10, function (err) {
        assert.strictEqual(this, ctx);
        assert(/timeout/.test(err));
        assert.strictEqual(reg.get(id), undefined);
        done();
      });
    });

    test('no timeout', function (done) {
      var ctx = {one: 1};
      var reg = new Registry(ctx);
      var id = reg.add(-1, function (err, two) {
        assert.strictEqual(this, ctx);
        assert.strictEqual(err, null);
        assert.equal(two, 2);
        assert.strictEqual(reg.get(id), undefined);
        done();
      });
      reg.get(id)(null, 2);
    });

    test('clear', function (done) {
      var ctx = {one: 1};
      var reg = new Registry(ctx);
      var n = 0;
      reg.add(20, fn);
      reg.add(20, fn);
      reg.clear();

      function fn(err) {
        assert(/interrupted/.test(err));
        if (++n == 2) {
          done();
        }
      }
    });

  });

  suite('StatefulEmitter', function () {

    test('custom timeout', function (done) {
      var ptcl = createProtocol({
        protocol: 'Ping',
        messages: {ping: {request: [], response: 'boolean'}}
      });
      var transport = {
        readable: new stream.PassThrough({objectMode: true}),
        writable: new stream.PassThrough({objectMode: true})
      };
      var ee = ptcl.createEmitter(transport, {objectMode: true, timeout: 0})
        .on('eot', function () { done(); });
      assert.equal(ee.getTimeout(), 0);
      var env = {message: ptcl.getMessage('ping'), request: {}};
      ee.emitMessage(env, 10, function (err) {
        assert(/timeout/.test(err));
        ee.destroy();
      });
    });

    test('missing message & callback', function (done) {
      var ptcl = createProtocol({
        protocol: 'Ping',
        messages: {ping: {request: [], response: 'boolean'}}
      });
      var transport = new stream.PassThrough({objectMode: true});
      var ee = ptcl.createEmitter(transport, {objectMode: true, timeout: 0})
        .on('eot', function () { done(); });
      assert.throws(function () {
        ee.emitMessage({message: ptcl.getMessage('ping'), request: {}});
      }, /missing callback/);
      ee.emitMessage({request: {}}, undefined, function (err) {
        assert(/missing message/.test(err));
        ee.destroy();
      });
    });

    test('invalid response', function (done) {
      var ptcl = createProtocol({
        protocol: 'Ping',
        messages: {ping: {request: [], response: 'boolean'}}
      });
      var transport = {
        readable: new stream.PassThrough({objectMode: true}),
        writable: new stream.PassThrough({objectMode: true})
      };
      var ee = ptcl.createEmitter(transport, {objectMode: true, timeout: 0})
        .on('eot', function () { done(); });
      ee.emitMessage({request: {}}, undefined, function (err) {
        assert(/missing message/.test(err));
      });
      var env = {message: ptcl.getMessage('ping'), request: {}};
      ee.destroy();
      return;
      ee.emitMessage(env, undefined, function (err) {
        debugger;
        assert(/invalid response/.test(err));
      });
      transport.writable.once('data', function (obj) {
        transport.readable.write({id: obj.id, payload: []});
      });
    });

  });

  suite('StatelessEmitter', function () {

    test('factory error', function (done) {
      var ptcl = createProtocol({
        protocol: 'Ping',
        messages: {ping: {request: [], response: 'boolean'}}
      }, {strictErrors: true, wrapUnions: true});
      var ee = ptcl.createEmitter(function (cb) {
        return stream.PassThrough()
          .on('finish', function () { cb(new Error('foobar')); });
      });
      ptcl.emit('ping', {}, ee, function (err) {
        assert.deepEqual(err, {string: 'foobar'});
        done();
      });
    });

  });

  suite('StatelessListener', function () {

    test('factory error', function (done) {
      var ptcl = createProtocol({
        protocol: 'Ping',
        messages: {ping: {request: [], response: 'boolean'}}
      });
      ptcl.createListener(function (cb) {
        cb(new Error('bar'));
        return stream.PassThrough();
      }).on('eot', function () { done(); });
    });

  });

  // TODO: Emitter and listener tests.

  suite('emit', function () {

    suite('stateful', function () {

      run(function (emitterPtcl, listenerPtcl, cb) {
        var pt1 = new stream.PassThrough();
        var pt2 = new stream.PassThrough();
        cb(
          emitterPtcl.createEmitter({readable: pt1, writable: pt2}),
          listenerPtcl.createListener({readable: pt2, writable: pt1})
        );
      });

    });

    suite('stateless', function () {

      run(function (emitterPtcl, listenerPtcl, cb) {
        cb(emitterPtcl.createEmitter(writableFactory));

        function writableFactory(emitterCb) {
          var reqPt = new stream.PassThrough()
            .on('finish', function () {
              listenerPtcl.createListener(function (listenerCb) {
                var resPt = new stream.PassThrough()
                  .on('finish', function () { emitterCb(null, resPt); });
                listenerCb(null, resPt);
                return reqPt;
              });
            });
          return reqPt;
        }
      });

    });

    function run(setupFn) {

      test('primitive types', function (done) {
        var ptcl = createProtocol({
          protocol: 'Math',
          messages: {
            negate: {
              request: [{name: 'n', type: 'int'}],
              response: 'long'
            }
          }
        });
        setupFn(ptcl, ptcl, function (ee) {
          var n1, n2;
          ee.on('eot', function () {
            assert.equal(n1, 1);
            assert.equal(n2, 0);
            done();
          });
          ptcl.on('negate', function (req, ee, cb) { cb(null, -req.n); });
          n1 = ptcl.emit('negate', {n: 20}, ee, function (err, res) {
            assert.equal(this, ptcl);
            assert.strictEqual(err, null);
            assert.equal(res, -20);
            n2 = this.emit('negate', {n: 'hi'}, ee, function (err) {
              assert(/invalid "int"/.test(err));
              ee.destroy();
            });
          });
        });
      });

      test('emit receive error', function (done) {
        var ptcl = createProtocol({
          protocol: 'Math',
          messages: {
            negate: {
              request: [{name: 'n', type: 'int'}],
              response: 'long',
              errors: [{type: 'map', values: 'string'}]
            }
          }
        });
        setupFn(ptcl, ptcl, function (ee) {
          ee.on('eot', function () { done(); });
          ptcl.on('negate', function (req, ee, cb) { cb({rate: '23'}); });
          ptcl.emit('negate', {n: 20}, ee, function (err) {
            assert.equal(this, ptcl);
            assert.deepEqual(err, {rate: '23'});
            ee.destroy();
          });
        });
      });

      test('complex type', function (done) {
        var ptcl = createProtocol({
          protocol: 'Literature',
          messages: {
            generate: {
              request: [{name: 'n', type: 'int'}],
              response: {
                type: 'array',
                items: {
                  name: 'N',
                  type: 'enum',
                  symbols: ['A', 'B']
                }
              }
            }
          }
        });
        setupFn(ptcl, ptcl, function (ee) {
          var type = ptcl.getType('N');
          ee.on('eot', function () { done(); });
          ptcl.on('generate', function (req, ee, cb) {
            var letters = [];
            while (req.n--) { letters.push(type.random()); }
            cb(null, letters);
          });
          ptcl.emit('generate', {n: 20}, ee, function (err, res) {
            assert.equal(this, ptcl);
            assert.strictEqual(err, null);
            assert.equal(res.length, 20);
            ee.destroy();
          });
        });
      });

      test('invalid request', function (done) {
        var ptcl = createProtocol({
          protocol: 'Math',
          messages: {
            negate: {
              request: [{name: 'n', type: 'int'}],
              response: 'int'
            }
          }
        }).on('negate', function () { assert(false); });
        setupFn(ptcl, ptcl, function (ee) {
          ee.on('eot', function () { done(); });
          ptcl.emit('negate', {n: 'a'}, ee, function (err) {
            assert(/invalid "int"/.test(err.message), null);
            ee.destroy();
          });
        });
      });

      test('error response', function (done) {
        var ptcl = createProtocol({
          protocol: 'Math',
          messages: {
            sqrt: {
              request: [{name: 'n', type: 'float'}],
              response: 'float'
            }
          }
        }).on('sqrt', function (req, ee, cb) {
          var n = req.n;
          if (n < 0) {
            cb(new Error('must be non-negative'));
          } else {
            cb(null, Math.sqrt(n));
          }
        });
        setupFn(ptcl, ptcl, function (ee) {
          ptcl.emit('sqrt', {n: 100}, ee, function (err, res) {
            assert(Math.abs(res - 10) < 1e-5);
            ptcl.emit('sqrt', {n: - 10}, ee, function (err) {
              assert.equal(this, ptcl);
              assert(/must be non-negative/.test(err.message));
              done();
            });
          });
        });
      });

      test('wrapped error response', function (done) {
        var ptcl = createProtocol({
          protocol: 'Math',
          messages: {
            sqrt: {
              request: [{name: 'n', type: 'float'}],
              response: 'null',
              errors: ['float']
            }
          }
        }, {wrapUnions: true}).on('sqrt', function (req, ee, cb) {
          var n = req.n;
          if (n < 0) {
            cb(new Error('must be non-negative'));
          } else {
            cb({float: Math.sqrt(n)});
          }
        });
        setupFn(ptcl, ptcl, function (ee) {
          ptcl.emit('sqrt', {n: -10}, ee, function (err) {
            assert(/must be non-negative/.test(err.message));
            ptcl.emit('sqrt', {n: 100}, ee, function (err) {
              assert(Math.abs(err.float - 10) < 1e-5);
              done();
            });
          });
        });
      });

      test('invalid response', function (done) {
        var ptcl = createProtocol({
          protocol: 'Math',
          messages: {
            sqrt: {
              request: [{name: 'n', type: 'float'}],
              response: 'float'
            }
          }
        }).on('sqrt', function (req, ee, cb) {
          var n = req.n;
          if (n < 0) {
            cb(null, 'complex'); // Invalid response.
          } else {
            cb(null, Math.sqrt(n));
          }
        });
        setupFn(ptcl, ptcl, function (ee) {
          ptcl.emit('sqrt', {n: - 10}, ee, function (err) {
            // The server error message is propagated to the client.
            assert(/invalid "float"/.test(err.message));
            ptcl.emit('sqrt', {n: 100}, ee, function (err, res) {
              // And the server doesn't die (we can make a new request).
              assert(Math.abs(res - 10) < 1e-5);
              done();
            });
          });
        });
      });

      test('invalid strict error', function (done) {
        var ptcl = createProtocol({
          protocol: 'Math',
          messages: {
            sqrt: {
              request: [{name: 'n', type: 'float'}],
              response: 'float'
            }
          }
        }, {strictErrors: true}).on('sqrt', function (req, ee, cb) {
          var n = req.n;
          if (n === -1) {
            cb(new Error('no i')); // Invalid error (should be a string).
          } else if (n < 0) {
            cb({error: 'complex'}); // Also invalid error.
          } else {
            cb(undefined, Math.sqrt(n));
          }
        });
        setupFn(ptcl, ptcl, function (ee) {
          ptcl.emit('sqrt', {n: -1}, ee, function (err) {
            assert(/invalid \["string"\]/.test(err));
            ptcl.emit('sqrt', {n: -2}, ee, function (err) {
              assert(/invalid \["string"\]/.test(err));
              ptcl.emit('sqrt', {n: 100}, ee, function (err, res) {
                // The server still doesn't die (we can make a new request).
                assert.strictEqual(err, undefined);
                assert(Math.abs(res - 10) < 1e-5);
                done();
              });
            });
          });
        });
      });

      test('out of order', function (done) {
        var ptcl = createProtocol({
          protocol: 'Delay',
          messages: {
            wait: {
              request: [
                {name: 'ms', type: 'float'},
                {name: 'id', type: 'string'}
              ],
              response: 'string'
            }
          }
        }).on('wait', function (req, ee, cb) {
          var delay = req.ms;
          if (delay < 0) {
            cb('delay must be non-negative');
            return;
          }
          setTimeout(function () { cb(null, req.id); }, delay);
        });
        var ids = [];
        setupFn(ptcl, ptcl, function (ee) {
          ee.on('eot', function (pending) {
            assert.equal(pending, 0);
            assert.equal(n1, 1);
            assert.equal(n2, 2);
            assert.equal(n3, 3);
            assert.deepEqual(ids, [null, 'b', 'a']);
            done();
          });
          var n1, n2, n3;
          n1 = ptcl.emit('wait', {ms: 500, id: 'a'}, ee, function (err, res) {
            assert.strictEqual(err, null);
            ids.push(res);
          });
          n2 = ptcl.emit('wait', {ms: 10, id: 'b'}, ee, function (err, res) {
            assert.strictEqual(err, null);
            ids.push(res);
            ee.destroy();
          });
          n3 = ptcl.emit('wait', {ms: -100, id: 'c'}, ee, function (err, res) {
            assert(/non-negative/.test(err));
            ids.push(res);
          });
        });
      });

      test('compatible protocols', function (done) {
        var emitterPtcl = createProtocol({
          protocol: 'emitterProtocol',
          messages: {
            age: {
              request: [{name: 'name', type: 'string'}],
              response: 'long'
            }
          }
        });
        var listenerPtcl = createProtocol({
          protocol: 'serverProtocol',
          messages: {
            age: {
              request: [
                {name: 'name', type: 'string'},
                {name: 'address', type: ['null', 'string'], 'default': null}
              ],
              response: 'int'
            },
            id: {
              request: [{name: 'name', type: 'string'}],
              response: 'long'
            }
          }
        });
        setupFn(
          emitterPtcl,
          listenerPtcl,
          function (ee) {
            listenerPtcl.on('age', function (req, ee, cb) {
              assert.equal(req.name, 'Ann');
              cb(null, 23);
            });
            emitterPtcl.emit('age', {name: 'Ann'}, ee, function (err, res) {
              assert.strictEqual(err, null);
              assert.equal(res, 23);
              done();
            });
          }
        );
      });

      test('compatible protocol with a complex type', function (done) {
        var ptcl1 = createProtocol({
          protocol: 'Literature',
          messages: {
            generate: {
              request: [{name: 'n', type: 'int'}],
              response: {
                type: 'array',
                items: {
                  name: 'N',
                  aliases: ['N2'],
                  type: 'enum',
                  symbols: ['A', 'B', 'C', 'D']
                }
              }
            }
          }
        });
        var ptcl2 = createProtocol({
          protocol: 'Literature',
          messages: {
            generate: {
              request: [{name: 'n', type: 'int'}],
              response: {
                type: 'array',
                items: {
                  name: 'N2',
                  aliases: ['N'],
                  type: 'enum',
                  symbols: ['A', 'B']
                }
              }
            }
          }
        });
        setupFn(ptcl1, ptcl2, function (ee) {
          var type = ptcl2.getType('N2');
          ee.on('eot', function () { done(); });
          ptcl2.on('generate', function (req, ee, cb) {
            var letters = [];
            while (req.n--) { letters.push(type.random()); }
            cb(null, letters);
          });
          ptcl1.emit('generate', {n: 20}, ee, function (err, res) {
            assert.equal(this, ptcl1);
            assert.strictEqual(err, null);
            assert.equal(res.length, 20);
            ee.destroy();
          });
        });
      });

      test('cached compatible protocols', function (done) {
        var ptcl1 = createProtocol({
          protocol: 'emitterProtocol',
          messages: {
            age: {
              request: [{name: 'name', type: 'string'}],
              response: 'long'
            }
          }
        });
        var ptcl2 = createProtocol({
          protocol: 'serverProtocol',
          namespace: 'foo',
          messages: {
            age: {
              request: [
                {name: 'name', type: 'string'},
                {name: 'address', type: ['null', 'string'], 'default': null}
              ],
              response: 'int'
            },
            id: {
              request: [{name: 'name', type: 'string'}],
              response: 'long'
            }
          }
        }).on('age', function (req, ee, cb) { cb(null, 48); });
        setupFn(
          ptcl1,
          ptcl2,
          function (ee1) {
            ptcl1.emit('age', {name: 'Ann'}, ee1, function (err, res) {
              assert.equal(res, 48);
              setupFn(
                ptcl1,
                ptcl2,
                function (ee2) { // ee2 has the server's protocol.
                  ptcl1.emit('age', {name: 'Bob'}, ee2, function (err, res) {
                    assert.equal(res, 48);
                    done();
                  });
                }
              );
            });
          }
        );
      });

      test('incompatible protocols missing message', function (done) {
        var emitterPtcl = createProtocol({
          protocol: 'emitterProtocol',
          messages: {
            age: {request: [{name: 'name', type: 'string'}], response: 'long'}
          }
        }, {wrapUnions: true});
        var listenerPtcl = createProtocol({protocol: 'serverProtocol'});
        setupFn(
          emitterPtcl,
          listenerPtcl,
          function (ee) {
            ee.on('error', function () {}); // For stateful protocols.
            emitterPtcl.emit('age', {name: 'Ann'}, ee, function (err) {
              assert(err.message);
              done();
            });
          }
        );
      });

      test('incompatible protocols', function (done) {
        var emitterPtcl = createProtocol({
          protocol: 'emitterProtocol',
          messages: {
            age: {request: [{name: 'name', type: 'string'}], response: 'long'}
          }
        }, {wrapUnions: true});
        var listenerPtcl = createProtocol({
          protocol: 'serverProtocol',
          messages: {
            age: {request: [{name: 'name', type: 'int'}], response: 'long'}
          }
        }).on('age', function (req, ee, cb) { cb(null, 0); });
        setupFn(
          emitterPtcl,
          listenerPtcl,
          function (ee) {
            ee.on('error', function () {}); // For stateful protocols.
            emitterPtcl.emit('age', {name: 'Ann'}, ee, function (err) {
              assert(err.message);
              done();
            });
          }
        );
      });

      test('incompatible protocols one way message', function (done) {
        var ptcl1 = createProtocol({
          protocol: 'ptcl1',
          messages: {ping: {request: [], response: 'null', 'one-way': true}}
        });
        var ptcl2 = createProtocol({
          protocol: 'ptcl2',
          messages: {ping: {request: [], response: 'null'}}
        });
        setupFn(ptcl1, ptcl2, function (ee) {
            ee.on('error', function (err) {
              // This will be called twice for stateful emitters: once when
              // interrupted, then for the incompatible protocol error.
              assert(err);
              if (!/interrupted/.test(err)) {
                done();
              }
            });
            ptcl1.emit('ping', {}, ee);
          }
        );
      });

      test('one way message', function (done) {
        var ptcl = createProtocol({
          protocol: 'ptcl',
          messages: {ping: {request: [], response: 'null', 'one-way': true}}
        });
        setupFn(ptcl, ptcl, function (ee) {
          ptcl.on('ping', function (req, ee, cb) {
            assert.strictEqual(cb, undefined);
            done();
          });
          ptcl.emit('ping', {}, ee);
        });
      });

      test('ignored response', function (done) {
        var ptcl = createProtocol({
          protocol: 'ptcl',
          messages: {ping: {request: [], response: 'null'}} // Not one-way.
        });
        setupFn(ptcl, ptcl, function (ee) {
          ptcl.on('ping', function (req, ee, cb) {
            cb(null, null);
            done();
          });
          ptcl.emit('ping', {}, ee);
        });
      });

      test('unknown message', function (done) {
        var ptcl = createProtocol({protocol: 'Empty'});
        setupFn(ptcl, ptcl, function (ee) {
          assert.throws(
            function () { ptcl.emit('echo', {}, ee); },
            /unknown message/
          );
          done();
        });
      });

      test('unhandled message', function (done) {
        var ptcl = createProtocol({
          protocol: 'Echo',
          messages: {
            echo: {
              request: [{name: 'id', type: 'string'}],
              response: 'string'
            },
            ping: {request: [], response: 'null', 'one-way': true}
          }
        });
        setupFn(ptcl, ptcl, function (ee) {
          ptcl.emit('echo', {id: ''}, ee, function (err) {
            assert(/unhandled/.test(err));
            done();
          });
        });
      });

      test('destroy emitter noWait', function (done) {
        var ptcl = createProtocol({
          protocol: 'Delay',
          messages: {
            wait: {
              request: [{name: 'ms', type: 'int'}],
              response: 'string'
            }
          }
        }).on('wait', function (req, ee, cb) {
            setTimeout(function () { cb(null, 'ok'); }, req.ms);
          });
        var interrupted = 0;
        setupFn(ptcl, ptcl, function (ee) {
          ee.on('eot', function (pending) {
            assert.equal(interrupted, 2);
            assert.equal(pending, 2);
            done();
          });
          ptcl.emit('wait', {ms: 75}, ee, interruptedCb);
          ptcl.emit('wait', {ms: 50}, ee, interruptedCb);
          ptcl.emit('wait', {ms: 10}, ee, function (err, res) {
            assert.equal(res, 'ok');
            ee.destroy(true);
          });

          function interruptedCb(err) {
            assert(/interrupted/.test(err.message));
            interrupted++;
          }
        });
      });

      test('destroy emitter', function (done) {
        var ptcl = createProtocol({
          protocol: 'Math',
          messages: {
            negate: {
              request: [{name: 'n', type: 'int'}],
              response: 'int'
            }
          }
        });
        setupFn(ptcl, ptcl, function (ee) {
          ptcl.on('negate', function (req, ee, cb) { cb(null, -req.n); });
          ptcl.emit('negate', {n: 20}, ee, function (err, res) {
            assert.strictEqual(err, null);
            assert.equal(res, -20);
            ee.destroy();
            this.emit('negate', {n: 'hi'}, ee, function (err) {
              assert(/destroyed/.test(err.message));
              done();
            });
          });
        });
      });

      test('catch server error', function (done) {
        var ptcl = createProtocol({
          protocol: 'Math',
          messages: {
            error1: {request: [], response: 'null'},
            negate: {
              request: [{name: 'n', type: 'int'}],
              response: 'int'
            }
          }
        });
        setupFn(ptcl, ptcl, function (ee) {
          ptcl
            .on('error1', function () { throw new Error('foobar'); })
            .on('negate', function (req, ee, cb) { cb(null, -req.n); })
            .emit('error1', {}, ee, function (err) {
              assert(/foobar/.test(err));
              // But the server doesn't die.
              this.emit('negate', {n: 20}, ee, function (err, res) {
                assert.strictEqual(err, null);
                assert.equal(res, -20);
                done();
              });
            });
        });
      });

    }

  });

});

// Helpers.

// Message framing.
function frame(buf) {
  var framed = new Buffer(buf.length + 4);
  framed.writeInt32BE(buf.length);
  buf.copy(framed, 4);
  return framed;
}

function createReadableTransport(bufs, frameSize) {
  return createReadableStream(bufs)
    .pipe(new protocols.streams.FrameEncoder(frameSize || 64));
}

function createWritableTransport(bufs) {
  var decoder = new protocols.streams.FrameDecoder();
  decoder.pipe(createWritableStream(bufs));
  return decoder;
}

function createTransport(readBufs, writeBufs) {
  return toDuplex(
    createReadableTransport(readBufs),
    createWritableTransport(writeBufs)
  );
}

function createPassthroughTransports() {
  var pt1 = stream.PassThrough();
  var pt2 = stream.PassThrough();
  return [{readable: pt1, writable: pt2}, {readable: pt2, writable: pt1}];
}

// Simplified stream constructor API isn't available in earlier node versions.

function createReadableStream(bufs) {
  var n = 0;
  function Stream() { stream.Readable.call(this, {objectMode: true}); }
  util.inherits(Stream, stream.Readable);
  Stream.prototype._read = function () {
    this.push(bufs[n++] || null);
  };
  var readable = new Stream();
  return readable;
}

function createWritableStream(bufs) {
  function Stream() { stream.Writable.call(this, {objectMode: true}); }
  util.inherits(Stream, stream.Writable);
  Stream.prototype._write = function (buf, encoding, cb) {
    bufs.push(buf);
    cb();
  };
  return new Stream();
}

// Combine two (binary) streams into a single duplex one. This is very basic
// and doesn't handle a lot of cases (e.g. where `_read` doesn't return
// something).
function toDuplex(readable, writable) {
  function Stream() {
    stream.Duplex.call(this);
    this.on('finish', function () { writable.end(); });
  }
  util.inherits(Stream, stream.Duplex);
  Stream.prototype._read = function () {
    this.push(readable.read());
  };
  Stream.prototype._write = function (buf, encoding, cb) {
    writable.write(buf);
    cb();
  };
  return new Stream();
}
