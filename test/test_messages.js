/* jshint node: true, mocha: true */

'use strict';

var messages = require('../lib/messages'),
    assert = require('assert'),
    stream = require('stream'),
    util = require('util');

var HANDSHAKE_REQUEST_TYPE = messages.HANDSHAKE_REQUEST_TYPE;
var HANDSHAKE_RESPONSE_TYPE = messages.HANDSHAKE_RESPONSE_TYPE;
var createProtocol = messages.createProtocol;


suite('messages', function () {

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
      assert.equal(p.getType('foo.Greeting').getName(true), 'record');
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
      assert.equal(Object.keys(messages).length, 1);
      assert(messages.ping !== undefined);
    });

    test('create listener', function (done) {
      var ptcl = createProtocol({protocol: 'Empty'});
      var transport = new stream.PassThrough();
      var ee = ptcl.createListener(transport, function (pending) {
        assert.equal(pending, 0);
        done();
      });
      ee.destroy();
    });

    test('subprotocol', function () {
      var ptcl = createProtocol({namespace: 'com.acme', protocol: 'Hello'});
      var subptcl = ptcl.subprotocol();
      assert.strictEqual(subptcl._emitterResolvers, ptcl._emitterResolvers);
      assert.strictEqual(subptcl._listenerResolvers, ptcl._listenerResolvers);
    });

    test('invalid emitter', function (done) {
      var ptcl = createProtocol({protocol: 'Empty'});
      ptcl.emit('hi', {}, null, function (err) {
        assert(/invalid emitter/.test(err.string));
        done();
      });
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

    var Message = messages.Message;

    test('empty errors', function () {
      var m = new Message('Hi', {
        request: [{name: 'greeting', type: 'string'}],
        response: 'int'
      });
      assert.deepEqual(m.errorType.toString(), '["string"]');
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

  });

  suite('MessageDecoder', function () {

    var MessageDecoder = messages.streams.MessageDecoder;

    test('ok', function (done) {
      var parts = [
        new Buffer([0, 1]),
        new Buffer([2]),
        new Buffer([]),
        new Buffer([3, 4, 5]),
        new Buffer([])
      ];
      var messages = [];
      var readable = createReadableStream(parts.map(frame), true);
      var writable = createWritableStream(messages, true)
        .on('finish', function () {
          assert.deepEqual(
            messages,
            [new Buffer([0, 1, 2]), new Buffer([3, 4, 5])]
          );
          done();
        });
      readable.pipe(new MessageDecoder()).pipe(writable);
    });

    test('trailing data', function (done) {
      var parts = [
        new Buffer([0, 1]),
        new Buffer([2]),
        new Buffer([]),
        new Buffer([3])
      ];
      var messages = [];
      var readable = createReadableStream(parts.map(frame), true);
      var writable = createWritableStream(messages, true);
      readable
        .pipe(new MessageDecoder())
        .on('error', function () {
          assert.deepEqual(messages, [new Buffer([0, 1, 2])]);
          done();
        })
        .pipe(writable);
    });

    test('empty', function (done) {
      var readable = createReadableStream([], true);
      readable
        .pipe(new MessageDecoder(true))
        .on('error', function () { done(); });
    });

  });

  suite('MessageEncoder', function () {

    var MessageEncoder = messages.streams.MessageEncoder;

    test('invalid frame size', function () {
      assert.throws(function () { new MessageEncoder(); });
    });

    test('ok', function (done) {
      var messages = [
        new Buffer([0, 1]),
        new Buffer([2])
      ];
      var frames = [];
      var readable = createReadableStream(messages, true);
      var writable = createWritableStream(frames, true);
      readable
        .pipe(new MessageEncoder(64))
        .pipe(writable)
        .on('finish', function () {
          assert.deepEqual(
            frames,
            [
              new Buffer([0, 0, 0, 2, 0, 1, 0, 0, 0, 0]),
              new Buffer([0, 0, 0, 1, 2, 0, 0, 0, 0])
            ]
          );
          done();
        });
    });

    test('all zeros', function (done) {
      var messages = [new Buffer([0, 0, 0, 0])];
      var frames = [];
      var readable = createReadableStream(messages, true);
      var writable = createWritableStream(frames, true);
      readable
        .pipe(new MessageEncoder(64))
        .pipe(writable)
        .on('finish', function () {
          assert.deepEqual(
            frames,
            [new Buffer([0, 0, 0, 4, 0, 0, 0, 0, 0, 0, 0, 0])]
          );
          done();
        });
    });

    test('short frame size', function (done) {
      var messages = [
        new Buffer([0, 1, 2]),
        new Buffer([2])
      ];
      var frames = [];
      var readable = createReadableStream(messages, true);
      var writable = createWritableStream(frames, true);
      readable
        .pipe(new MessageEncoder(2))
        .pipe(writable)
        .on('finish', function () {
          assert.deepEqual(
            frames,
            [
              new Buffer([0, 0, 0, 2, 0, 1, 0, 0, 0, 1, 2, 0, 0, 0, 0]),
              new Buffer([0, 0, 0, 1, 2, 0, 0, 0, 0])
            ]
          );
          done();
        });
    });

  });

  suite('StatefulEmitter', function () {

    // These tests manually generate expected requests and responses (as
    // opposed to using a matching listener for other tests below).

    test('ok handshake', function (done) {
      var buf = HANDSHAKE_RESPONSE_TYPE.toBuffer({match: 'BOTH'});
      var bufs = [];
      var ptcl = createProtocol({protocol: 'Empty'});
      var handshake = false;
      ptcl.createEmitter(createTransport([buf], bufs))
        .on('handshake', function (req, res) {
            handshake = true;
            assert(res.match === 'BOTH');
            assert.deepEqual(
              Buffer.concat(bufs),
              HANDSHAKE_REQUEST_TYPE.toBuffer({
                emitterHash: new Buffer(ptcl._hashString, 'binary'),
                serverHash: new Buffer(ptcl._hashString, 'binary')
              })
            );
            this.destroy();
        })
        .on('eot', function () {
          assert(handshake);
          done();
        });
    });

    test('no server match handshake', function (done) {
      var ptcl = createProtocol({protocol: 'Empty'});
      var resBufs = [
        {
          match: 'NONE',
          serverHash: {'org.apache.avro.ipc.MD5': new Buffer(16)},
          serverProtocol: {string: ptcl.toString()},
        },
        {match: 'BOTH'}
      ].map(function (val) { return HANDSHAKE_RESPONSE_TYPE.toBuffer(val); });
      var reqBufs = [];
      var handshakes = 0;
      ptcl.createEmitter(createTransport(resBufs, reqBufs))
        .on('handshake', function (req, res) {
          if (handshakes++) {
            assert(res.match === 'BOTH');
            this.destroy();
          } else {
            assert(res.match === 'NONE');
          }
        })
        .on('eot', function () {
          assert.equal(handshakes, 2);
          done();
        });
    });

    test('incompatible protocol', function (done) {
      var ptcl = createProtocol({protocol: 'Empty'});
      var hash = new Buffer(16); // Pretend the hash was different.
      var resBufs = [
        {
          match: 'NONE',
          serverHash: {'org.apache.avro.ipc.MD5': hash},
          serverProtocol: {string: ptcl.toString()},
        },
        {
          match: 'NONE',
          serverHash: {'org.apache.avro.ipc.MD5': hash},
          serverProtocol: {string: ptcl.toString()},
          meta: {map: {error: new Buffer('abcd')}}
        }
      ].map(function (val) { return HANDSHAKE_RESPONSE_TYPE.toBuffer(val); });
      var error = false;
      ptcl.createEmitter(createTransport(resBufs, []))
        .on('error', function (err) {
          error = true;
          assert.equal(err.message, 'abcd');
        })
        .on('eot', function () {
          assert(error);
          done();
        });
    });

    test('handshake error', function (done) {
      var resBufs = [
        new Buffer([4, 0, 0]), // Invalid handshakes.
        new Buffer([4, 0, 0])
      ];
      var ptcl = createProtocol({protocol: 'Empty'});
      var error = false;
      ptcl.createEmitter(createTransport(resBufs, []))
        .on('error', function (err) {
          error = true;
          assert.equal(err.message, 'handshake error');
        })
        .on('eot', function () {
          assert(error);
          done();
        });
    });

    test('orphan response', function (done) {
      var resBufs = [
        new Buffer([0, 0, 0]), // OK handshake.
        new Buffer([1, 2, 3])
      ];
      var ptcl = createProtocol({protocol: 'Empty'});
      var error = false;
      ptcl.createEmitter(createTransport(resBufs, []))
        .on('error', function (err) {
          error = true;
          assert.equal(err.message, 'orphan response');
        })
        .on('eot', function () {
          assert(error);
          done();
        });
    });

    test('ended readable', function (done) {
      var bufs = [];
      var ptcl = createProtocol({protocol: 'Empty'});
      ptcl.createEmitter(createTransport([], bufs))
        .on('eot', function () {
          assert.equal(bufs.length, 1); // A single handshake was sent.
          done();
        });
    });

    test('interrupted', function (done) {
      var ptcl = createProtocol({
        protocol: 'Empty',
        messages: {
          id: {request: [{name: 'id', type: 'int'}], response: 'int'}
        }
      });
      var resBufs = [
        new Buffer([0, 0, 0]), // OK handshake.
      ];
      var interrupted = 0;
      var transport = createTransport(resBufs, []);
      var ee = ptcl.createEmitter(transport, function () {
        assert.equal(interrupted, 2);
        done();
      });

      ptcl.emit('id', {id: 123}, ee, cb);
      ptcl.emit('id', {id: 123}, ee, cb);

      function cb(err) {
        assert.deepEqual(err, {string: 'interrupted'});
        interrupted++;
      }
    });

    test('missing message', function (done) {
      var ptcl1 = createProtocol({
        protocol: 'Ping',
        messages: {
          ping: {request: [], response: 'string'}
        }
      });
      var ptcl2 = createProtocol({protocol: 'Empty'});
      var transports = createPassthroughTransports();
      ptcl2.createListener(transports[1]);
      ptcl1.createEmitter(transports[0])
        .on('error', function (err) {
          assert(/missing server message: ping/.test(err.message));
          done();
        });
    });

  });

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
                  .on('finish', function () { emitterCb(resPt); });
                listenerCb(resPt);
                return reqPt;
              });
            });
          return reqPt;
        }
      });

    });

    function run(setupFn) {

      test('single', function (done) {
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
          ee.on('eot', done);
          ptcl.on('negate', function (req, ee, cb) { cb(null, -req.n); });
          ptcl.emit('negate', {n: 20}, ee, function (err, res) {
            assert.strictEqual(err, null);
            assert.equal(res, -20);
            this.emit('negate', {n: 'hi'}, ee, function (err) {
              assert(/invalid "int"/.test(err.string));
              ee.destroy();
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
            cb(new Error('delay must be non-negative'));
            return;
          }
          setTimeout(function () { cb(null, req.id); }, delay);
        });
        var ids = [];
        setupFn(ptcl, ptcl, function (ee) {
          ee.on('eot', function (pending) {
            assert.equal(pending, 0);
            assert.deepEqual(ids, [null, 'b', 'a']);
            done();
          });
          ptcl.emit('wait', {ms: 100, id: 'a'}, ee, function (err, res) {
            assert.strictEqual(err, null);
            ids.push(res);
          });
          ptcl.emit('wait', {ms: 10, id: 'b'}, ee, function (err, res) {
            assert.strictEqual(err, null);
            ids.push(res);
            ee.destroy();
          });
          ptcl.emit('wait', {ms: -100, id: 'c'}, ee, function (err, res) {
            assert(/non-negative/.test(err.string));
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

      test('incompatible protocols', function (done) {
        var emitterPtcl = createProtocol({
          protocol: 'emitterProtocol',
          messages: {
            age: {request: [{name: 'name', type: 'string'}], response: 'long'}
          }
        });
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
              assert(err);
              done();
            });
          }
        );
      });

      test('unknown message', function (done) {
        var ptcl = createProtocol({protocol: 'Empty'});
        setupFn(ptcl, ptcl, function (ee) {
          ptcl.emit('echo', {}, ee, function (err) {
            assert(/unknown/.test(err.string));
            done();
          });
        });
      });

      test('unsupported message', function (done) {
        var ptcl = createProtocol({
          protocol: 'Echo',
          messages: {
            echo: {
              request: [{name: 'id', type: 'string'}],
              response: 'string'
            }
          }
        });
        setupFn(ptcl, ptcl, function (ee) {
          ptcl.emit('echo', {id: ''}, ee, function (err) {
            assert(/unsupported/.test(err.string));
            done();
          });
        });
      });

      test('destroy noWait', function (done) {
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
        var eoted = false;
        setupFn(ptcl, ptcl, function (ee) {
          ee.on('eot', function (pending) {
            eoted = true;
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
            assert(/interrupted/.test(err.string));
            interrupted++;
          }
        });
      });

      test('destroy', function (done) {
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
              assert(/destroyed/.test(err.string));
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
    .pipe(new messages.streams.MessageEncoder(frameSize || 64));
}

function createWritableTransport(bufs) {
  var decoder = new messages.streams.MessageDecoder();
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
  function Stream() { stream.Readable.call(this); }
  util.inherits(Stream, stream.Readable);
  Stream.prototype._read = function () {
    this.push(bufs[n++] || null);
  };
  var readable = new Stream();
  return readable;
}

function createWritableStream(bufs) {
  function Stream() { stream.Writable.call(this); }
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
