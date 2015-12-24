/* jshint node: true, mocha: true */

'use strict';

var protocols = require('../lib/protocols'),
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
      assert.equal(p.getType('foo.Greeting').getName(true), 'record');
    });

    test('missing message', function () {
      var ptcl = createProtocol({namespace: 'com.acme', protocol: 'Hello'});
      assert.throws(function () {
        ptcl.onMessage('add', function () {});
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

    test('subprotocol', function () {
      var ptcl = createProtocol({namespace: 'com.acme', protocol: 'Hello'});
      var subptcl = ptcl.subprotocol();
      assert.strictEqual(subptcl._emitterResolvers, ptcl._emitterResolvers);
      assert.strictEqual(subptcl._listenerResolvers, ptcl._listenerResolvers);
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

    test('empty errors', function () {
      var m = new protocols.Message('Hi', {
        request: [{name: 'greeting', type: 'string'}],
        response: 'int'
      });
      assert.deepEqual(m.errorType.toString(), '["string"]');
    });

    test('missing response', function () {
      assert.throws(function () {
        new protocols.Message('Hi', {
          request: [{name: 'greeting', type: 'string'}]
        });
      });
    });

    test('invalid one-way', function () {
      // Non-null response.
      assert.throws(function () {
        new protocols.Message('Hi', {
          request: [{name: 'greeting', type: 'string'}],
          response: 'string',
          'one-way': true
        });
      });
      // Non-empty errors.
      assert.throws(function () {
        new protocols.Message('Hi', {
          request: [{name: 'greeting', type: 'string'}],
          response: 'null',
          errors: ['int'],
          'one-way': true
        });
      });
    });

  });

  suite('MessageDecoder', function () {

    var MessageDecoder = protocols.streams.MessageDecoder;

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

    var MessageEncoder = protocols.streams.MessageEncoder;

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

  suite('stateful', function () {

    suite('Emitter', function () {

      // These tests manually generate expected requests and responses (as
      // opposed to using a matching listener for other tests below).

      test('ok handshake', function (done) {
        var buf = HANDSHAKE_RESPONSE_TYPE.toBuffer({match: 'BOTH'});
        var bufs = [];
        var ptcl = createProtocol({protocol: 'Empty'});
        var emitter = ptcl.createEmitter(createTransport([buf], bufs));
        var handshake = false;
        emitter
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
        var emitter = ptcl.createEmitter(createTransport(resBufs, reqBufs));
        var handshakes = 0;
        emitter
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
        var emitter = ptcl.createEmitter(createTransport(resBufs, []));
        var error = false;
        emitter
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
        var emitter = ptcl.createEmitter(createTransport(resBufs, []));
        var error = false;
        emitter
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
        var emitter = ptcl.createEmitter(createTransport(resBufs, []));
        var error = false;
        emitter
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
        var emitter = ptcl.createEmitter(createTransport([], bufs));
        emitter
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
        var emitter = ptcl.createEmitter(transport, function () {
          assert.equal(interrupted, 2);
          done();
        });

        emitter.emitMessage('id', {id: 123}, cb);
        emitter.emitMessage('id', {id: 123}, cb);

        function cb(err) {
          assert.deepEqual(err, {string: 'interrupted'});
          interrupted++;
        }
      });

    });

    test('same protocol', function (done) {
      var ptcl = createProtocol({
        protocol: 'Math',
        messages: {
          negate: {
            request: [{name: 'n', type: 'int'}],
            response: 'int'
          }
        }
      });
      statefulSetup(ptcl, ptcl, function (emitter) {
        ptcl.onMessage('negate', function (req, cb) { cb(null, -req.n); });
        emitter
          .on('eot', done)
          .emitMessage('negate', {n: 20}, function (err, res) {
            assert.strictEqual(err, null);
            assert.equal(res, -20);
            emitter.emitMessage('negate', {n: 'hi'}, function (err) {
              assert(/invalid "int"/.test(err.string));
              emitter.destroy();
            });
          });
      });
    });

    test('async', function (done) {
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
      }).onMessage('wait', function (req, cb) {
        var delay = req.ms;
        if (delay < 0) {
          cb(new Error('delay must be non-negative'));
          return;
        }
        setTimeout(function () { cb(null, req.id); }, delay);
      });
      var ids = [];
      statefulSetup(ptcl, ptcl, function (emt) {
        emt.on('eot', function (pending) {
          assert.equal(pending, 0);
          assert.deepEqual(ids, [null, 'b', 'a']);
          done();
        });
        emt.emitMessage('wait', {ms: 100, id: 'a'}, function (err, res) {
          assert.strictEqual(err, null);
          ids.push(res);
        });
        emt.emitMessage('wait', {ms: 10, id: 'b'}, function (err, res) {
          assert.strictEqual(err, null);
          ids.push(res);
          emt.destroy();
        });
        emt.emitMessage('wait', {ms: -100, id: 'c'}, function (err, res) {
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
      statefulSetup(
        emitterPtcl,
        listenerPtcl,
        function (emitter) {
          listenerPtcl.onMessage('age', function (params, cb) {
            assert.equal(params.name, 'Ann');
            cb(null, 23);
          });
          emitter
            .emitMessage('age', {name: 'Ann'}, function (err, res) {
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
      });
      statefulSetup(
        emitterPtcl,
        listenerPtcl,
        function (emitter) {
          listenerPtcl.onMessage('age', function (params, cb) {
            assert.equal(params.name, 'Ann');
            cb(null, 23);
          });
          emitter
            .on('error', function (err) {
              assert(err !== null);
              done();
            })
            .emitMessage('age', {name: 'Ann'}, function () {
              assert(false);
            });
        }
      );
    });

    function statefulSetup(emitterPtcl, listenerPtcl, cb) {
      var pt1 = new stream.PassThrough();
      var pt2 = new stream.PassThrough();
      cb(
        emitterPtcl.createEmitter({readable: pt1, writable: pt2}),
        listenerPtcl.createListener({readable: pt2, writable: pt1})
      );
    }

  });

  suite('stateless', function () {

    test('same protocol', function (done) {
      var ptcl = createProtocol({
        protocol: 'Echo',
        messages: {
          echo: {
            request: [{name: 'id', type: 'string'}],
            response: 'string'
          }
        }
      }).onMessage('echo', function (req, cb) {
          cb(null, req.id);
        });
      statelessSetup(ptcl, ptcl, function (emitter) {
        emitter.emitMessage('echo', {id: 'hello'}, function (err, res) {
          assert.equal(res, 'hello');
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
      }).onMessage('wait', function (req, cb) {
          setTimeout(function () { cb(null, 'ok'); }, req.ms);
        });
      var interrupted = 0;
      var eoted = false;
      statelessSetup(ptcl, ptcl, function (emitter) {
        emitter.on('eot', function (pending) {
          eoted = true;
          assert.equal(pending, 2);
          assert.equal(interrupted, 2);
          done();
        });
        emitter.emitMessage('wait', {ms: 75}, interruptedCb);
        emitter.emitMessage('wait', {ms: 50}, interruptedCb);
        emitter.emitMessage('wait', {ms: 10}, function (err, res) {
          assert.equal(res, 'ok');
          emitter.destroy(true);
        });

        function interruptedCb(err) {
          assert(/interrupted/.test(err.string));
          interrupted++;
        }
      });
    });

    function statelessSetup(emitterPtcl, listenerPtcl, cb) {
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
    .pipe(new protocols.streams.MessageEncoder(frameSize || 64));
}

function createWritableTransport(bufs) {
  var decoder = new protocols.streams.MessageDecoder();
  decoder.pipe(createWritableStream(bufs));
  return decoder;
}

function createTransport(readBufs, writeBufs) {
  return toDuplex(
    createReadableTransport(readBufs),
    createWritableTransport(writeBufs)
  );
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
