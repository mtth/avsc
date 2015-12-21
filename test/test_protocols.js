/* jshint node: true, mocha: true */

'use strict';

var protocols = require('../lib/protocols'),
    assert = require('assert'),
    stream = require('stream'),
    util = require('util');

var HANDSHAKE_REQUEST_TYPE = protocols.HANDSHAKE_REQUEST_TYPE;
var HANDSHAKE_RESPONSE_TYPE = protocols.HANDSHAKE_RESPONSE_TYPE;
// var MessageDecoder = protocols.streams.MessageDecoder;
var createProtocol = protocols.createProtocol;
var MessageEncoder = protocols.streams.MessageEncoder;
// var listeners = protocols.listeners;
var emitters = protocols.emitters;


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
      p.clearCache();
    });

    test('missing messages', function () {
      assert.doesNotThrow(function () {
        createProtocol({namespace: 'com.acme', protocol: 'Hello'});
      });
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

    test('inspect', function () {
      var p = createProtocol({
        namespace: 'hello',
        protocol: 'World',
      });
      assert.equal(p.inspect(), '<Protocol "hello.World">');
    });

    test('createEmitter', function () {
      var p = createProtocol({protocol: 'Hi'});
      var d = createDuplexStream();
      var e = createDuplexStream();
      var c;
      c = p.createEmitter({readable: e, writable: d});
      assert(c instanceof emitters.StatefulEmitter);
      c = p.createEmitter(e);
      assert(c instanceof emitters.StatefulEmitter);
      c = p.createEmitter(function (cb) { cb(e); return d; });
      assert(c instanceof emitters.StatelessEmitter);
      c = p.createEmitter(e, foo);
      assert.equal(c.listeners('eot').length, 1);

      function foo() {}
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

    var StatefulEmitter = protocols.emitters.StatefulEmitter;

    test('ok handshake', function (done) {
      var buf = HANDSHAKE_RESPONSE_TYPE.toBuffer({match: 'BOTH'});
      var bufs = [];
      var ptcl = createNumberProtocol();
      var emitter = new StatefulEmitter(
        ptcl,
        createReadableStream([buf]),
        createWritableStream(bufs)
      );
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

    test('invalid handshake', function (done) {
      var ptcl = createNumberProtocol();
      // Pretend the hash was different.
      var hash = new Buffer(16);
      var resBufs = [
        {
          match: 'NONE',
          serverHash: {'org.apache.avro.ipc.MD5': hash},
          serverProtocol: {string: ptcl.toString()},
        },
        {match: 'BOTH'}
      ].map(function (val) { return HANDSHAKE_RESPONSE_TYPE.toBuffer(val); });
      var reqBufs = [];
      var emitter = new StatefulEmitter(
        ptcl,
        createReadableStream(resBufs),
        createWritableStream(reqBufs)
      );
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
      var ptcl = createNumberProtocol();
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
      var emitter = new StatefulEmitter(
        ptcl,
        createReadableStream(resBufs),
        createWritableStream([])
      );
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
      var emitter = new StatefulEmitter(
        createNumberProtocol(),
        createReadableStream(resBufs),
        createWritableStream([])
      );
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
      var emitter = new StatefulEmitter(
        createNumberProtocol(),
        createReadableStream(resBufs),
        createWritableStream([])
      );
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
      var ptcl = createNumberProtocol();
      var bufs = [];
      var emitter = new StatefulEmitter(
        ptcl,
        createReadableStream([]),
        createWritableStream([])
      );
      emitter
        .on('eot', function () {
          assert.equal(bufs.length, 0); // No handshake was sent.
          done();
        });
    });

    test('interrupted', function (done) {
      var ptcl = createNumberProtocol();
      var encoder = new MessageEncoder(64);
      var interrupted = 0;
      var emitter = new StatefulEmitter(
        ptcl,
        encoder,
        createWritableStream([])
      ).on('eot', function () {
        assert.equal(interrupted, 2);
        done();
      });

      encoder.write(HANDSHAKE_RESPONSE_TYPE.toBuffer({match: 'BOTH'}));
      emitter.emitMessage('parse', {string: '123'}, cb);
      emitter.emitMessage('parse', {string: '123'}, cb);
      encoder.end();

      function cb(err) {
        assert.deepEqual(err, {string: 'interrupted'});
        interrupted++;
      }
    });

  });

  suite('StatelessEmitter', function () {


  });

  suite('Emitter Server', function () {

    var ptcl = createProtocol({
      protocol: 'Test',
      messages: {
        m1: {
          request: [{name: 'number', type: 'int'}],
          response: 'string',
          errors: ['int']
        },
        m2: {
          request: [{name: 'number', type: 'int'}],
          response: 'int'
        }
      }
    });

    test('emitter server same protocol', function (done) {
      setupEmitterListener(ptcl, ptcl, function (emitter) {
        ptcl.onMessage('m1', function (params, cb) {
          var n = params.number;
          if (n % 2) {
            cb({'int': n});
          } else {
            cb(null, 'ok');
          }
        });
        emitter
          .emitMessage('m1', {number: 2}, function (err, res) {
            assert.strictEqual(err, null);
            assert.equal(res, 'ok');
            emitter.emitMessage('m1', {number: 3}, function (err) {
              assert.deepEqual(err, {'int': 3});
              done();
            });
          });
      });
    });

    test('parallel emitter server same protocol', function (done) {
      setupEmitterListener(ptcl, ptcl, function (emitter) {
        ptcl.onMessage('m2', function (params, cb) {
          var num = params.number; // Longer timeout for first messages.
          setTimeout(function () { cb(null, num); }, 10 * num);
        });

        var numbers = irange(10);
        var n = 0;
        numbers.forEach(emit);

        function emit(num) {
          emitter.emitMessage('m2', {number: num}, function (err, res) {
            assert.strictEqual(err, null);
            assert.equal(res, num);
            if (++n === numbers.length) {
              done();
            }
          });
        }
      });
    });

    test('emitter server compatible protocols', function (done) {
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
      setupEmitterListener(
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

    test('emitter server incompatible protocols', function (done) {
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
      setupEmitterListener(
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

  });

});

// Message framing.
function frame(buf) {
  var framed = new Buffer(buf.length + 4);
  framed.writeInt32BE(buf.length);
  buf.copy(framed, 4);
  return framed;
}

// Inverted range.
function irange(n) {
  var arr = [];
  while (n) {
    arr.push(n--);
  }
  return arr;
}

// A few simple protocols shared below.
function createNumberProtocol() {
  return createProtocol({
    protocol: 'Number',
    messages: {
      parse: {
        request: [{name: 'string', type: 'string'}],
        response: 'int'
      },
      add: {
        request: [{name: 'numbers', type: {type: 'array', items: 'int'}}],
        response: 'int'
      }
    }
  });
}

function setupEmitterListener(emitterPtcl, listenerPtcl, cb) {
  var pt1 = new stream.PassThrough();
  var pt2 = new stream.PassThrough();
  var emitter = emitterPtcl.createEmitter({readable: pt1, writable: pt2});
  var listener = listenerPtcl.createListener({readable: pt2, writable: pt1});
  cb(emitter, listener);
}

// Simplified constructor API isn't available in node <= 1.0.

function createReadableStream(bufs, noFrame) {
  var n = 0;
  function Stream() {
    stream.Readable.call(this);
  }
  util.inherits(Stream, stream.Readable);
  Stream.prototype._read = function () {
    this.push(bufs[n++] || null);
  };
  var readable = new Stream();
  return noFrame ?
    readable :
    readable.pipe(new protocols.streams.MessageEncoder(64));
}

function createWritableStream(bufs, noUnframe) {
  function Stream() {
    stream.Writable.call(this);
  }
  util.inherits(Stream, stream.Writable);
  Stream.prototype._write = function (buf, encoding, cb) {
    bufs.push(buf);
    cb();
  };
  var writable = new Stream();
  if (noUnframe) {
    return writable;
  } else {
    var decoder = new protocols.streams.MessageDecoder();
    decoder.pipe(writable);
    return decoder;
  }
}

// Black hole stream (ends when finished).
function createDuplexStream() {
  function Stream() {
    stream.Duplex.call(this);
    this.on('finish', function () { this.push(null); });
  }
  util.inherits(Stream, stream.Duplex);
  Stream.prototype._read = function () {};
  Stream.prototype._write = function (buf, encoding, cb) { cb(); };
  return new Stream();
}
