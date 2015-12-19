/* jshint node: true, mocha: true */

'use strict';

var protocols = require('../lib/protocols'),
    assert = require('assert'),
    stream = require('stream'),
    util = require('util');

var HANDSHAKE_REQUEST_TYPE = protocols.HANDSHAKE_REQUEST_TYPE;
var HANDSHAKE_RESPONSE_TYPE = protocols.HANDSHAKE_RESPONSE_TYPE;
var MessageEncoder = protocols.streams.MessageEncoder;
var Protocol = protocols.Protocol;


suite('protocols', function () {

  suite('Protocol', function () {

    test('valid', function () {
      var p = new protocols.Protocol({
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
    });

    test('missing messages', function () {
      assert.doesNotThrow(function () {
        new protocols.Protocol({namespace: 'com.acme', protocol: 'Hello'});
      });
    });

    test('missing name', function () {
      assert.throws(function () {
        new protocols.Protocol({namespace: 'com.acme', messages: {}});
      });
    });

    test('missing type', function () {
      assert.throws(function () {
        new protocols.Protocol({
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
      var p = new protocols.Protocol({
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

  suite('StatefulClient', function () {

    var StatefulClient = protocols.clients.StatefulClient;

    test('ok handshake', function (done) {
      var buf = HANDSHAKE_RESPONSE_TYPE.toBuffer({match: 'BOTH'});
      var bufs = [];
      var ptcl = createNumberProtocol();
      var client = new StatefulClient(
        ptcl,
        createReadableStream([buf]),
        createWritableStream(bufs)
      );
      var handshake = false;
      client
        .on('handshake', function (req, res) {
          handshake = true;
          assert(res.match === 'BOTH');
          assert.deepEqual(
            Buffer.concat(bufs),
            HANDSHAKE_REQUEST_TYPE.toBuffer({
              clientHash: ptcl.getHash(),
              serverHash: ptcl.getHash()
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
      var client = new StatefulClient(
        ptcl,
        createReadableStream(resBufs),
        createWritableStream(reqBufs)
      );
      var handshakes = 0;
      client
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
      // Pretend the hash was different.
      var hash = new Buffer(16);
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
      var client = new StatefulClient(
        ptcl,
        createReadableStream(resBufs),
        createWritableStream([])
      );
      var error = false;
      client
        .on('error', function (err) {
          error = true;
          assert.equal(err.message, 'abcd');
          this.destroy();
        })
        .on('eot', function () {
          assert(error);
          done();
        });
    });

    test('ended readable', function (done) {
      var ptcl = createNumberProtocol();
      var bufs = [];
      var client = new StatefulClient(
        ptcl,
        createReadableStream([]),
        createWritableStream([])
      );
      client
        .on('eot', function () {
          assert.equal(bufs.length, 0); // No handshake was sent.
          done();
        });
    });

    test('interrupted', function (done) {
      var ptcl = createNumberProtocol();
      var encoder = new MessageEncoder(64);
      var interrupted = 0;
      var client = new StatefulClient(
        ptcl,
        encoder,
        createWritableStream([])
      ).on('eot', function () {
        assert.equal(interrupted, 2);
        done();
      });

      encoder.write(HANDSHAKE_RESPONSE_TYPE.toBuffer({match: 'BOTH'}));
      client.emitMessage('parse', {string: '123'}, cb);
      client.emitMessage('parse', {string: '123'}, cb);
      encoder.end();

      function cb(err) {
        assert.deepEqual(err, {string: 'interrupted'});
        interrupted++;
      }
    });

  });

  suite('StatefulChannel', function () {

  });

  suite('Client Server', function () {

    var ptcl = new Protocol({
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

    test('client server same protocol', function (done) {
      setupClientServer(ptcl, ptcl, function (client, server) {
        server.onMessage('m1', function (params, cb) {
          var n = params.number;
          if (n % 2) {
            cb({'int': n});
          } else {
            cb(null, 'ok');
          }
        });
        client
          .emitMessage('m1', {number: 2}, function (err, res) {
            assert.strictEqual(err, null);
            assert.equal(res, 'ok');
            client.emitMessage('m1', {number: 3}, function (err) {
              assert.deepEqual(err, {'int': 3});
              done();
            });
          });
      });
    });

    test('parallel client server same protocol', function (done) {
      setupClientServer(ptcl, ptcl, function (client, server) {
        server.onMessage('m2', function (params, cb) {
          var num = params.number; // Longer timeout for first messages.
          setTimeout(function () { cb(null, num); }, 10 * num);
        });

        var numbers = irange(10);
        var n = 0;
        numbers.forEach(emit);

        function emit(num) {
          client.emitMessage('m2', {number: num}, function (err, res) {
            assert.strictEqual(err, null);
            assert.equal(res, num);
            if (++n === numbers.length) {
              done();
            }
          });
        }
      });
    });

    test('client server compatible protocols', function (done) {
      var clientPtcl = new Protocol({
        protocol: 'clientProtocol',
        messages: {
          age: {
            request: [{name: 'name', type: 'string'}],
            response: 'long'
          }
        }
      });
      var serverPtcl = new Protocol({
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
      setupClientServer(
        clientPtcl,
        serverPtcl,
        function (client, server) {
          server.onMessage('age', function (params, cb) {
            assert.equal(params.name, 'Ann');
            cb(null, 23);
          });
          client
            .emitMessage('age', {name: 'Ann'}, function (err, res) {
              assert.strictEqual(err, null);
              assert.equal(res, 23);
              done();
            });
        }
      );
    });

    test('client server incompatible protocols', function (done) {
      var clientPtcl = new Protocol({
        protocol: 'clientProtocol',
        messages: {
          age: {request: [{name: 'name', type: 'string'}], response: 'long'}
        }
      });
      var serverPtcl = new Protocol({
        protocol: 'serverProtocol',
        messages: {
          age: {request: [{name: 'name', type: 'int'}], response: 'long'}
        }
      });
      setupClientServer(
        clientPtcl,
        serverPtcl,
        function (client, server) {
          server.onMessage('age', function (params, cb) {
            assert.equal(params.name, 'Ann');
            cb(null, 23);
          });
          client
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
  return new Protocol({
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

function setupClientServer(clientPtcl, serverPtcl, cb) {
  var pt1 = new stream.PassThrough();
  var pt2 = new stream.PassThrough();
  var client = clientPtcl.createClient({readable: pt1, writable: pt2});
  var server = serverPtcl.createServer();
  var channel = server.createChannel({readable: pt2, writable: pt1});
  cb(client, server, channel);
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
