/* jshint node: true, mocha: true */

'use strict';

var protocols = require('../lib/protocols'),
    assert = require('assert'),
    stream = require('stream'),
    util = require('util');

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

  });

  suite('Message', function () {

    test('missing response', function () {
      assert.throws(function () {
        new protocols.Message({
          request: [{name: 'greeting', type: 'string'}]
        });
      });
    });

    test('invalid one-way', function () {
      // Non-null response.
      assert.throws(function () {
        new protocols.Message({
          request: [{name: 'greeting', type: 'string'}],
          response: 'string',
          'one-way': true
        });
      });
      // Non-empty errors.
      assert.throws(function () {
        new protocols.Message({
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
      var readable = createReadableStream(parts, true);
      var writable = createWritableStream(messages)
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
      var readable = createReadableStream(parts, true);
      var writable = createWritableStream(messages);
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

    var MessageEncoder = protocols.streams.MessageEncoder;

    test('ok', function (done) {
      var messages = [
        new Buffer([0, 1]),
        new Buffer([2])
      ];
      var frames = [];
      var readable = createReadableStream(messages);
      var writable = createWritableStream(frames);
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
        client.emitMessage('m1', {number: 2}, function (err, res) {
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
      setupClientServer(clientPtcl, serverPtcl, function (client, server) {
        server.onMessage('age', function (params, cb) {
          assert.equal(params.name, 'Ann');
          cb(null, 23);
        });
        client.emitMessage('age', {name: 'Ann'}, function (err, res) {
          assert.strictEqual(err, null);
          assert.equal(res, 23);
          done();
        });
      });
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

function setupClientServer(clientPtcl, serverPtcl, cb) {
  var pt1 = new stream.PassThrough();
  var pt2 = new stream.PassThrough();
  var client = clientPtcl.createClient({readable: pt1, writable: pt2});
  var server = serverPtcl.createServer()
    .addTransport({readable: pt2, writable: pt1});
  cb(client, server);
}

// Simplified constructor API isn't available in node <= 1.0.

function createReadableStream(bufs, isFramed) {
  var n = 0;
  function Stream() {
    stream.Readable.call(this);
  }
  util.inherits(Stream, stream.Readable);
  Stream.prototype._read = function () {
    var buf = bufs[n++];
    if (!buf) {
      this.push(null);
    } else {
      this.push(isFramed ? frame(buf) : buf);
    }
  };
  return new Stream();
}

function createWritableStream(bufs) {
  function Stream() {
    stream.Writable.call(this);
  }
  util.inherits(Stream, stream.Writable);
  Stream.prototype._write = function (buf, encoding, cb) {
    bufs.push(buf);
    cb();
  };
  return new Stream();
}
