/* jshint node: true, mocha: true */

'use strict';

var protocols = require('../lib/protocols'),
    assert = require('assert'),
    stream = require('stream');

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
      var n = 0;
      var readable = new stream.Readable({
        read: function () {
          var buf = parts[n++];
          this.push(buf ? frame(buf) : null);
        }
      });
      var writable = new stream.Writable({
        write: function (buf, encoding, cb) {
          messages.push(buf);
          cb();
        }
      }).on('finish', function () {
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
      var n = 0;
      var readable = new stream.Readable({
        read: function () {
          var buf = parts[n++];
          this.push(buf ? frame(buf) : null);
        }
      });
      var writable = new stream.Writable({
        write: function (buf, encoding, cb) {
          messages.push(buf);
          cb();
        }
      });
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
      var n = 0;
      var readable = new stream.Readable({
        read: function () {
          this.push(messages[n++] || null);
        }
      });
      var writable = new stream.Writable({
        write: function (buf, encoding, cb) {
          frames.push(buf);
          cb();
        }
      });
      readable
        .pipe(new MessageEncoder())
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

    var protocol = new Protocol({
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

    test('client server', function (done) {
      var pt1 = new stream.PassThrough();
      var pt2 = new stream.PassThrough();
      var client = protocol.createClient({readable: pt1, writable: pt2});
      protocol.createServer()
        .addTransport({readable: pt2, writable: pt1})
        .onMessage('m1', function (params, cb) {
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

    test('parallel client server', function (done) {
      var pt1 = new stream.PassThrough();
      var pt2 = new stream.PassThrough();
      var client = protocol.createClient({readable: pt1, writable: pt2});
      protocol.createServer()
        .addTransport({readable: pt2, writable: pt1})
        .onMessage('m2', function (params, cb) {
          var num = params.number; // Longer timeout for first messages.
          setTimeout(function () { cb(null, num); }, num);
        });

      var numbers = irange(500);
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
