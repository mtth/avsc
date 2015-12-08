/* jshint node: true, mocha: true */

'use strict';

var protocols = require('../lib/protocols'),
    assert = require('assert'),
    stream = require('stream');

var Protocol = protocols.Protocol;
var transports = protocols.transports;

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

  suite('transports', function () {

    suite('StreamTransport', function () {

      test('read', function (done) {
        var bufs = [
          new Buffer([0, 1]),
          new Buffer([2]),
          new Buffer([]),
          new Buffer([3, 4, 5]),
          new Buffer([])
        ];
        var n = 0;
        var readable = new stream.Readable({
          read: function () {
            var buf = bufs[n++];
            this.push(buf ? frame(buf) : null);
          }
        });
        var writable = new stream.Writable();
        var messages = [];
        var tm = new transports.StreamTransport(readable, writable);
        tm.on('data', function (buf) { messages.push(buf); })
          .on('end', function () {
            assert.deepEqual(messages, [
              {data: new Buffer([0, 1, 2]), hint: 0},
              {data: new Buffer([3, 4, 5]), hint: 0}
            ]);
            done();
          });
      });

      test('write', function (done) {
        var bufs = [];
        var nReads = 0;
        var readable = new stream.Readable({read: function () { nReads++; }});
        var writable = new stream.Writable({
          write: function (chunk, encoding, cb) {
            bufs.push(chunk);
            cb();
          }
        });
        var tm = new transports.StreamTransport(readable, writable);
        tm.write({data: new Buffer([0, 1])});
        tm.write({data: new Buffer([3])});
        tm.end();
        writable
          .on('finish', function () {
            assert.equal(nReads, 1);
            assert.deepEqual(
              Buffer.concat(bufs),
              Buffer.concat([
                new Buffer([0, 1]),
                new Buffer([]),
                new Buffer([3]),
                new Buffer([]),
              ].map(frame))
            );
            done();
          });
      });

      test('end', function (done) {
        var bufs = [];
        var nReads = 0;
        var readable = new stream.Readable({read: function () { nReads++; }});
        var writable = new stream.Writable({
          write: function (chunk, encoding, cb) {
            bufs.push(chunk);
            cb();
          }
        });
        var tm = new transports.StreamTransport(readable, writable, {end: true});
        tm.write({data: new Buffer([0, 1])});
        writable
          .on('finish', function () {
            assert.equal(nReads, 1);
            assert.deepEqual(
              Buffer.concat(bufs),
              Buffer.concat([
                new Buffer([0, 1]),
                new Buffer([])
              ].map(frame))
            );
            done();
          });
      });

    });

    suite('CallbackTransport', function () {

      test('roundtrip', function (done) {
        var objs = [];
        var ct = new transports.CallbackTransport(function (cb) {
          // Similar-ish to what an echo `http.request` would do.
          var bufs = [];
          return new stream.Writable({
              write: function (chunk, encoding, cb) {
                bufs.push(chunk);
                cb();
              }
            }).on('finish', function () {
              var n = 0;
              cb(new stream.Readable({
                read: function () { this.push(bufs[n++] || null); }
              }));
            });
        }).on('data', function (obj) { objs.push(obj); })
          .on('end', function () {
            assert.deepEqual(objs, [
              {data: new Buffer([0, 1, 2]), hint: 1},
              {data: new Buffer([3, 4]), hint: 5}
            ]);
            done();
          });

        ct.write({data: new Buffer([0, 1, 2]), hint: 1});
        ct.write({data: new Buffer([3, 4]), hint: 5});
        ct.end();
      });

    });

    function frame(buf) {
      var framed = new Buffer(buf.length + 4);
      framed.writeInt32BE(buf.length);
      buf.copy(framed, 4);
      return framed;
    }

  });

  suite('Emitter', function () {

    var Emitter = protocols.Emitter;

    var protocol = new Protocol({
      protocol: 'Test',
      messages: {
        m1: {
          request: [{name: 'number', type: 'int'}],
          response: 'string',
          errors: ['int']
        }
      }
    });

    test('client server', function (done) {
      var pt1 = new stream.PassThrough();
      var pt2 = new stream.PassThrough();
      var client = new Emitter(protocol, {mode: 'client'});
      client.setTransport(new transports.StreamTransport(pt1, pt2));
      var server = new Emitter(protocol, {mode: 'server'});
      server.setTransport(new transports.StreamTransport(pt2, pt1));
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

});
