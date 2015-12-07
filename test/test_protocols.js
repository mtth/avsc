/* jshint node: true, mocha: true */

'use strict';

var protocols = require('../lib/protocols'),
    schemas = require('../lib/schemas'),
    assert = require('assert'),
    stream = require('stream');


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
      assert(p.getTypes()['foo.Greeting'] instanceof schemas.types.RecordType);
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

  suite('Transmitter', function () {

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
      var tm = new protocols.Transmitter(readable, writable);
      tm.on('data', function (buf) { messages.push(buf); })
        .on('end', function () {
          assert.deepEqual(messages, [
            new Buffer([0, 1, 2]),
            new Buffer([3, 4, 5])
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
      var tm = new protocols.Transmitter(readable, writable, {end: true});
      tm.write(new Buffer([0, 1]));
      tm.write(new Buffer([3]));
      tm.end();
      writable.on('finish', function () {
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

    function frame(buf) {
      var framed = new Buffer(buf.length + 4);
      framed.writeInt32BE(buf.length);
      buf.copy(framed, 4);
      return framed;
    }

  });

});
