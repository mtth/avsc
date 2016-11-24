/* jshint node: true, mocha: true */

'use strict';

var types = require('../lib/types'),
    protocols = require('../lib/protocols'),
    assert = require('assert'),
    stream = require('stream'),
    util = require('util');


var Protocol = protocols.Protocol;


suite('protocols', function () {

  suite('Protocol', function () {

    test('get name and types', function () {
      var p = Protocol.create({
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
      var ptcl = Protocol.create({namespace: 'com.acme', protocol: 'Hello'});
      assert.throws(function () {
        ptcl.on('add', function () {});
      }, /unknown/);
    });

    test('missing name', function () {
      assert.throws(function () {
        Protocol.create({namespace: 'com.acme', messages: {}});
      });
    });

    test('missing type', function () {
      assert.throws(function () {
        Protocol.create({
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

    test('multiple references to namespaced types', function () {
      // This test is a useful sanity check for hoisting implementations.
      var n = 0;
      var p = Protocol.create({
        protocol: 'Hello',
        namespace: 'ping',
        types: [
          {
            name: 'Ping',
            type: 'record',
            fields: []
          },
          {
            name: 'Pong',
            type: 'record',
            fields: [{name: 'ping', type: 'Ping'}]
          },
          {
            name: 'Pung',
            type: 'record',
            fields: [{name: 'ping', type: 'Ping'}]
          }
        ]
      }, {typeHook: hook});
      assert.equal(p.getType('ping.Ping').getTypeName(), 'record');
      assert.equal(p.getType('ping.Pong').getTypeName(), 'record');
      assert.equal(p.getType('ping.Pung').getTypeName(), 'record');
      assert.equal(n, 5);

      function hook() { n++; }
    });

    test('special character in name', function () {
      assert.throws(function () {
        Protocol.create({
          protocol: 'Ping',
          messages: {
            'ping/1': {
              request: [],
              response: 'string'
            }
          }
        });
      }, /invalid message name/);
    });

    test('get messages', function () {
      var ptcl;
      ptcl = Protocol.create({protocol: 'Empty'});
      assert.deepEqual(ptcl.getMessages(), {});
      ptcl = Protocol.create({
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
      var ptcl = Protocol.create({
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
      var p1 = Protocol.create({protocol: 'Hey'});
      var p2 = Protocol.create({protocol: 'Hi'});
      var ee = p2.createEmitter(new stream.PassThrough(), {noPing: true});
      assert.throws(
        function () { p1.emit('hi', {}, ee); },
        /invalid emitter/
      );
      done();
    });

    test('getFingerprint', function () {
      var p = Protocol.create({
        namespace: 'hello',
        protocol: 'World',
      });
      assert.deepEqual(p.getFingerprint('md5'), p.getFingerprint());
    });

    test('toString', function () {
      var p = Protocol.create({
        namespace: 'hello',
        protocol: 'World',
      });
      assert.equal(p.toString(), '{"protocol":"hello.World"}');
    });

    test('inspect', function () {
      var p = Protocol.create({
        namespace: 'hello',
        protocol: 'World',
      });
      assert.equal(p.inspect(), '<Protocol "hello.World">');
    });

    test('using constructor', function () {
      var p = new protocols.Protocol({protocol: 'Empty'});
      assert.equal(p.getName(), 'Empty');
      assert.deepEqual(p.getMessages(), []);
    });

    test('namespacing', function () {
      var p;

      p = newProtocol('foo.Foo', '');
      assert.equal(p.getName(), 'foo.Foo');
      assert(p.getType('Bar'));
      assert(p.getType('Baz'));

      p = newProtocol('foo.Foo');
      assert.equal(p.getName(), 'foo.Foo');
      assert(p.getType('foo.Bar'));
      assert(p.getType('Baz'));

      p = newProtocol('Foo', 'bar');
      assert.equal(p.getName(), 'bar.Foo');
      assert(p.getType('bar.Bar'));
      assert(p.getType('Baz'));

      p = newProtocol('Foo', 'bar', {namespace: 'opt'});
      assert.equal(p.getName(), 'bar.Foo');
      assert(p.getType('bar.Bar'));
      assert(p.getType('Baz'));

      p = newProtocol('Foo', undefined, {namespace: 'opt'});
      assert.equal(p.getName(), 'opt.Foo');
      assert(p.getType('opt.Bar'));
      assert(p.getType('Baz'));

      p = newProtocol('.Foo', undefined, {namespace: 'opt'});
      assert.equal(p.getName(), 'Foo');
      assert(p.getType('Bar'));
      assert(p.getType('Baz'));

      function newProtocol(name, namespace, opts) {
        return new protocols.Protocol({
          protocol: name,
          namespace: namespace,
          types: [
            {type: 'record', name: 'Bar', fields: []},
            {type: 'record', name: '.Baz', fields: []}
          ]
        }, opts);
      }
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

    test('toString', function () {
      var attrs = {
        request: [{name: 'ping', type: 'string'}],
        response: 'null',
      };
      var m = new Message('Ping', attrs);
      assert.deepEqual(JSON.parse(m.toString()), attrs);
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
      var type = types.Type.create({
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
      var type = types.Type.create({
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

    test('mask', function (done) {
      var ctx = {one: 1};
      var n = 0;
      var reg = new Registry(ctx, 31);
      assert.equal(reg.add(10, fn), 1);
      assert.equal(reg.add(10, fn), 0);
      reg.get(4)(null);
      reg.get(3)(null);

      function fn(err) {
        assert.strictEqual(err, null);
        if (++n == 2) {
          done();
        }
      }
    });

  });

  suite('StatefulEmitter', function () {

    test('connection timeout', function (done) {
      var ptcl = Protocol.create({
        protocol: 'Ping',
        messages: {ping: {request: [], response: 'boolean'}}
      });
      var transport = {
        readable: new stream.PassThrough(),
        writable: new stream.PassThrough()
      };
      ptcl.createEmitter(transport, {timeout: 5})
        .on('error', function (err) {
          assert(/connection timeout/.test(err));
          assert(this.isDestroyed());
          done();
        });
    });

    test('custom timeout', function (done) {
      var ptcl = Protocol.create({
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
      ee.emitMessage('ping', {request: {}}, {timeout: 10}, function (err) {
        assert(/timeout/.test(err));
        ee.destroy();
      });
    });

    test('missing message & callback', function (done) {
      var ptcl = Protocol.create({
        protocol: 'Ping',
        messages: {ping: {request: [], response: 'boolean'}}
      });
      var transport = new stream.PassThrough({objectMode: true});
      var ee = ptcl.createEmitter(transport, {objectMode: true, timeout: 0})
        .on('eot', function () { done(); });
      assert.throws(function () {
        ee.emitMessage({message: ptcl.getMessage('ping'), request: {}});
      }, /missing callback/);
      ee.emitMessage('foo', {request: {}}, function (err) {
        assert(/missing message/.test(err));
        ee.destroy();
      });
    });

    test('invalid response', function (done) {
      var ptcl = Protocol.create({
        protocol: 'Ping',
        messages: {ping: {request: [], response: 'boolean'}}
      });
      var transport = {
        readable: new stream.PassThrough({objectMode: true}),
        writable: new stream.PassThrough({objectMode: true})
      };
      var opts = {noPing: true, objectMode: true, timeout: 0};
      var ee = ptcl.createEmitter(transport, opts)
        .on('eot', function () { done(); });
      assert.strictEqual(ee.getTimeout(), 0);
      ee.emitMessage('ping', {request: {}}, function (err) {
        assert(/invalid response/.test(err));
        assert(!this.isDestroyed());
        this.destroy();
      });
      transport.writable.once('data', function (obj) {
        assert.deepEqual(
          obj,
          {id: 1, payload: [new Buffer('\x00\x08ping', 'binary')]}
        );
        transport.readable.write({id: obj.id, payload: [new Buffer([3])]});
      });
    });

    test('readable ended', function (done) {
      var ptcl = Protocol.create({
        protocol: 'Ping',
        messages: {ping: {request: [], response: 'boolean'}}
      });
      var transports = createPassthroughTransports();
      ptcl.createEmitter(transports[0]).on('eot', function () { done(); });
      transports[0].readable.push(null);
    });

    test('writable finished', function (done) {
      var ptcl = Protocol.create({
        protocol: 'Ping',
        messages: {ping: {request: [], response: 'boolean'}}
      });
      // We must use object mode here since ending the encoding stream won't
      // end the underlying writable stream.
      var transports = createPassthroughTransports(true);
      ptcl.createEmitter(transports[0], {objectMode: true})
        .on('eot', function () { done(); });
      transports[0].writable.end();
    });

    test('keep writable open', function (done) {
      var ptcl = Protocol.create({
        protocol: 'Ping',
        messages: {ping: {request: [], response: 'boolean'}}
      });
      // We must use object mode here since ending the encoding stream won't
      // end the underlying writable stream.
      var transports = createPassthroughTransports(true);
      var ee = ptcl.createEmitter(
        transports[0],
        {objectMode: true, endWritable: false}
      ).on('eot', function () {
        transports[0].writable.write({}); // Doesn't fail.
        done();
      });
      ee.destroy();
    });

    test('discover protocol', function (done) {
      // Check that we can interrupt a handshake part-way, so that we can ping
      // a remote server for its protocol, but still reuse the same connection
      // for a later trasnmission.
      var p1 = Protocol.create({protocol: 'Empty'});
      var p2 = Protocol.create({
        protocol: 'Ping',
        messages: {ping: {request: [], response: 'boolean'}}
      }).on('ping', function (req, ml, cb) { cb(null, true); });
      var transports = createPassthroughTransports();
      p2.createListener(transports[0]);
      p1.createEmitter(transports[1])
        .on('handshake', function (hreq, hres) {
          this.destroy();
          assert.equal(hres.serverProtocol, p2.getSchema());
        })
        .on('eot', function () {
          // The transports are still available for a connection.
          var me = p2.createEmitter(transports[1]);
          p2.emit('ping', {}, me, function (err, res) {
            assert.strictEqual(res, true);
            done();
          });
        });
    });

    test('destroy listener end', function (done) {
      var ptcl = Protocol.create({
        protocol: 'Math',
        messages: {
          negate: {
            request: [{name: 'n', type: 'int'}],
            response: 'int'
          }
        }
      }).on('negate', function (req, ee, cb) {
        ee.destroy(true);
        cb(null, -req.n);
      });
      var transports = createPassthroughTransports(true);
      var ee = ptcl.createEmitter(transports[0], {objectMode: true});
      var env = {request: {n: 20}};
      ee.emitMessage('negate', env, {timeout: 10}, function (err) {
        assert(/timeout/.test(err));
        done();
      });
    });

  });

  suite('StatelessEmitter', function () {

    test('factory error', function (done) {
      var ptcl = Protocol.create({
        protocol: 'Ping',
        messages: {ping: {request: [], response: 'boolean'}}
      }, {wrapUnions: true});
      var ee = ptcl.createEmitter(function (cb) {
        return new stream.PassThrough({objectMode: true})
          .on('finish', function () { cb(new Error('foobar')); });
      }, {noPing: true, objectMode: true, strictErrors: true});
      ptcl.emit('ping', {}, ee, function (err) {
        assert.deepEqual(err, {string: 'foobar'});
        assert(!ee.isDestroyed());
        done();
      });
    });

    test('default encoder error', function (done) {
      var ptcl = Protocol.create({
        protocol: 'Ping',
        messages: {ping: {request: [], response: 'boolean'}}
      }, {wrapUnions: true});
      var ee = ptcl.createEmitter(function (cb) {
        return new stream.PassThrough()
          .on('finish', function () { cb(new Error('foobar')); });
      }, {noPing: true, strictErrors: true});
      ptcl.emit('ping', {}, ee, function (err) {
        assert.deepEqual(err, {string: 'foobar'});
        assert(!ee.isDestroyed());
        done();
      });
    });

    test('reuse writable', function (done) {
      var ptcl = Protocol.create({
        protocol: 'Ping',
        messages: {ping: {request: [], response: 'null'}}
      });
      var readable = new stream.PassThrough({objectMode: true});
      var writable = new stream.PassThrough({objectMode: true})
        .on('data', function (data) {
          var hres = new Buffer([0, 0, 0, 0]); // Encoded handshake response.
          var res = new Buffer([0, 0]); // Encoded response (flag and meta).
          readable.write({id: data.id, payload: [hres, res]});
        });
      var ee = ptcl.createEmitter(function (cb) {
        cb(null, readable);
        return writable;
      }, {noPing: true, objectMode: true, endWritable: false});
      ptcl.emit('ping', {}, ee, function (err) {
        assert(!err);
        ptcl.emit('ping', {}, ee, function (err) {
          assert(!err); // We can reuse it.
          done();
        });
      });
    });

    test('interrupt writable', function (done) {
      var ptcl = Protocol.create({
        protocol: 'Ping',
        messages: {ping: {request: [], response: 'null'}}
      });
      // Fake handshake response.
      var hres = protocols.HANDSHAKE_RESPONSE_TYPE.clone({
        match: 'NONE',
        serverProtocol: ptcl.toString(),
        serverHash: ptcl.getFingerprint()
      });
      var readable = new stream.PassThrough({objectMode: true});
      var writable = new stream.PassThrough({objectMode: true})
        .on('data', function (data) {
          readable.write({id: data.id, payload: [hres.toBuffer()]});
        });
      var numHandshakes = 0;
      ptcl.createEmitter(function (cb) {
        cb(null, readable);
        return writable;
      }, {objectMode: true}).on('handshake', function (hreq, actualHres) {
        numHandshakes++;
        assert.deepEqual(actualHres, hres);
        this.destroy(true);
      }).on('error', function (err) {
        assert(/interrupted/.test(err));
      });
      setTimeout(function () {
        // Only a single handshake should have occurred.
        assert.equal(numHandshakes, 1);
        done();
      }, 50);
    });

  });

  suite('StatefulListener', function () {

    test('custom handler', function (done) {
      var ptcl = Protocol.create({
        protocol: 'Math',
        messages: {
          negate: {
            request: [{name: 'num', type: 'int'}],
            response: 'int'
          }
        }
      }).on('negate', skip);

      var transports = createPassthroughTransports();
      var reqEnv = {
        header: {one: new Buffer([1])},
        request: {num: 23}
      };
      var resEnv = {
        header: {two: new Buffer([2])},
        response: -23,
        error: undefined
      };

      ptcl.createListener(transports[0])
        .onMessage(function (name, env, ptcl, cb) {
          // Somehow message equality fails here (but not in the response
          // envelope below). This might be because a new protocol is created
          // from handshakes on the listener, but not on the emitter?
          assert.equal(name, 'negate');
          assert.equal(this.getPending(), 1);
          assert.deepEqual(env, reqEnv);
          assert.strictEqual(this.getProtocol().getHandler(name), skip);
          cb(null, resEnv);
        });

      var ee = ptcl.createEmitter(transports[1])
        .on('eot', function () {
          assert(this.isDestroyed());
          done();
        });

      ee.emitMessage('negate', reqEnv, function (err, env, meta) {
        assert.deepEqual(env, resEnv);
        assert(this.getProtocol().equals(meta.serverProtocol));
        this.destroy();
      });

      function skip() { throw new Error('no'); }
    });

    test('readable ended', function (done) {
      var ptcl = Protocol.create({
        protocol: 'Ping',
        messages: {ping: {request: [], response: 'boolean'}}
      });
      var transport = new stream.PassThrough();
      ptcl.createListener(transport).on('eot', function () {
        assert(this.isDestroyed());
        done();
      });
      transport.push(null);
    });

    test('writable finished', function (done) {
      var ptcl = Protocol.create({
        protocol: 'Ping',
        messages: {ping: {request: [], response: 'boolean'}}
      });
      // We must use object mode here since ending the encoding stream won't
      // end the underlying writable stream.
      var transports = createPassthroughTransports(true);
      ptcl.createListener(transports[0], {objectMode: true})
        .on('eot', function () { done(); });
      transports[0].writable.end();
    });

  });

  suite('StatelessListener', function () {

    test('factory error', function (done) {
      var ptcl = Protocol.create({
        protocol: 'Ping',
        messages: {ping: {request: [], response: 'boolean'}}
      });
      var err;
      ptcl.createListener(function (cb) {
        cb(new Error('bar'));
        return new stream.PassThrough();
      }).on('error', function () { err = arguments[0]; })
        .on('eot', function () {
          assert(/bar/.test(err));
          done();
        });
    });

    test('delayed writable', function (done) {
      var ptcl = Protocol.create({
        protocol: 'Ping',
        messages: {ping: {request: [], response: 'boolean'}}
      });
      var objs = [];
      var readable = new stream.PassThrough({objectMode: true});
      var writable = new stream.PassThrough({objectMode: true})
        .on('data', function (obj) { objs.push(obj); });
      ptcl.createListener(function (cb) {
        setTimeout(function () { cb(null, writable); }, 50);
        return readable;
      }, {objectMode: true}).on('eot', function () {
        assert.deepEqual(objs.length, 1);
        done();
      });
      readable.write({
        id: 0,
        payload: [
          protocols.HANDSHAKE_REQUEST_TYPE.toBuffer({
            clientHash: ptcl.getFingerprint(),
            serverHash: ptcl.getFingerprint()
          }),
          new Buffer([3]) // Invalid request contents.
        ]
      });
    });

    test('reuse writable', function (done) {
      var ptcl = Protocol.create({
        protocol: 'Ping',
        messages: {ping: {request: [], response: 'null'}}
      }).on('ping', function (req, ee, cb) {
        cb(null, null);
      });
      var payload = [
        protocols.HANDSHAKE_REQUEST_TYPE.toBuffer({
          clientHash: ptcl.getFingerprint(),
          serverHash: ptcl.getFingerprint()
        }),
        new Buffer('\x00\x08ping')
      ];
      var objs = [];
      var readable = new stream.PassThrough({objectMode: true});
      var writable = new stream.PassThrough({objectMode: true})
        .on('data', function (obj) { objs.push(obj); });
      var ee;
      ee = createListener()
        .on('eot', function () {
          assert.deepEqual(objs.length, 2);
          done();
        });
      readable.write({id: 0, payload: payload});
      readable.end({id: 1, payload: payload});

      function createListener() {
        return ptcl.createListener(function (cb) {
          cb(null, writable);
          return readable;
        }, {endWritable: false, noPing: true, objectMode: true});
      }
    });

  });

  suite('emit', function () {

    suite('stateful', function () {

      run(function (emitterPtcl, listenerPtcl, opts, cb) {
        if (!cb) {
          cb = opts;
          opts = undefined;
        }
        var pt1 = new stream.PassThrough();
        var pt2 = new stream.PassThrough();
        cb(
          emitterPtcl.createEmitter({readable: pt1, writable: pt2}, opts),
          listenerPtcl.createListener({readable: pt2, writable: pt1}, opts)
        );
      });

      test('explicit server fingerprint', function (done) {
        var transports = createPassthroughTransports();
        var p1 = Protocol.create({
          protocol: 'Math',
          messages: {
            negate: {
              request: [{name: 'n', type: 'int'}],
              response: 'long'
            }
          }
        });
        var p2 = Protocol.create({
          protocol: 'Math',
          messages: {
            negate: {
              request: [{name: 'n', type: 'long'}],
              response: 'int'
            }
          }
        });
        var ml1 = p2.createListener(transports[0]);
        var me1 = p1.createEmitter(transports[1]);
        me1.on('handshake', function (hreq, hres) {
          if (hres.match === 'NONE') {
            return;
          }
          // When we reach here a connection has been established, so both
          // emitter and listener caches have been populated with correct
          // adapters.
          var transports = createPassthroughTransports();
          p2.createListener(transports[0], {cache: ml1.getCache()})
            .once('handshake', onHandshake);
          p1.createEmitter(transports[1], {
            cache: me1.getCache(),
            serverFingerprint: p2.getFingerprint()
          }).once('handshake', onHandshake);

          var n = 0;
          function onHandshake(hreq, hres) {
            // The remote protocol should be available.
            assert.equal(hres.match, 'BOTH');
            if (++n === 2) {
              done();
            }
          }
        });
      });

      test('cached client fingerprint', function (done) {
        var transports = createPassthroughTransports();
        var p1 = Protocol.create({
          protocol: 'Math',
          messages: {
            negate: {
              request: [{name: 'n', type: 'int'}],
              response: 'long'
            }
          }
        });
        var p2 = Protocol.create({
          protocol: 'Math',
          messages: {
            negate: {
              request: [{name: 'n', type: 'long'}],
              response: 'int'
            }
          }
        });
        var ml1 = p2.createListener(transports[0]);
        var me1 = p1.createEmitter(transports[1], {timeout: 0});
        me1.on('handshake', function (hreq, hres) {
          if (hres.match === 'NONE') {
            return;
          }
          var transports = createPassthroughTransports();
          // The listener now has the client's protocol.
          p2.createListener(transports[0], {cache: ml1.getCache()})
            .once('handshake', function (hreq, hres) {
              assert.equal(hres.match, 'CLIENT');
              done();
            });
          p1.createEmitter(transports[1]);
        });
      });

      test('scoped transports', function (done) {
        var transports = createPassthroughTransports();
        var ptcl = Protocol.create({
          protocol: 'Case',
          messages: {
            upper: {
              request: [{name: 'str', type: 'string'}],
              response: 'string'
            }
          }
        }).on('upper', function (req, ee, cb) {
          cb(null, req.str.toUpperCase());
        });
        var meA = ptcl.createEmitter(transports[1], {scope: 'a'});
        ptcl.createListener(transports[0], {scope: 'a'});
        var meB = ptcl.createEmitter(transports[0], {scope: 'b'});
        ptcl.createListener(transports[1], {scope: 'b'});
        ptcl.emit('upper', {str: 'hi'}, meA, function (err, res) {
          assert.strictEqual(err, null);
          assert.equal(res, 'HI');
          ptcl.emit('upper', {str: 'hey'}, meB, function (err, res) {
            assert.strictEqual(err, null);
            assert.equal(res, 'HEY');
            done();
          });
        });
      });

    });

    suite('stateless', function () {

      run(function (emitterPtcl, listenerPtcl, opts, cb) {
        if (!cb) {
          cb = opts;
          opts = undefined;
        }
        cb(emitterPtcl.createEmitter(writableFactory, opts));

        function writableFactory(emitterCb) {
          var reqPt = new stream.PassThrough()
            .on('finish', function () {
              listenerPtcl.createListener(function (listenerCb) {
                var resPt = new stream.PassThrough()
                  .on('finish', function () { emitterCb(null, resPt); });
                listenerCb(null, resPt);
                return reqPt;
              }, opts);
            });
          return reqPt;
        }
      });

    });

    function run(setupFn) {

      test('primitive types', function (done) {
        var ptcl = Protocol.create({
          protocol: 'Math',
          messages: {
            negate: {
              request: [{name: 'n', type: 'int'}],
              response: 'long'
            }
          }
        });
        setupFn(ptcl, ptcl, function (ee) {
          ptcl.on('negate', function (req, ee, cb) { cb(null, -req.n); });
          var n1, n2;
          ee.on('eot', function () {
            assert.equal(n1, 1);
            assert.equal(n2, 0);
            done();
          }).once('handshake', function (hreq, hres) {
            // Allow the initial ping to complete.
            assert.equal(hres.match, 'BOTH');
            process.nextTick(function () {
              // Also let the pending count go down.
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
        });
      });

      test('emit receive error', function (done) {
        var ptcl = Protocol.create({
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
        var ptcl = Protocol.create({
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
        var ptcl = Protocol.create({
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
        var ptcl = Protocol.create({
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
        var ptcl = Protocol.create({
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

      test('wrapped remote protocol', function (done) {
        var ptcl1 = Protocol.create({
          protocol: 'Math',
          messages: {
            invert: {
              request: [{name: 'n', type: ['int', 'float']}],
              response: ['int', 'float']
            }
          }
        }, {wrapUnions: true});
        var ptcl2 = Protocol.create({
          protocol: 'Math',
          messages: {
            invert: {
              request: [{name: 'n', type: ['int', 'float']}],
              response: ['float', 'int']
            }
          }
        }, {wrapUnions: true}).on('invert', function (req, ee, cb) {
          if (req.n.int) {
            cb(null, {float: 1 / req.n.int});
          } else {
            cb(null, {int: (1 / req.n.float) | 0});
          }
        });
        setupFn(ptcl1, ptcl2, function (ee) {
          ptcl1.emit('invert', {n: {int: 10}}, ee, function (err, res) {
            assert(Math.abs(res.float - 0.1) < 1e-5);
            ptcl1.emit('invert', {n: {float: 10}}, ee, function (err, res) {
              assert.equal(res.int, 0);
              done();
            });
          });
        });
      });

      test('invalid response', function (done) {
        var ptcl = Protocol.create({
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
        var ptcl = Protocol.create({
          protocol: 'Math',
          messages: {
            sqrt: {
              request: [{name: 'n', type: 'float'}],
              response: 'float'
            }
          }
        }).on('sqrt', function (req, ee, cb) {
          var n = req.n;
          if (n === -1) {
            cb(new Error('no i')); // Invalid error (should be a string).
          } else if (n < 0) {
            cb({error: 'complex'}); // Also invalid error.
          } else {
            cb(undefined, Math.sqrt(n));
          }
        });
        setupFn(ptcl, ptcl, {strictErrors: true}, function (ee) {
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
        var ptcl = Protocol.create({
          protocol: 'Delay',
          messages: {
            w: {
              request: [
                {name: 'ms', type: 'float'},
                {name: 'id', type: 'string'}
              ],
              response: 'string'
            }
          }
        }).on('w', function (req, ee, cb) {
          var delay = req.ms;
          if (delay < 0) {
            cb('delay must be non-negative');
            return;
          }
          setTimeout(function () { cb(null, req.id); }, delay);
        });
        var ids = [];
        setupFn(ptcl, ptcl, function (ee) {
          var n1, n2, n3;
          ee.on('eot', function (pending) {
            assert.equal(pending, 0);
            assert.equal(n1, 1);
            assert.equal(n2, 2);
            assert.equal(n3, 3);
            assert.deepEqual(ids, [null, 'b', 'a']);
            done();
          }).once('handshake', function (hreq, hres) {
            assert.equal(hres.match, 'BOTH');
            process.nextTick(function () {
              n1 = ptcl.emit('w', {ms: 500, id: 'a'}, ee, function (err, res) {
                assert.strictEqual(err, null);
                ids.push(res);
              });
              n2 = ptcl.emit('w', {ms: 10, id: 'b'}, ee, function (err, res) {
                assert.strictEqual(err, null);
                ids.push(res);
                ee.destroy();
              });
              n3 = ptcl.emit('w', {ms: -10, id: 'c'}, ee, function (err, res) {
                assert(/non-negative/.test(err));
                ids.push(res);
              });
            });
          });
        });
      });

      test('compatible protocols', function (done) {
        var emitterPtcl = Protocol.create({
          protocol: 'emitterProtocol',
          messages: {
            age: {
              request: [{name: 'name', type: 'string'}],
              response: 'long'
            }
          }
        });
        var listenerPtcl = Protocol.create({
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
        var ptcl1 = Protocol.create({
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
        var ptcl2 = Protocol.create({
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
        var ptcl1 = Protocol.create({
          protocol: 'emitterProtocol',
          messages: {
            age: {
              request: [{name: 'name', type: 'string'}],
              response: 'long'
            }
          }
        });
        var ptcl2 = Protocol.create({
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
        var emitterPtcl = Protocol.create({
          protocol: 'emitterProtocol',
          messages: {
            age: {request: [{name: 'name', type: 'string'}], response: 'long'}
          }
        }, {wrapUnions: true});
        var listenerPtcl = Protocol.create({protocol: 'serverProtocol'});
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
        var emitterPtcl = Protocol.create({
          protocol: 'emitterProtocol',
          messages: {
            age: {request: [{name: 'name', type: 'string'}], response: 'long'}
          }
        }, {wrapUnions: true});
        var listenerPtcl = Protocol.create({
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
        var ptcl1 = Protocol.create({
          protocol: 'ptcl1',
          messages: {ping: {request: [], response: 'null', 'one-way': true}}
        });
        var ptcl2 = Protocol.create({
          protocol: 'ptcl2',
          messages: {ping: {request: [], response: 'null'}}
        });
        setupFn(ptcl1, ptcl2, function (ee) {
            ee.on('error', function (err) {
              // This will be called twice for stateful emitters: once when
              // interrupted, then for the incompatible protocol error.
              assert(err);
              this.destroy();
            }).on('eot', function () { done(); });
            ptcl1.emit('ping', {}, ee);
          }
        );
      });

      test('one way message', function (done) {
        var ptcl = Protocol.create({
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
        var ptcl = Protocol.create({
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

      test('duplicate message callback', function (done) {
        var ptcl = Protocol.create({
          protocol: 'ptcl',
          messages: {ping: {request: [], response: 'null'}} // Not one-way.
        });
        setupFn(ptcl, ptcl, function (ee) {
          ptcl.on('ping', function (req, ee, cb) {
            ee.on('error', function (err) {
              assert(/multiple/.test(err));
              done();
            });
            cb(null, null);
            cb(null, null);
          });
          ptcl.emit('ping', {}, ee);
        });
      });

      test('unknown message', function (done) {
        var ptcl = Protocol.create({protocol: 'Empty'});
        setupFn(ptcl, ptcl, function (ee) {
          assert.throws(
            function () { ptcl.emit('echo', {}, ee); },
            /unknown message/
          );
          done();
        });
      });

      test('unhandled message', function (done) {
        var ptcl = Protocol.create({
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

      test('timeout', function (done) {
        var ptcl = Protocol.create({
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
          ptcl.on('echo', function (req, ee, cb) {
            setTimeout(function () { cb(null, req.id); }, 50);
          });
          var env = {request: {id: 'foo'}};
          ee.emitMessage('echo', env, {timeout: 10}, function (err) {
            assert(/timeout/.test(err));
            setTimeout(function () {
              // Give enough time for the actual response to come back and
              // still check that nothing fails.
              done();
            }, 100);
          });
        });
      });

      test('destroy emitter noWait', function (done) {
        var ptcl = Protocol.create({
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
        var ptcl = Protocol.create({
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

      test('destroy listener noWait', function (done) {
        var ptcl = Protocol.create({
          protocol: 'Math',
          messages: {
            negate: {
              request: [{name: 'n', type: 'int'}],
              response: 'int'
            }
          }
        });
        setupFn(ptcl, ptcl, {endWritable: false}, function (ee) {
          ptcl.on('negate', function (req, ee, cb) {
            ee.destroy(true);
            cb(null, -req.n);
          });
          var env = {request: {n: 20}};
          ee.emitMessage('negate', env, {timeout: 10}, function(err) {
            assert(/timeout/.test(err));
            done();
          });
        });
      });

      test('catch server error', function (done) {
        var ptcl = Protocol.create({
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

  suite('discover attributes', function () {

    test('stateful ok', function (done) {
      var attrs = {
        protocol: 'Case',
        messages: {
          upper: {
            request: [{name: 'str', type: 'string'}],
            response: 'string'
          }
        }
      };
      var ptcl = Protocol.create(attrs).on('upper', function (req, ee, cb) {
        cb(null, req.str.toUpperCase());
      });
      var transports = createPassthroughTransports();
      ptcl.createListener(transports[1]);
      Protocol.discoverAttributes(transports[0], function (err, actualAttrs) {
        assert.strictEqual(err, null);
        assert.deepEqual(actualAttrs, attrs);
        // Check that the transport is still usable.
        var me = ptcl.createEmitter(transports[0]).on('eot', function() {
          done();
        });
        ptcl.emit('upper', {str: 'foo'}, me, function (err, res) {
          assert.strictEqual(err, null);
          assert.equal(res, 'FOO');
          me.destroy();
        });
      });
    });

    test('stateless ok', function (done) {
      var attrs = {
        protocol: 'Case',
        messages: {
          upper: {
            request: [{name: 'str', type: 'string'}],
            response: 'string'
          }
        }
      };
      var ptcl = Protocol.create(attrs).on('upper', function (req, ee, cb) {
        cb(null, req.str.toUpperCase());
      });
      Protocol.discoverAttributes(writableFactory, function (err, actual) {
        assert.strictEqual(err, null);
        assert.deepEqual(actual, attrs);
        // Check that the transport is still usable.
        var me = ptcl.createEmitter(writableFactory).on('eot', function() {
          done();
        });
        ptcl.emit('upper', {str: 'foo'}, me, function (err, res) {
          assert.strictEqual(err, null);
          assert.equal(res, 'FOO');
          me.destroy();
        });
      });

      function writableFactory(emitterCb) {
        var reqPt = new stream.PassThrough()
          .on('finish', function () {
            ptcl.createListener(function (listenerCb) {
              var resPt = new stream.PassThrough()
                .on('finish', function () { emitterCb(null, resPt); });
              listenerCb(null, resPt);
              return reqPt;
            });
          });
        return reqPt;
      }
    });

    test('stateful wrong scope', function (done) {
      var attrs = {
        protocol: 'Case',
        messages: {
          upper: {
            request: [{name: 'str', type: 'string'}],
            response: 'string'
          }
        }
      };
      var ptcl = Protocol.create(attrs).on('upper', function (req, ee, cb) {
        cb(null, req.str.toUpperCase());
      });
      var scope = 'bar';
      var transports = createPassthroughTransports();
      ptcl.createListener(transports[1], {scope: scope});
      Protocol.discoverAttributes(transports[0], {timeout: 5}, function (err) {
        assert(/timeout/.test(err));
        // Check that the transport is still usable.
        var me = ptcl.createEmitter(transports[0], {scope: scope})
          .on('eot', function() { done(); });
        ptcl.emit('upper', {str: 'foo'}, me, function (err, res) {
          assert.strictEqual(err, null);
          assert.equal(res, 'FOO');
          me.destroy();
        });
      });
    });

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

function createPassthroughTransports(objectMode) {
  var pt1 = stream.PassThrough({objectMode: objectMode});
  var pt2 = stream.PassThrough({objectMode: objectMode});
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
