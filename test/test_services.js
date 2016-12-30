/* jshint node: true, mocha: true */

'use strict';

var types = require('../lib/types'),
    services = require('../lib/services'),
    assert = require('assert'),
    stream = require('stream'),
    util = require('util');


var Service = services.Service;


suite('services', function () {

  suite('Service', function () {

    test('get name, types, and protocol', function () {
      var p = {
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
      };
      var s = Service.forProtocol(p);
      assert.equal(s.getName(), 'foo.HelloWorld');
      assert.equal(s.getType('foo.Greeting').getTypeName(), 'record');
      assert.equal(s.getTypes().length, 2);
      assert.deepEqual(
        s.getTypes().map(function (t) { return t.getName(); }).sort(),
        ['foo.Curse', 'foo.Greeting']
      );
      assert.deepEqual(s.getProtocol(), p);
    });

    test('missing message', function () {
      var ptcl = Service.forProtocol({namespace: 'com.acme', protocol: 'Hello'});
      assert.throws(function () {
        ptcl.on('add', function () {});
      }, /unknown/);
    });

    test('missing name', function () {
      assert.throws(function () {
        Service.forProtocol({namespace: 'com.acme', messages: {}});
      });
    });

    test('missing type', function () {
      assert.throws(function () {
        Service.forProtocol({
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
      var p = Service.forProtocol({
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
        Service.forProtocol({
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
      ptcl = Service.forProtocol({protocol: 'Empty'});
      assert.deepEqual(ptcl.getMessages(), {});
      ptcl = Service.forProtocol({
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
      var ptcl = Service.forProtocol({
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
      var p1 = Service.forProtocol({protocol: 'Hey'});
      var p2 = Service.forProtocol({protocol: 'Hi'});
      var ee = p2.createEmitter(new stream.PassThrough(), {noPing: true});
      assert.throws(
        function () { p1.emit('hi', {}, ee); },
        /invalid emitter/
      );
      done();
    });

    test('getSchema', function () {
      var schema = {
        protocol: 'Hello',
        messages: {
          ping: {request: [], response: 'boolean', doc: ''},
          pong: {request: [], response: 'null', 'one-way': true}
        },
        doc: 'Hey'
      };
      var p = Service.forProtocol(schema);
      assert.deepEqual(p.getSchema({exportAttrs: true}), schema);
    });

    test('getSchema no top-level type references', function () {
      var schema = {
        protocol: 'Hello',
        types: [
          {
            type: 'record',
            name: 'Foo',
            fields: [
              {name: 'bar', type: {type: 'fixed', name: 'Bar', size: 4}}
            ]
          }
        ]
      };
      var p = Service.forProtocol(schema);
      var t = p.getType('Foo');
      // Bar's reference shouldn't be included in the returned types array.
      assert.deepEqual(p.getSchema().types, [t.getSchema()]);
    });

    test('getDocumentation', function () {
      var p = Service.forProtocol({protocol: 'Hello', doc: 'Hey'});
      assert.equal(p.getDocumentation(), 'Hey');
    });

    test('getFingerprint', function () {
      var p = Service.forProtocol({
        namespace: 'hello',
        protocol: 'World',
      });
      assert.deepEqual(p.getFingerprint('md5'), p.getFingerprint());
    });

    test('isService', function () {
      var p = Service.forProtocol({
        namespace: 'hello',
        protocol: 'World',
      });
      assert(Service.isService(p));
      assert(!Service.isService(undefined));
      assert(!Service.isService({protocol: 'bar'}));
    });

    test('equals', function () {
      var p = Service.forProtocol({
        namespace: 'hello',
        protocol: 'World',
      });
      assert(p.equals(p));
      assert(!p.equals(undefined));
      assert(!p.equals(Service.forProtocol({protocol: 'Foo'})));
    });

    test('inspect', function () {
      var p = Service.forProtocol({
        namespace: 'hello',
        protocol: 'World',
      });
      assert.equal(p.inspect(), '<Service "hello.World">');
    });

    test('using constructor', function () {
      var p = new services.Service({protocol: 'Empty'});
      assert.equal(p.getName(), 'Empty');
      assert.deepEqual(p.getMessages(), []);
    });

    test('namespacing', function () {
      var p;

      p = newService('foo.Foo', '');
      assert.equal(p.getName(), 'foo.Foo');
      assert(p.getType('Bar'));
      assert(p.getType('Baz'));

      p = newService('foo.Foo');
      assert.equal(p.getName(), 'foo.Foo');
      assert(p.getType('foo.Bar'));
      assert(p.getType('Baz'));

      p = newService('Foo', 'bar');
      assert.equal(p.getName(), 'bar.Foo');
      assert(p.getType('bar.Bar'));
      assert(p.getType('Baz'));

      p = newService('Foo', 'bar', {namespace: 'opt'});
      assert.equal(p.getName(), 'bar.Foo');
      assert(p.getType('bar.Bar'));
      assert(p.getType('Baz'));

      p = newService('Foo', undefined, {namespace: 'opt'});
      assert.equal(p.getName(), 'opt.Foo');
      assert(p.getType('opt.Bar'));
      assert(p.getType('Baz'));

      p = newService('.Foo', undefined, {namespace: 'opt'});
      assert.equal(p.getName(), 'Foo');
      assert(p.getType('Bar'));
      assert(p.getType('Baz'));

      function newService(name, namespace, opts) {
        return new services.Service({
          protocol: name,
          namespace: namespace,
          types: [
            {type: 'record', name: 'Bar', fields: []},
            {type: 'record', name: '.Baz', fields: []}
          ]
        }, opts);
      }
    });

    test('createClient transport option', function () {
      var ptcl = Service.forProtocol({protocol: 'Empty'});
      var client = ptcl.createClient({transport: new stream.PassThrough()});
      assert.deepEqual(client.getEmitters().length, 1);
    });

    test('createListener strict', function () {
      var ptcl = Service.forProtocol({protocol: 'Empty'});
      assert.throws(function () {
        ptcl.createListener(new stream.PassThrough(), {strictErrors: true});
      });
    });

  });

  suite('Message', function () {

    var Message = services.Message;

    test('empty errors', function () {
      var m = Message.forSchema('Hi', {
        request: [{name: 'greeting', type: 'string'}],
        response: 'int'
      });
      assert.deepEqual(m.getErrorType().toString(), '["string"]');
    });

    test('missing response', function () {
      assert.throws(function () {
        Message.forSchema('Hi', {
          request: [{name: 'greeting', type: 'string'}]
        });
      });
    });

    test('invalid one-way', function () {
      // Non-null response.
      assert.throws(function () {
        Message.forSchema('Hi', {
          request: [{name: 'greeting', type: 'string'}],
          response: 'string',
          'one-way': true
        });
      });
      // Non-empty errors.
      assert.throws(function () {
        Message.forSchema('Hi', {
          request: [{name: 'greeting', type: 'string'}],
          response: 'null',
          errors: ['int'],
          'one-way': true
        });
      });
    });

    test('getters', function () {
      var s = {
        request: [{name: 'ping', type: 'string'}],
        response: 'null'
      };
      var m = Message.forSchema('Ping', s);
      assert.equal(m.getName(), 'Ping');
      assert.equal(m.getRequestType().getFields()[0].getName(), 'ping');
      assert.equal(m.getResponseType().getName(true), 'null');
      assert.deepEqual(m.getSchema(), s);
      assert.strictEqual(m.isOneWay(), false);
    });

    test('getDocumentation', function () {
      var schema = {
        request: [{name: 'ping', type: 'string'}],
        response: 'null',
        doc: 'Pong'
      };
      var m = Message.forSchema('Ping', schema);
      assert.equal(m.getDocumentation(), 'Pong');
    });

    test('invalid types', function () {
      assert.throws(function () {
        new Message('intRequest', types.Type.forSchema('int'));
      }, /invalid request type/);
      assert.throws(function () {
        new Message(
          'intError',
          types.Type.forSchema({type: 'record', fields: []}),
          types.Type.forSchema('int')
        );
      }, /invalid error type/);
    });

  });

  suite('FrameDecoder & FrameEncoder', function () {

    var FrameDecoder = services.streams.FrameDecoder;
    var FrameEncoder = services.streams.FrameEncoder;

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
      var type = types.Type.forSchema({
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

    var NettyDecoder = services.streams.NettyDecoder;
    var NettyEncoder = services.streams.NettyEncoder;

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
      var type = types.Type.forSchema({
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

    var Registry = services.Registry;

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
      var ptcl = Service.forProtocol({
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
      var ptcl = Service.forProtocol({
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

    test('ping', function (done) {
      var svc = Service.forProtocol({protocol: 'Ping' });
      var transport = {
        readable: new stream.PassThrough({objectMode: true}),
        writable: new stream.PassThrough({objectMode: true})
      };
      var ee = svc.createClient()
        .createEmitter(transport, {noPing: true, objectMode: true, timeout: 5})
        .on('eot', function () { done(); });
      ee.ping(function (err) {
        assert(/timeout/.test(err), err);
        ee.destroy();
      });
    });

    test('missing message & callback', function (done) {
      var ptcl = Service.forProtocol({
        protocol: 'Ping',
        messages: {ping: {request: [], response: 'boolean'}}
      });
      var transports = createPassthroughTransports(true);
      var ee = ptcl.createEmitter(
        transports[0], {objectMode: true, timeout: 0}
      ).on('eot', function () { done(); });
      assert.throws(function () {
        ee.emitMessage({message: ptcl.getMessage('ping'), request: {}});
      }, /missing callback/);
      ee.emitMessage('foo', {request: {}}, function (err) {
        assert(/unknown message/.test(err));
        ee.destroy();
      });
    });

    test('invalid response', function (done) {
      var ptcl = Service.forProtocol({
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
        assert.equal(err.rpcCode, 'INVALID_RESPONSE');
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
      var ptcl = Service.forProtocol({
        protocol: 'Ping',
        messages: {ping: {request: [], response: 'boolean'}}
      });
      var transports = createPassthroughTransports();
      ptcl.createEmitter(transports[0]).on('eot', function () { done(); });
      transports[0].readable.push(null);
    });

    test('writable finished', function (done) {
      var ptcl = Service.forProtocol({
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
      var ptcl = Service.forProtocol({
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

    test('discover service', function (done) {
      // Check that we can interrupt a handshake part-way, so that we can ping
      // a remote server for its service, but still reuse the same connection
      // for a later trasnmission.
      var p1 = Service.forProtocol({protocol: 'Empty'});
      var p2 = Service.forProtocol({
        protocol: 'Ping',
        messages: {ping: {request: [], response: 'boolean'}}
      }).on('ping', function (req, ml, cb) { cb(null, true); });
      var transports = createPassthroughTransports();
      p2.createListener(transports[0]);
      p1.createEmitter(transports[1], {endWritable: false})
        .on('handshake', function (hreq, hres) {
          this.destroy();
          assert.equal(hres.serverProtocol, JSON.stringify(p2.getSchema()));
        })
        .on('eot', function () {
          // The transports are still available for a connection.
          var me = p2.createEmitter(transports[1]);
          p2.emit('ping', {}, me, function (err, res) {
            assert.strictEqual(err, null);
            assert.strictEqual(res, true);
            me.on('eot', function () { done(); })
              .destroy();
          });
        });
    });

    test('destroy listener end', function (done) {
      var ptcl = Service.forProtocol({
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
      var ptcl = Service.forProtocol({
        protocol: 'Ping',
        messages: {ping: {request: [], response: 'boolean'}}
      }, {wrapUnions: true});
      var ee = ptcl.createEmitter(function (cb) {
        return new stream.PassThrough({objectMode: true})
          .on('finish', function () { cb(new Error('foobar')); });
      }, {noPing: true, objectMode: true, strictErrors: true});
      ptcl.emit('ping', {}, ee, function (err) {
        assert(/foobar/.test(err.string), err);
        assert(!ee.isDestroyed());
        done();
      });
    });

    test('default encoder error', function (done) {
      var ptcl = Service.forProtocol({
        protocol: 'Ping',
        messages: {ping: {request: [], response: 'boolean'}}
      }, {wrapUnions: true});
      var ee = ptcl.createEmitter(function (cb) {
        return new stream.PassThrough()
          .on('finish', function () { cb(new Error('foobar')); });
      }, {noPing: true, strictErrors: true});
      ptcl.emit('ping', {}, ee, function (err) {
        assert(/foobar/.test(err.string));
        assert(!ee.isDestroyed());
        done();
      });
    });

    test('reuse writable', function (done) {
      var ptcl = Service.forProtocol({
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

    test('invalid handshake response', function (done) {
      var ptcl = Service.forProtocol({
        protocol: 'Ping',
        messages: {ping: {request: [], response: 'null'}}
      });
      var readable = new stream.PassThrough({objectMode: true});
      var writable = new stream.PassThrough({objectMode: true})
        .on('data', function (data) {
          var buf = new Buffer([0, 0, 0, 2, 48]);
          readable.write({id: data.id, payload: [buf]});
        });
      var ee = ptcl.createEmitter(function (cb) {
        cb(null, readable);
        return writable;
      }, {noPing: true, objectMode: true, endWritable: false});
      ptcl.emit('ping', {}, ee, function (err) {
        assert(/INVALID_HANDSHAKE_RESPONSE/.test(err.rpcCode));
        done();
      });
    });

    test('interrupt writable', function (done) {
      var ptcl = Service.forProtocol({
        protocol: 'Ping',
        messages: {ping: {request: [], response: 'null'}}
      });
      // Fake handshake response.
      var hres = services.HANDSHAKE_RESPONSE_TYPE.clone({
        match: 'NONE',
        serverService: ptcl.toString(),
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
      var ptcl = Service.forProtocol({
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
        .onMessage(function (name, env, cb) {
          // Somehow message equality fails here (but not in the response
          // envelope below). This might be because a new service is created
          // from handshakes on the listener, but not on the emitter?
          assert.equal(name, 'negate');
          assert.equal(this.getPending(), 1);
          assert.deepEqual(env, reqEnv);
          assert.throws(function () {
            ptcl.getHandler(name)();
          }, /nothing/);
          assert.strictEqual(ptcl.getHandler('foo'), undefined);
          cb(null, resEnv);
        });

      var ee = ptcl.createEmitter(transports[1])
        .on('eot', function () {
          assert(this.isDestroyed());
          done();
        });

      ee.emitMessage('negate', reqEnv, function (err, env) {
        assert.deepEqual(env, resEnv);
        assert(this.getProtocol().equals(ptcl));
        this.destroy();
      });

      function skip() { throw new Error('nothing'); }
    });

    test('readable ended', function (done) {
      var ptcl = Service.forProtocol({
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
      var ptcl = Service.forProtocol({
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
      var ptcl = Service.forProtocol({
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
      var ptcl = Service.forProtocol({
        protocol: 'Ping',
        messages: {ping: {request: [], response: 'boolean', errors: ['int']}}
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
          services.HANDSHAKE_REQUEST_TYPE.toBuffer({
            clientHash: ptcl.getFingerprint(),
            serverHash: ptcl.getFingerprint()
          }),
          new Buffer([3]) // Invalid request contents.
        ]
      });
    });

    test('reuse writable', function (done) {
      var ptcl = Service.forProtocol({
        protocol: 'Ping',
        messages: {ping: {request: [], response: 'null'}}
      }).on('ping', function (req, ee, cb) {
        cb(null, null);
      });
      var payload = [
        services.HANDSHAKE_REQUEST_TYPE.toBuffer({
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

  suite('emitters & listeners', function () { // <5.0 API.

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
        var p1 = Service.forProtocol({
          protocol: 'Math',
          messages: {
            negate: {
              request: [{name: 'n', type: 'int'}],
              response: 'long'
            }
          }
        });
        var p2 = Service.forProtocol({
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
            serverHash: p2.getHash(),
          }).once('handshake', onHandshake);

          var n = 0;
          function onHandshake(hreq, hres) {
            // The remote service should be available.
            assert.equal(hres.match, 'BOTH');
            if (++n === 2) {
              done();
            }
          }
        });
      });

      test('cached client fingerprint', function (done) {
        var transports = createPassthroughTransports();
        var p1 = Service.forProtocol({
          protocol: 'Math',
          messages: {
            negate: {
              request: [{name: 'n', type: 'int'}],
              response: 'long'
            }
          }
        });
        var p2 = Service.forProtocol({
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
          // The listener now has the client's service.
          p2.createListener(transports[0], {cache: ml1.getCache()})
            .once('handshake', function (hreq, hres) {
              assert.equal(hres.match, 'CLIENT');
              var cache = this.getCache();
              var adapter = cache[p1.getFingerprint()];
              assert(adapter.getClientProtocol().equals(p1));
              assert(adapter.getServerProtocol().equals(this.getProtocol()));
              done();
            });
          p1.createEmitter(transports[1]);
        });
      });

      test('scoped transports', function (done) {
        var transports = createPassthroughTransports();
        var ptcl = Service.forProtocol({
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
        var ptcl = Service.forProtocol({
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
            assert.equal(n2, 1);
            done();
          }).once('handshake', function (hreq, hres) {
            // Allow the initial ping to complete.
            assert.equal(hres.match, 'BOTH');
            setTimeout(function () {
              // Also let the pending count go down.
              n1 = ptcl.emit('negate', {n: 20}, ee, function (err, res) {
                assert.equal(this, ptcl);
                assert.strictEqual(err, null);
                assert.equal(res, -20);
                n2 = this.emit('negate', {n: 'hi'}, ee, function (err) {
                  assert.equal(err.rpcCode, 'INVALID_REQUEST');
                  process.nextTick(function () { ee.destroy(); });
                });
              });
            }, 0);
          });
        });
      });

      test('emit receive error', function (done) {
        var ptcl = Service.forProtocol({
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
        var ptcl = Service.forProtocol({
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
        var ptcl = Service.forProtocol({
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
            assert.equal(err.rpcCode, 'INVALID_REQUEST');
            ee.destroy();
          });
        });
      });

      test('error response', function (done) {
        var ptcl = Service.forProtocol({
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
        var ptcl = Service.forProtocol({
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

      test('wrapped remote service', function (done) {
        var ptcl1 = Service.forProtocol({
          protocol: 'Math',
          messages: {
            invert: {
              request: [{name: 'n', type: ['int', 'float']}],
              response: ['int', 'float']
            }
          }
        }, {wrapUnions: true});
        var ptcl2 = Service.forProtocol({
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
        var ptcl = Service.forProtocol({
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
          } else if (n === 0) {
            cb(new Error('zero!')); // Ok error response.
          } else {
            cb(null, Math.sqrt(n));
          }
        });
        setupFn(ptcl, ptcl, function (ee) {
          ptcl.emit('sqrt', {n: - 10}, ee, function (err) {
            assert.equal(err.message, 'INTERNAL_SERVER_ERROR');
            ptcl.emit('sqrt', {n: 0}, ee, function (err) {
              assert(/zero!/.test(err.message));
              ptcl.emit('sqrt', {n: 100}, ee, function (err, res) {
                // And the server doesn't die (we can make a new request).
                assert(Math.abs(res - 10) < 1e-5);
                done();
              });
            });
          });
        });
      });

      test('out of order', function (done) {
        var ptcl = Service.forProtocol({
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
            assert.deepEqual(ids, [undefined, 'b', 'a']);
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

      test('compatible services', function (done) {
        var emitterPtcl = Service.forProtocol({
          protocol: 'emitterService',
          messages: {
            age: {
              request: [{name: 'name', type: 'string'}],
              response: 'long'
            }
          }
        });
        var listenerPtcl = Service.forProtocol({
          protocol: 'serverService',
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

      test('compatible service with a complex type', function (done) {
        var ptcl1 = Service.forProtocol({
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
        var ptcl2 = Service.forProtocol({
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

      test('cached compatible services', function (done) {
        var ptcl1 = Service.forProtocol({
          protocol: 'emitterService',
          messages: {
            age: {
              request: [{name: 'name', type: 'string'}],
              response: 'long'
            }
          }
        });
        var ptcl2 = Service.forProtocol({
          protocol: 'serverService',
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
                function (ee2) { // ee2 has the server's service.
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

      test('incompatible services missing message', function (done) {
        var emitterPtcl = Service.forProtocol({
          protocol: 'emitterService',
          messages: {
            age: {request: [{name: 'name', type: 'string'}], response: 'long'}
          }
        }, {wrapUnions: true});
        var listenerPtcl = Service.forProtocol({protocol: 'serverService'});
        setupFn(
          emitterPtcl,
          listenerPtcl,
          function (ee) {
            ee.on('error', function () {}); // For stateful services.
            emitterPtcl.emit('age', {name: 'Ann'}, ee, function (err) {
              assert(err.message);
              done();
            });
          }
        );
      });

      test('incompatible services', function (done) {
        var emitterPtcl = Service.forProtocol({
          protocol: 'emitterService',
          messages: {
            age: {request: [{name: 'name', type: 'string'}], response: 'long'}
          }
        }, {wrapUnions: true});
        var listenerPtcl = Service.forProtocol({
          protocol: 'serverService',
          messages: {
            age: {request: [{name: 'name', type: 'int'}], response: 'long'}
          }
        }).on('age', function (req, ee, cb) { cb(null, 0); });
        setupFn(
          emitterPtcl,
          listenerPtcl,
          function (ee) {
            ee.on('error', function () {}); // For stateful services.
            emitterPtcl.emit('age', {name: 'Ann'}, ee, function (err) {
              assert(err.message);
              done();
            });
          }
        );
      });

      test('incompatible services one way message', function (done) {
        var ptcl1 = Service.forProtocol({
          protocol: 'ptcl1',
          messages: {ping: {request: [], response: 'null', 'one-way': true}}
        });
        var ptcl2 = Service.forProtocol({
          protocol: 'ptcl2',
          messages: {ping: {request: [], response: 'null'}}
        });
        setupFn(ptcl1, ptcl2, function (ee) {
            ee.on('error', function (err) {
              // This will be called twice for stateful emitters: once when
              // interrupted, then for the incompatible service error.
              assert(err);
              this.destroy();
            }).on('eot', function () { done(); });
            ptcl1.emit('ping', {}, ee);
          }
        );
      });

      test('one way message', function (done) {
        var ptcl = Service.forProtocol({
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
        var ptcl = Service.forProtocol({
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
        var ptcl = Service.forProtocol({
          protocol: 'ptcl',
          messages: {ping: {request: [], response: 'null'}} // Not one-way.
        });
        setupFn(ptcl, ptcl, function (ee) {
          ptcl.on('ping', function (req, ee, cb) {
            ee.on('error', function (err) {
              assert(/duplicate/.test(err));
              done();
            });
            cb(null, null);
            cb(null, null);
          });
          ptcl.emit('ping', {}, ee);
        });
      });

      test('unknown message', function (done) {
        var ptcl = Service.forProtocol({protocol: 'Empty'});
        setupFn(ptcl, ptcl, function (ee) {
          assert.throws(
            function () { ptcl.emit('echo', {}, ee); },
            /unknown message/
          );
          done();
        });
      });

      test('unhandled message', function (done) {
        var ptcl = Service.forProtocol({
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
            assert(/NOT_IMPLEMENTED/.test(err));
            done();
          });
        });
      });

      test('timeout', function (done) {
        var ptcl = Service.forProtocol({
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
        var ptcl = Service.forProtocol({
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
        var ptcl = Service.forProtocol({
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
        var ptcl = Service.forProtocol({
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
        var ptcl = Service.forProtocol({
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
              assert.equal(err.message, 'INTERNAL_SERVER_ERROR');
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

  suite('Client', function () {

    test('no emitters', function (done) {
      var ptcl = Service.forProtocol({
        protocol: 'Ping',
        messages: {ping: {request: [], response: 'boolean'}}
      });
      var client = ptcl.createClient()
        .on('error', function (err) {
          assert(/no emitters available/.test(err));
          done();
        });
      // With callback.
      client.ping(function (err) {
        assert(/no emitters available/.test(err));
        // Without
        client.ping();
      });
    });

    test('destroy emitters', function (done) {
      var ptcl = Service.forProtocol({
        protocol: 'Ping',
        messages: {ping: {request: [], response: 'boolean'}}
      });
      var transport = {
        readable: new stream.PassThrough(),
        writable: new stream.PassThrough()
      };
      var client = ptcl.createClient();
      client.createEmitter(transport)
        .on('eot', function () {
          done();
        });
      client.destroyEmitters({noWait: true});
    });

    test('default policy', function (done) {
      var ptcl = Service.forProtocol({
        protocol: 'Ping',
        messages: {ping: {request: [], response: 'null', 'one-way': true}}
      });
      var transport = {
        readable: new stream.PassThrough(),
        writable: new stream.PassThrough()
      };
      var client = ptcl.createClient();
      client.createEmitter(transport, {noPing: true});
      client.createEmitter(transport, {noPing: true});
      client.ping(function (err) {
        assert(!err);
        done();
      });
    });

    test('custom policy', function (done) {
      var ptcl = Service.forProtocol({
        protocol: 'Ping',
        messages: {ping: {request: [], response: 'boolean'}}
      });
      var transport = {
        readable: new stream.PassThrough(),
        writable: new stream.PassThrough()
      };

      var client = ptcl.createClient({emitterPolicy: policy});
      var emitters = [
        client.createEmitter(transport),
        client.createEmitter(transport)
      ];
      client.ping();

      function policy(actualEmitters) {
        assert.deepEqual(actualEmitters, emitters);
        done();
      }
    });

    test('remote protocols existing', function () {
      var ptcl1 = Service.forProtocol({protocol: 'Empty1'});
      var ptcl2 = Service.forProtocol({protocol: 'Empty2'});
      var remotePtcls = {abc: ptcl2.getSchema()};
      var client = ptcl1.createClient({
        remoteFingerprint: 'abc',
        remoteProtocols: remotePtcls
      });
      assert.deepEqual(client.getRemoteProtocols(), remotePtcls);
    });

  });

  suite('Server', function () {

    test('get listeners', function (done) {
      var ptcl = Service.forProtocol({protocol: 'Empty1'});
      var server = ptcl.createServer();
      var transport = {
        readable: new stream.PassThrough(),
        writable: new stream.PassThrough()
      };
      var listeners = [
        server.createListener(transport),
        server.createListener(transport)
      ];
      assert.deepEqual(server.getListeners(), listeners);
      listeners[0]
        .on('eot', function () {
          assert.deepEqual(server.getListeners(), [listeners[1]]);
          done();
        })
        .destroy();
    });

    test('remote protocols', function () {
      var ptcl1 = Service.forProtocol({protocol: 'Empty1'});
      var ptcl2 = Service.forProtocol({protocol: 'Empty2'});
      var remotePtcls = {abc: ptcl2.getSchema()};
      var server = ptcl1.createServer({remoteProtocols: remotePtcls});
      assert.deepEqual(server.getRemoteProtocols(), remotePtcls);
    });

    test('no capitalization', function () {
      var ptcl = Service.forProtocol({
        protocol: 'Ping',
        messages: {ping: {request: [], response: 'boolean'}}
      });
      var server = ptcl.createServer({noCapitalize: true});
      assert(!server.onPing);
      assert(typeof server.onping == 'function');
    });

  });

  suite('clients & servers', function () { // >=5.0 API.

    test('create client with server', function (done) {
      var svc = Service.forProtocol({
        protocol: 'Echo',
        messages: {
          echo: {request: [{name: 'n', type: 'int'}], response: 'int'}
        }
      });
      var server = svc.createServer()
        .onEcho(function (n, cb) { cb(null, n); });
      var client = svc.createClient({server: server});
      client.echo(123, function (err, n) {
        assert(!err, err);
        assert.equal(n, 123);
        done();
      });
    });

    test('client call context factory', function (done) {
      var svc = Service.forProtocol({
        protocol: 'Math',
        messages: {
          neg: {request: [{name: 'n', type: 'int'}], response: 'int'}
        }
      });
      var server = svc.createServer()
        .onNeg(function (n, cb) { cb(null, -n); });
      var transports = createPassthroughTransports();
      var opts = {id: 0};
      var client = svc.createClient({
        context: context,
        transport: transports[0]
      }).use(function (wreq, wres, next) {
          // Check that middleware have the right context.
          assert.equal(this.foo,'foo');
          next(null, function (err, prev) {
            assert.equal(this.foo,'foo');
            prev(err);
          });
        });
      var emitter = client.getEmitters()[0];
      server.createListener(transports[1]);
      client.neg(1, opts, function (err, n) {
        assert(!err, err);
        assert.equal(n, -1);
        assert.equal(this.num, 0);
        client.neg('abc', opts, function (err) {
          assert(/invalid "int"/.test(err), err);
          assert.equal(this.num, 1);
          done();
        });
      });

      function context(emitter_, ctxOpts, callOpts) {
        assert.strictEqual(emitter_, emitter);
        assert.strictEqual(callOpts, opts);
        return {foo: 'foo', num: opts.id++};
      }
    });

    test('server call constant context', function (done) {
      var svc = Service.forProtocol({
        protocol: 'Math',
        messages: {
          neg: {request: [{name: 'n', type: 'int'}], response: 'int'}
        }
      });
      var ctx = {numCalls: 0};
      var server = svc.createServer({context: ctx})
        .use(function (wreq, wres, next) {
          // Check that middleware have the right context.
          assert.strictEqual(this, ctx);
          assert.equal(ctx.numCalls++, 0);
          next(null, function (err, prev) {
            assert(!err, err);
            assert.strictEqual(this, ctx);
            assert.equal(ctx.numCalls++, 2);
            prev(err);
          });
        })
        .onNeg(function (n, cb) {
          assert.strictEqual(this, ctx);
          assert.equal(ctx.numCalls++, 1);
          cb(null, -n);
        });
      var transports = createPassthroughTransports();
      var client = svc.createClient({transport: transports[0]});
      server.createListener(transports[1]);
      client.neg(1, function (err, n) {
        assert(!err, err);
        assert.equal(n, -1);
        assert.equal(ctx.numCalls, 3);
        done();
      });
    });

    test('server call context options', function (done) {
      var svc = Service.forProtocol({
        protocol: 'Math',
        messages: {
          neg: {request: [{name: 'n', type: 'int'}], response: 'int'}
        }
      });
      var ctxOpts = 123;
      var server = svc.createServer({context: context})
        .use(function (wreq, wres, next) {
          assert.strictEqual(this.id, 123);
          next();
        })
        .onNeg(function (n, cb) {
          assert.strictEqual(this.id, 123);
          cb(null, -n);
        });
      var transports = createPassthroughTransports();
      var client = svc.createClient({transport: transports[0]});
      server.createListener(transports[1], {contextOptions: ctxOpts});
      client.neg(1, function (err, n) {
        assert(!err, err);
        assert.equal(n, -1);
        done();
      });

      function context(listener, opts) {
        assert.strictEqual(opts, ctxOpts);
        return {id: ctxOpts};
      }
    });

    test('server default handler', function (done) {
      var svc = Service.forProtocol({
        protocol: 'Math',
        messages: {
          neg: {request: [{name: 'n', type: 'int'}], response: 'int'},
          abs: {request: [{name: 'n', type: 'int'}], response: 'int'}
        }
      });
      var server = svc.createServer({defaultHandler: defaultHandler})
        .onNeg(function (n, cb) { cb(null, -n); });
      var client = svc.createClient({server: server});

      client.neg(1, function (err, n) {
        assert(!err, err);
        assert.equal(n, -1);
        client.abs(5, function (err, n) {
          assert(!err, err);
          assert.equal(n, 10);
          done();
        });
      });

      function defaultHandler(wreq, wres, cb) {
        assert.equal(wreq.getMessage().getName(), 'abs');
        wres.setResponse(10);
        cb();
      }
    });

    suite('stateful', function () {

      run(function (clientPtcl, serverPtcl, opts, cb) {
        if (!cb) {
          cb = opts;
          opts = undefined;
        }
        opts = opts || {};
        opts.silent = true;
        var pt1 = new stream.PassThrough();
        var pt2 = new stream.PassThrough();
        var client = clientPtcl.createClient(opts);
        client.createEmitter({readable: pt1, writable: pt2});
        var server = serverPtcl.createServer(opts);
        server.createListener({readable: pt2, writable: pt1}, opts);
        cb(client, server);
      });

    });

    suite('stateless', function () {

      run(function (clientPtcl, serverPtcl, opts, cb) {
        if (!cb) {
          cb = opts;
          opts = undefined;
        }
        opts = opts || {};
        opts.silent = true;
        var client = clientPtcl.createClient(opts);
        client.createEmitter(writableFactory);
        var server = serverPtcl.createServer(opts);
        cb(client, server);

        function writableFactory(emitterCb) {
          var reqPt = new stream.PassThrough()
            .on('finish', function () {
              server.createListener(function (listenerCb) {
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
        var ptcl = Service.forProtocol({
          protocol: 'Math',
          messages: {
            negate: {
              request: [{name: 'n', type: 'int'}],
              response: 'long'
            }
          }
        });
        setupFn(ptcl, ptcl, function (client, server) {
          server
            .onNegate(function (n, cb) {
              assert.strictEqual(this.getServer(), server);
              cb(null, -n);
            });
          var emitter = client.getEmitters()[0];
          emitter.on('eot', function () {
              done();
            })
            .once('handshake', function (hreq, hres) {
              // Allow the initial ping to complete.
              assert.equal(hres.match, 'BOTH');
              process.nextTick(function () {
                client.negate(20, function (err, res) {
                  assert.equal(this, emitter);
                  assert.strictEqual(err, null);
                  assert.equal(res, -20);
                  client.negate('ni',  function (err) {
                    assert(/invalid "int"/.test(err.message));
                    assert.equal(err.rpcCode, 'INVALID_REQUEST');
                    this.destroy();
                  });
                });
              });
            });
        });
      });

      test('invalid strict error', function (done) {
        var ptcl = Service.forProtocol({
          protocol: 'Math',
          messages: {
            sqrt: {
              request: [{name: 'n', type: 'float'}],
              response: 'float'
            }
          }
        });
        setupFn(ptcl, ptcl, {strictErrors: true}, function (client, server) {
          server.onSqrt(function (n, cb) {
            if (n === -1) {
              cb(new Error('no i')); // Invalid error (should be a string).
            } else if (n < 0) {
              throw new Error('negative');
            } else {
              cb(undefined, Math.sqrt(n));
            }
          });
          client.sqrt(-1, function (err) {
            assert.equal(err, 'INTERNAL_SERVER_ERROR');
            client.sqrt(-2, function (err) {
              assert.equal(err, 'INTERNAL_SERVER_ERROR');
              client.sqrt(100, function (err, res) {
                // The server still doesn't die (we can make a new request).
                assert.strictEqual(err, undefined);
                assert(Math.abs(res - 10) < 1e-5);
                done();
              });
            });
          });
        });
      });

      test('client middleware', function (done) {
        var ptcl = Service.forProtocol({
          protocol: 'Math',
          messages: {
            neg: {request: [{name: 'n', type: 'int'}], response: 'int'}
          }
        });
        setupFn(ptcl, ptcl, function (client, server) {
          server.onNeg(function (n, cb) { cb(null, -n); });
          var buf = new Buffer([0, 1]);
          var isDone = false;
          var emitter = client.getEmitters()[0];
          client
            .use(function (wreq, wres, next) {
              // No callback.
              assert.strictEqual(this, emitter);
              assert.deepEqual(wreq.getHeader(), {});
              wreq.getHeader().buf = buf;
              assert.deepEqual(wreq.getRequest(), {n: 2});
              next();
            })
            .use(function (wreq, wres, next) {
              // Callback here.
              assert.deepEqual(wreq.getHeader(), {buf: buf});
              wreq.getRequest().n = 3;
              next(null, function (err, prev) {
                assert(!err);
                assert.strictEqual(this, emitter);
                assert.deepEqual(wres.getResponse(), -3);
                isDone = true;
                prev();
              });
            })
            .neg(2, function (err, res) {
              assert.strictEqual(err, null);
              assert.equal(res, -3);
              assert(isDone);
              done();
            });
        });
      });

      test('client middleware forward error', function (done) {
        var ptcl = Service.forProtocol({
          protocol: 'Math',
          messages: {
            neg: {request: [{name: 'n', type: 'int'}], response: 'int'}
          }
        });
        setupFn(ptcl, ptcl, function (client, server) {
          server.onNeg(function (n, cb) { cb(null, -n); });
          var fwdErr = new Error('forward!');
          var bwdErr = new Error('backward!');
          var called = false;
          client
            .use(function (wreq, wres, next) {
              next(null, function (err, prev) {
                assert.strictEqual(err, fwdErr);
                assert(!called);
                prev(bwdErr); // Substitute the error.
              });
            })
            .use(function (wreq, wres, next) {
              next(fwdErr, function (err, prev) {
                called = true;
                prev();
              });
            })
            .neg(2, function (err) {
              assert.strictEqual(err, bwdErr);
              done();
            });
        });
      });

      test('client middleware duplicate forward calls', function (done) {
        var ptcl = Service.forProtocol({
          protocol: 'Math',
          messages: {
            neg: {request: [{name: 'n', type: 'int'}], response: 'int'}
          }
        });
        setupFn(ptcl, ptcl, function (client, server) {
          server.onNeg(function (n, cb) { cb(null, -n); });
          client.getEmitters()[0]
            .on('error', function (err) {
              assert(/duplicate middleware forward/.test(err.message));
              setTimeout(function () { done(); }, 0);
            });
          client
            .use(function (wreq, wres, next) {
              next();
              next();
            })
            .neg(2, function (err, res) {
              assert.equal(res, -2);
            });
        });
      });

      test('server middleware', function (done) {
        var ptcl = Service.forProtocol({
          protocol: 'Math',
          messages: {
            neg: {request: [{name: 'n', type: 'int'}], response: 'int'}
          }
        });
        setupFn(ptcl, ptcl, function (client, server) {
          var isDone = false;
          var buf = new Buffer([0, 1]);
          var listener; // Listener isn't ready yet.
          server
            .use(function (wreq, wres, next) {
              assert.strictEqual(this.getServer(), server);
              listener = this;
              assert.deepEqual(wreq.getRequest(), {n: 2});
              next(null, function (err, prev) {
                assert.strictEqual(this, listener);
                wres.getHeader().buf = buf;
                prev();
              });
            })
            .onNeg(function (n, cb) { cb(null, -n); });
          client
            .use(function (wreq, wres, next) {
              next(null, function (err, prev) {
                assert.deepEqual(wres.getHeader(), {buf: buf});
                isDone = true;
                prev();
              });
            })
            .neg(2, function (err, res) {
              assert.strictEqual(err, null);
              assert.equal(res, -2);
              assert(isDone);
              done();
            });
        });
      });

      test('server middleware duplicate backward calls', function (done) {
        var ptcl = Service.forProtocol({
          protocol: 'Math',
          messages: {
            neg: {request: [{name: 'n', type: 'int'}], response: 'int'}
          }
        });
        setupFn(ptcl, ptcl, function (client, server) {
          server
            .use(function (wreq, wres, next) {
              // Attach error handler to listener.
              this.on('error', function (err) {
                assert(/duplicate/.test(err));
                setTimeout(function () { done(); }, 0);
              });
              next(null, function (err, prev) {
                prev();
                prev();
              });
            })
            .onNeg(function (n, cb) { cb(null, -n); });
          client
            .neg(2, function (err, res) {
              assert.strictEqual(err, null);
              assert.equal(res, -2);
            });
        });
      });

      test('server middleware invalid response header', function (done) {
        var ptcl = Service.forProtocol({
          protocol: 'Math',
          messages: {
            neg: {request: [{name: 'n', type: 'int'}], response: 'int'}
          }
        });
        setupFn(ptcl, ptcl, function (client, server) {
          var fooErr = new Error('foo');
          var sawFoo = 0;
          server
            .on('error', function (err) {
              if (err === fooErr) {
                sawFoo++;
                return;
              }
              assert.equal(sawFoo, 1);
              assert(/invalid "bytes"/.test(err.message));
              setTimeout(function () { done(); }, 0);
            })
            .use(function (wreq, wres, next) {
              wres.getHeader().id = 123;
              next();
            })
            .onNeg(function () { throw fooErr; });
          client
            .neg(2, function (err) {
              assert(/INTERNAL_SERVER_ERROR/.test(err.message));
            });
        });
      });

      test('error formatter', function (done) {
        var ptcl = Service.forProtocol({
          protocol: 'Math',
          messages: {
            neg: {request: [{name: 'n', type: 'int'}], response: 'int'}
          }
        });
        var opts = {systemErrorFormatter: formatter};
        var numErrs = 0;
        var numFormats = 0;
        var barErr = new Error('baribababa');
        setupFn(ptcl, ptcl, opts, function (client, server) {
          server
            .onNeg(function () { throw barErr; })
            .on('error', function (err) {
              process.nextTick(function () {
                // Allow other assertion to complete.
                assert.strictEqual(err, barErr);
                if (++numErrs === 2) {
                  done();
                }
              });
            });
          client
            .neg(2, function (err) {
              assert(/FOO/.test(err));
              client.neg(1, function (err) {
                assert.equal(err.message, 'INTERNAL_SERVER_ERROR');
              });
            });
        });

        function formatter(err) {
          if (numFormats++) {
            throw new Error('format!');
          }
          assert.strictEqual(err, barErr);
          return 'FOO';
        }
      });

      test('remote protocols', function (done) {
        var clientPtcl = {
          protocol: 'Math1',
          foo: 'bar', // Custom attribute.
          doc: 'hi',
          messages: {
            neg: {request: [{name: 'n', type: 'int'}], response: 'long'}
          }
        };
        var serverPtcl = {
          protocol: 'Math2',
          doc: 'hey',
          bar: 'foo', // Custom attribute.
          messages: {
            neg: {request: [{name: 'n', type: 'long'}], response: 'int'}
          }
        };
        var clientSvc = Service.forProtocol(clientPtcl);
        var serverSvc = Service.forProtocol(serverPtcl);
        setupFn(clientSvc, serverSvc, function (client, server) {
          server
            .onNeg(function (n, cb) { cb(null, -n); });
          client
            .neg(2, function (err, res) {
              assert.equal(res, -2);
              var remotePtcl;
              // Client.
              remotePtcl = {};
              remotePtcl[serverSvc.getHash()] = serverPtcl;
              assert.deepEqual(client.getRemoteProtocols(), remotePtcl);
              // Server.
              remotePtcl = {};
              remotePtcl[clientSvc.getHash()] = clientPtcl;
              assert.deepEqual(server.getRemoteProtocols(), remotePtcl);
              done();
            });
        });
      });

      test('client timeout', function (done) {
        var ptcl = Service.forProtocol({
          protocol: 'Sleep',
          messages: {
            sleep: {request: [{name: 'ms', type: 'int'}], response: 'int'}
          }
        });
        setupFn(ptcl, ptcl, {defaultTimeout: 50}, function (client, server) {
          server
            .onSleep(function (n, cb) {
              // Delay response by the number requested.
              setTimeout(function () { cb(null, n); }, n);
            });
          client.sleep(10, function (err, res) {
            // Default timeout used here, but delay is short enough.
            assert.strictEqual(err, null);
            assert.equal(res, 10);
            client.sleep(100, function (err) {
              // Default timeout used here, but delay is _not_ short enough.
              assert(/timeout/.test(err));
              client.sleep(100, {timeout: 200}, function (err, res) {
                // Custom timeout, high enough for the delay.
                assert.strictEqual(err, null);
                assert.equal(res, 100);
                done();
              });
            });
          });
        });
      });

      test('server error after handler', function (done) {
        var svc = Service.forProtocol({
          protocol: 'Math',
          messages: {
            neg: {request: [{name: 'n', type: 'int'}], response: 'int'}
          }
        });
        setupFn(svc, svc, function (client, server) {
          var numErrors = 0;
          server.onNeg(function (n, cb) {
            this.on('error', function (err) {
              numErrors++;
              assert(/bar/.test(err), err);
            });
            cb(null, -n);
            throw new Error('bar');
          });
          client.neg(2, function (err, n) {
            assert(!err, err);
            assert.equal(n, -2);
            setTimeout(function () {
              assert.equal(numErrors, 1);
              done();
            }, 0);
          });
        });
      });

    }

  });

  suite('discover attributes', function () {

    var discoverProtocol = services.discoverProtocol;

    test('stateful ok', function (done) {
      var schema = {
        protocol: 'Case',
        messages: {
          upper: {
            request: [{name: 'str', type: 'string'}],
            response: 'string'
          }
        }
      };
      var ptcl = Service.forProtocol(schema);
      var server = ptcl.createServer()
        .onUpper(function (str, cb) {
          cb(null, str.toUpperCase());
        });
      var transports = createPassthroughTransports();
      server.createListener(transports[1]);
      discoverProtocol(transports[0], function (err, actualAttrs) {
        assert.strictEqual(err, null);
        assert.deepEqual(actualAttrs, schema);
        // Check that the transport is still usable.
        var client = ptcl.createClient();
        client.createEmitter(transports[0])
          .on('eot', function() {
            done();
          });
        client.upper('foo', function (err, res) {
          assert.strictEqual(err, null);
          assert.equal(res, 'FOO');
          this.destroy();
        });
      });
    });

    test('stateless ok', function (done) {
      // Using old API.
      var schema = {
        protocol: 'Case',
        messages: {
          upper: {
            request: [{name: 'str', type: 'string'}],
            response: 'string'
          }
        }
      };
      var ptcl = Service.forProtocol(schema)
        .on('upper', function (req, ee, cb) {
          cb(null, req.str.toUpperCase());
        });
      discoverProtocol(writableFactory, function (err, actual) {
        assert.strictEqual(err, null);
        assert.deepEqual(actual, schema);
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
      var schema = {
        protocol: 'Case',
        messages: {
          upper: {
            request: [{name: 'str', type: 'string'}],
            response: 'string'
          }
        }
      };
      var ptcl = Service.forProtocol(schema)
        .on('upper', function (req, ee, cb) {
          cb(null, req.str.toUpperCase());
        });
      var scope = 'bar';
      var transports = createPassthroughTransports();
      ptcl.createListener(transports[1], {scope: scope});
      discoverProtocol(transports[0], {timeout: 5}, function (err) {
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
