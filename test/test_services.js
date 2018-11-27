/* jshint node: true, mocha: true */

'use strict';

var types = require('../lib/types'),
    services = require('../lib/services'),
    utils = require('../lib/utils'),
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
      assert.equal(s.name, 'foo.HelloWorld');
      assert.equal(s.type('foo.Greeting').getTypeName(), 'record');
      assert.equal(s.type('string').getTypeName(), 'string');
      assert.equal(s.types.length, 4);
      assert.deepEqual(s.protocol, p);
    });

    test('missing message', function () {
      var svc = Service.forProtocol({
        namespace: 'com.acme',
        protocol: 'Hello'
      });
      assert.throws(function () {
        svc.on('add', function () {});
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
      var s = Service.forProtocol({
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
      assert.equal(s.type('ping.Ping').getTypeName(), 'record');
      assert.equal(s.type('ping.Pong').getTypeName(), 'record');
      assert.equal(s.type('ping.Pung').getTypeName(), 'record');
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
      var svc;
      svc = Service.forProtocol({protocol: 'Empty'});
      assert.deepEqual(svc.messages, {});
      svc = Service.forProtocol({
        protocol: 'Ping',
        messages: {
          ping: {
            request: [],
            response: 'string'
          }
        }
      });
      var messages = svc.messages;
      assert.deepEqual(messages, [svc.message('ping')]);
    });

    test('subprotocol', function () {
      var svc = Service.forProtocol({
        namespace: 'com.acme',
        protocol: 'Hello',
        types: [{name: 'Id', type: 'fixed', size: 2}],
        messages: {ping: {request: [], response: 'null'}}
      });
      var subptcl = svc.subprotocol();
      assert(subptcl.getFingerprint().equals(svc.getFingerprint()));
      assert.strictEqual(subptcl._emitterResolvers, svc._emitterResolvers);
      assert.strictEqual(subptcl._listenerResolvers, svc._listenerResolvers);
    });

    test('invalid emitter', function (done) {
      var svc1 = Service.forProtocol({protocol: 'Hey'});
      var svc2 = Service.forProtocol({protocol: 'Hi'});
      var ee = svc2.createEmitter(new stream.PassThrough(), {noPing: true});
      assert.throws(
        function () { svc1.emit('hi', {}, ee); },
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
      var svc = Service.forProtocol(schema);
      assert.deepEqual(svc.getSchema({exportAttrs: true}), schema);
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
      var svc = Service.forProtocol(schema);
      var t = svc.type('Foo');
      // Bar's reference shouldn't be included in the returned types array.
      assert.deepEqual(svc.getSchema().types, [t.getSchema()]);
    });

    test('get documentation', function () {
      var svc = Service.forProtocol({protocol: 'Hello', doc: 'Hey'});
      assert.equal(svc.doc, 'Hey');
    });

    test('getFingerprint', function () {
      var svc = Service.forProtocol({
        namespace: 'hello',
        protocol: 'World',
      });
      assert.deepEqual(svc.getFingerprint('md5'), svc.getFingerprint());
    });

    test('isService', function () {
      var svc = Service.forProtocol({
        namespace: 'hello',
        protocol: 'World',
      });
      assert(Service.isService(svc));
      assert(!Service.isService(undefined));
      assert(!Service.isService({protocol: 'bar'}));
    });

    test('equals', function () {
      var svc = Service.forProtocol({
        namespace: 'hello',
        protocol: 'World',
      });
      assert(svc.equals(svc));
      assert(!svc.equals(undefined));
      assert(!svc.equals(Service.forProtocol({protocol: 'Foo'})));
    });

    test('inspect', function () {
      var svc = Service.forProtocol({
        namespace: 'hello',
        protocol: 'World',
      });
      assert.equal(svc.inspect(), '<Service "hello.World">');
    });

    test('using constructor', function () {
      var svc = new services.Service({protocol: 'Empty'});
      assert.equal(svc.name, 'Empty');
      assert.deepEqual(svc.messages, []);
    });

    test('namespacing', function () {
      var svc;

      svc = newService('foo.Foo', '');
      assert.equal(svc.name, 'foo.Foo');
      assert(svc.type('Bar'));
      assert(svc.type('Baz'));

      svc = newService('foo.Foo');
      assert.equal(svc.name, 'foo.Foo');
      assert(svc.type('foo.Bar'));
      assert(svc.type('Baz'));

      svc = newService('Foo', 'bar');
      assert.equal(svc.name, 'bar.Foo');
      assert(svc.type('bar.Bar'));
      assert(svc.type('Baz'));

      svc = newService('Foo', 'bar', {namespace: 'opt'});
      assert.equal(svc.name, 'bar.Foo');
      assert(svc.type('bar.Bar'));
      assert(svc.type('Baz'));

      svc = newService('Foo', undefined, {namespace: 'opt'});
      assert.equal(svc.name, 'opt.Foo');
      assert(svc.type('opt.Bar'));
      assert(svc.type('Baz'));

      svc = newService('.Foo', undefined, {namespace: 'opt'});
      assert.equal(svc.name, 'Foo');
      assert(svc.type('Bar'));
      assert(svc.type('Baz'));

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

    test('createClient transport option', function (done) {
      var svc = Service.forProtocol({protocol: 'Empty'});
      svc.createClient({transport: new stream.PassThrough()})
        .on('channel', function () { done(); });
    });

    test('createListener strict', function () {
      var svc = Service.forProtocol({protocol: 'Empty'});
      assert.throws(function () {
        svc.createListener(new stream.PassThrough(), {strictErrors: true});
      });
    });

    test('compatible', function () {
      var emptySvc = Service.forProtocol({protocol: 'Empty'});
      var pingSvc = Service.forProtocol({
        protocol: 'Ping',
        messages: {
          ping: {request: [], response: 'boolean'},
          pong: {request: [], response: 'int'}
        }
      });
      var pongSvc = Service.forProtocol({
        protocol: 'Pong',
        messages: {
          pong: {request: [], response: 'long'}
        }
      });
      assert(Service.compatible(emptySvc, pingSvc));
      assert(!Service.compatible(pingSvc, emptySvc));
      assert(!Service.compatible(pingSvc, pongSvc));
      assert(Service.compatible(pongSvc, pingSvc));
    });
  });

  suite('Message', function () {

    var Message = services.Message;

    test('empty errors', function () {
      var m = Message.forSchema('Hi', {
        request: [{name: 'greeting', type: 'string'}],
        response: 'int'
      });
      assert.deepEqual(m.errorType.toString(), '["string"]');
    });

    test('non-array request', function () {
      assert.throws(function () {
        Message.forSchema('Hi', {
          request: 'string',
          response: 'int'
        });
      }, /invalid \w* request/);
    });

    test('missing response', function () {
      assert.throws(function () {
        Message.forSchema('Hi', {
          request: [{name: 'greeting', type: 'string'}]
        });
      }, /invalid \w* response/);
    });

    test('non-array errors', function () {
      assert.throws(function () {
        Message.forSchema('Hi', {
          request: [{name: 'greeting', type: 'string'}],
          response: 'int',
          errors: 'int'
        });
      }, /invalid \w* error/);
    });

    test('invalid one-way', function () {
      // Non-null response.
      assert.throws(function () {
        Message.forSchema('Hi', {
          request: [{name: 'greeting', type: 'string'}],
          response: 'string',
          'one-way': true
        });
      }, /inapplicable/);
      // Non-empty errors.
      assert.throws(function () {
        Message.forSchema('Hi', {
          request: [{name: 'greeting', type: 'string'}],
          response: 'null',
          errors: ['int'],
          'one-way': true
        });
      }, /inapplicable/);
    });

    test('getters', function () {
      var s = {
        request: [{name: 'ping', type: 'string'}],
        response: 'null'
      };
      var m = Message.forSchema('Ping', s);
      assert.equal(m.name, 'Ping');
      assert.equal(m.requestType.getFields()[0].getName(), 'ping');
      assert.equal(m.responseType.getName(true), 'null');
      assert.strictEqual(m.oneWay, false);
      assert.strictEqual(m.isOneWay(), false);
      assert.deepEqual(m.schema(), s);
    });

    test('get documentation', function () {
      var schema = {
        request: [{name: 'ping', type: 'string'}],
        response: 'null',
        doc: 'Pong'
      };
      var m = Message.forSchema('Ping', schema);
      assert.equal(m.doc, 'Pong');
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

    test('schema multiple errors', function () {
      var s = {
        request: [{name: 'ping', type: 'string'}],
        response: 'null',
        errors: ['int', 'bytes']
      };
      var m = Message.forSchema('Ping', s);
      assert.deepEqual(m.schema(), s);
    });
  });

  suite('FrameDecoder & FrameEncoder', function () {

    var FrameDecoder = services.streams.FrameDecoder;
    var FrameEncoder = services.streams.FrameEncoder;

    test('decode', function (done) {
      var frames = [
        utils.bufferFrom([0, 1]),
        utils.bufferFrom([2]),
        utils.bufferFrom([]),
        utils.bufferFrom([3, 4]),
        utils.bufferFrom([])
      ].map(frame);
      var messages = [];
      createReadableStream(frames)
        .pipe(new FrameDecoder())
        .on('finish', function () {
          assert.deepEqual(
            messages,
            [
              {id: null, payload: [utils.bufferFrom([0, 1]), utils.bufferFrom([2])]},
              {id: null, payload: [utils.bufferFrom([3, 4])]}
            ]
          );
          done();
        })
        .pipe(createWritableStream(messages));
    });

    test('decode with trailing data', function (done) {
      var frames = [
        utils.bufferFrom([0, 1]),
        utils.bufferFrom([2]),
        utils.bufferFrom([]),
        utils.bufferFrom([3])
      ].map(frame);
      var messages = [];
      createReadableStream(frames)
        .pipe(new FrameDecoder())
        .on('error', function () {
          assert.deepEqual(
            messages,
            [{id: null, payload: [utils.bufferFrom([0, 1]), utils.bufferFrom([2])]}]
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
        {id: 1, payload: [utils.bufferFrom([1, 3, 5]), utils.bufferFrom([6, 8])]},
        {id: 4, payload: [utils.bufferFrom([123, 23])]}
      ];
      var frames = [];
      createReadableStream(messages)
        .pipe(new FrameEncoder())
        .pipe(createWritableStream(frames))
        .on('finish', function () {
          assert.deepEqual(
            frames,
            [
              utils.bufferFrom([0, 0, 0, 3]),
              utils.bufferFrom([1, 3, 5]),
              utils.bufferFrom([0, 0, 0, 2]),
              utils.bufferFrom([6, 8]),
              utils.bufferFrom([0, 0, 0, 0]),
              utils.bufferFrom([0, 0, 0, 2]),
              utils.bufferFrom([123, 23]),
              utils.bufferFrom([0, 0, 0, 0])
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
        utils.bufferFrom([0, 0, 0, 2, 0, 0, 0]),
        utils.bufferFrom([1, 0, 0, 0, 5, 1, 3, 4, 2, 5, 1])
      ];
      var dst = [];
      createReadableStream(src)
        .pipe(new NettyDecoder())
        .on('error', function () {
          assert.deepEqual(
            dst,
            [{id: 2, payload: [utils.bufferFrom([1, 3, 4, 2, 5])]}]
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

  suite('Adapter', function () {

    var Adapter = services.Adapter;

    test('truncated request & response', function () {
      var s = Service.forProtocol({
        protocol: 'Echo',
        messages: {
          echo: {request: [{name: 's', type: 'string'}], response: 'string'}
        }
      });
      var a = new Adapter(s, s);
      assert.throws(function () {
        a._decodeRequest(utils.bufferFrom([24]));
      }, /truncated/);
      assert.throws(function () {
        a._decodeResponse(utils.bufferFrom([48]), {headers: {}}, s.message('echo'));
      }, /truncated/);
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
        assert(/interrupted/.test(err), err);
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

  suite('StatefulClientChannel', function () {

    test('connection timeout', function (done) {
      var svc = Service.forProtocol({
        protocol: 'Ping',
        messages: {ping: {request: [], response: 'boolean'}}
      });
      var transport = {
        readable: new stream.PassThrough(),
        writable: new stream.PassThrough()
      };
      svc.createClient().createChannel(transport, {timeout: 5})
        .on('eot', function (pending, err) {
          assert(/timeout/.test(err), err);
          assert.strictEqual(this.client.service, svc);
          assert(this.destroyed);
          done();
        });
    });

    test('ping', function (done) {
      var svc = Service.forProtocol({protocol: 'Ping' });
      var transport = {
        readable: new stream.PassThrough({objectMode: true}),
        writable: new stream.PassThrough({objectMode: true})
      };
      var channel = svc.createClient()
        .createChannel(transport, {noPing: true, objectMode: true, timeout: 5})
        .on('eot', function () { done(); });
      channel.ping(function (err) {
        assert(/timeout/.test(err), err);
        channel.destroy();
      });
    });

    test('readable ended', function (done) {
      var svc = Service.forProtocol({
        protocol: 'Ping',
        messages: {ping: {request: [], response: 'boolean'}}
      });
      var transports = createPassthroughTransports();
      svc.createClient()
        .createChannel(transports[0])
        .on('eot', function () { done(); });
      transports[0].readable.push(null);
    });

    test('writable finished', function (done) {
      var svc = Service.forProtocol({
        protocol: 'Ping',
        messages: {ping: {request: [], response: 'boolean'}}
      });
      // We must use object mode here since ending the encoding stream won't
      // end the underlying writable stream.
      var transports = createPassthroughTransports(true);
      svc.createClient()
        .createChannel(transports[0], {objectMode: true})
        .on('eot', function () { done(); });
      transports[0].writable.end();
    });

    test('keep writable open', function (done) {
      var svc = Service.forProtocol({
        protocol: 'Ping',
        messages: {ping: {request: [], response: 'boolean'}}
      });
      // We must use object mode here since ending the encoding stream won't
      // end the underlying writable stream.
      var transports = createPassthroughTransports(true);
      svc.createClient()
        .createChannel(transports[0], {objectMode: true, endWritable: false})
        .on('eot', function () {
          transports[0].writable.write({}); // Doesn't fail.
          done();
        })
        .destroy();
    });

    test('discover service', function (done) {
      // Check that we can interrupt a handshake part-way, so that we can ping
      // a remote server for its service, but still reuse the same connection
      // for a later trasnmission.
      var svc1 = Service.forProtocol({protocol: 'Empty'});
      var svc2 = Service.forProtocol({
        protocol: 'Ping',
        messages: {ping: {request: [], response: 'boolean'}}
      });

      var transports = createPassthroughTransports();
      var server2 = svc2.createServer()
        .onPing(function (cb) { cb(null, true); });
      var chn1 = server2.createChannel(transports[0]);
      assert.strictEqual(chn1.getProtocol(), svc2); // Deprecated.
      assert.strictEqual(chn1.server.service, svc2);
      svc1.createClient()
        .createChannel(transports[1], {endWritable: false})
        .on('handshake', function (hreq, hres) {
          this.destroy();
          assert.equal(hres.serverProtocol, JSON.stringify(svc2.protocol));
        })
        .on('eot', function () {
          // The transports are still available for a connection.
          var client = svc2.createClient();
          var chn2 = client.createChannel(transports[1]);
          client.ping(function (err, res) {
            assert.strictEqual(err, null);
            assert.strictEqual(res, true);
            chn2.on('eot', function () { done(); }).destroy();
          });
        });
    });

    test('trailing decoder', function (done) {
      var svc = Service.forProtocol({
        protocol: 'Ping',
        messages: {ping: {request: [], response: 'boolean'}}
      });
      var transports = createPassthroughTransports();
      svc.createClient()
        .createChannel(transports[0], {noPing: true})
        .on('eot', function (pending, err) {
          assert.equal(pending, 0);
          assert(/trailing/.test(err), err);
          done();
        });
      transports[0].readable.end(utils.bufferFrom([48]));
    });
  });

  suite('StatelessClientChannel', function () {

    test('factory error', function (done) {
      var svc = Service.forProtocol({
        protocol: 'Ping',
        messages: {ping: {request: [], response: 'boolean'}}
      }, {wrapUnions: true});
      var client = svc.createClient({strictTypes: true});
      var chn = client.createChannel(function (cb) {
        return new stream.PassThrough({objectMode: true})
          .on('finish', function () { cb(new Error('foobar')); });
      }, {noPing: true, objectMode: true});
      client.ping(function (err) {
        assert(/foobar/.test(err.string), err);
        assert(!chn.destroyed);
        assert(!chn.isDestroyed()); // Deprecated.
        done();
      });
    });

    test('factory error no writable', function (done) {
      var svc = Service.forProtocol({
        protocol: 'Ping',
        messages: {ping: {request: [], response: 'boolean'}}
      }, {wrapUnions: true});
      var client = svc.createClient();
      var chn = client.createChannel(function () {}, {noPing: true});
      client.ping(function (err) {
        assert(/invalid writable stream/.test(err), err);
        assert(!chn.destroyed);
        assert(!chn.isDestroyed()); // Deprecated.
        done();
      });
    });

    test('trailing data', function (done) {
      var svc = Service.forProtocol({
        protocol: 'Ping',
        messages: {ping: {request: [], response: 'boolean'}}
      }, {wrapUnions: true});
      var client = svc.createClient();
      var readable = new stream.PassThrough();
      var sawError = false;
      var chn = client.createChannel(function (cb) {
        cb(null, readable);
        return new stream.PassThrough();
      }, {noPing: true})
        .on('eot', function (pending, err) {
          assert(/trailing/.test(err), err);
          sawError = true;
        });
      client.ping(function (err) {
        assert(/interrupted/.test(err), err);
        assert(chn.destroyed);
        assert(sawError);
        done();
      });
      readable.end(utils.bufferFrom([48]));
    });

    test('default encoder error', function (done) {
      var svc = Service.forProtocol({
        protocol: 'Ping',
        messages: {ping: {request: [], response: 'boolean'}}
      }, {wrapUnions: true});
      var client = svc.createClient({strictTypes: true});
      var chn = client.createChannel(function (cb) {
        return new stream.PassThrough()
          .on('finish', function () { cb(new Error('foobar')); });
      }, {noPing: true});
      client.ping(function (err) {
        assert(/foobar/.test(err.string));
        assert(!chn.destroyed);
        done();
      });
    });

    test('reuse writable', function (done) {
      var svc = Service.forProtocol({
        protocol: 'Ping',
        messages: {ping: {request: [], response: 'null'}}
      });
      var readable = new stream.PassThrough({objectMode: true});
      var writable = new stream.PassThrough({objectMode: true})
        .on('data', function (data) {
          var hres = utils.bufferFrom([0, 0, 0, 0]); // Encoded handshake response.
          var res = utils.bufferFrom([0, 0]); // Encoded response (flag and meta).
          readable.write({id: data.id, payload: [hres, res]});
        });
      var client = svc.createClient();
      client.createChannel(function (cb) {
        cb(null, readable);
        return writable;
      }, {noPing: true, objectMode: true, endWritable: false});
      client.ping(function (err) {
        assert(!err, err);
        client.ping(function (err) {
          assert(!err, err); // We can reuse it.
          done();
        });
      });
    });

    test('invalid handshake response', function (done) {
      var svc = Service.forProtocol({
        protocol: 'Ping',
        messages: {ping: {request: [], response: 'null'}}
      });
      var readable = new stream.PassThrough({objectMode: true});
      var writable = new stream.PassThrough({objectMode: true})
        .on('data', function (data) {
          var buf = utils.bufferFrom([0, 0, 0, 2, 48]);
          readable.write({id: data.id, payload: [buf]});
        });
      var client = svc.createClient();
      client.createChannel(function (cb) {
        cb(null, readable);
        return writable;
      }, {noPing: true, objectMode: true, endWritable: false});
      client.ping(function (err) {
        assert(/truncated.*HandshakeResponse/.test(err), err);
        done();
      });
    });

    test('interrupt writable', function (done) {
      var svc = Service.forProtocol({
        protocol: 'Ping',
        messages: {ping: {request: [], response: 'null'}}
      });
      // Fake handshake response.
      var hres = services.HANDSHAKE_RESPONSE_TYPE.clone({
        match: 'NONE',
        serverService: JSON.stringify(svc.protocol),
        serverHash: svc.hash
      });
      var readable = new stream.PassThrough({objectMode: true});
      var writable = new stream.PassThrough({objectMode: true})
        .on('data', function (data) {
          readable.write({id: data.id, payload: [hres.toBuffer()]});
        });
      var numHandshakes = 0;
      var client = svc.createClient();
      client.createChannel(function (cb) {
        cb(null, readable);
        return writable;
      }, {objectMode: true}).on('handshake', function (hreq, actualHres) {
        numHandshakes++;
        assert.deepEqual(actualHres, hres);
        this.destroy(true);
      }).on('error', function (err) {
        assert(/interrupted/.test(err), err);
      });
      setTimeout(function () {
        // Only a single handshake should have occurred.
        assert.equal(numHandshakes, 1);
        done();
      }, 50);
    });
  });

  suite('StatefulServerChannel', function () {

    test('readable ended', function (done) {
      var svc = Service.forProtocol({
        protocol: 'Ping',
        messages: {ping: {request: [], response: 'boolean'}}
      });
      var transport = new stream.PassThrough();
      svc.createServer().createChannel(transport).on('eot', function () {
        assert(this.destroyed);
        assert(this.isDestroyed()); // Deprecated.
        done();
      });
      transport.push(null);
    });

    test('readable trailing data', function (done) {
      var svc = Service.forProtocol({
        protocol: 'Ping',
        messages: {ping: {request: [], response: 'boolean'}}
      });
      var transport = new stream.PassThrough();
      svc.createServer().createChannel(transport)
        .on('eot', function (pending, err) {
          assert(/trailing/.test(err), err);
          assert(this.destroyed);
          assert(this.isDestroyed()); // Deprecated.
          done();
        });
      transport.end(utils.bufferFrom([48]));
    });

    test('writable finished', function (done) {
      var svc = Service.forProtocol({
        protocol: 'Ping',
        messages: {ping: {request: [], response: 'boolean'}}
      });
      // We must use object mode here since ending the encoding stream won't
      // end the underlying writable stream.
      var transports = createPassthroughTransports(true);
      svc.createServer().createChannel(transports[0], {objectMode: true})
        .on('eot', function () { done(); });
      transports[0].writable.end();
    });
  });

  suite('StatelessServerChannel', function () {

    test('factory error', function (done) {
      var svc = Service.forProtocol({
        protocol: 'Ping',
        messages: {ping: {request: [], response: 'boolean'}}
      });
      svc.createServer({silent: true}).createChannel(function (cb) {
        cb(new Error('bar'));
        return new stream.PassThrough();
      }).on('eot', function (pending, err) {
          assert(/bar/.test(err), err);
          done();
        });
    });

    test('trailing data', function (done) {
      var svc = Service.forProtocol({
        protocol: 'Ping',
        messages: {ping: {request: [], response: 'boolean', errors: ['int']}}
      });
      var transports = createPassthroughTransports();
      svc.createServer({silent: true}).createChannel(function (cb) {
        cb(null, transports[0].writable);
        return transports[1].readable;
      }).on('eot', function (pending, err) {
        assert(/trailing/.test(err), err);
        done();
      });
      transports[1].readable.end(utils.bufferFrom([48]));
    });

    test('delayed writable', function (done) {
      var svc = Service.forProtocol({
        protocol: 'Ping',
        messages: {ping: {request: [], response: 'boolean', errors: ['int']}}
      });
      var objs = [];
      var readable = new stream.PassThrough({objectMode: true});
      var writable = new stream.PassThrough({objectMode: true})
        .on('data', function (obj) { objs.push(obj); });
      svc.createServer({silent: true}).createChannel(function (cb) {
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
            clientHash: svc.hash,
            serverHash: svc.hash
          }),
          utils.bufferFrom([3]) // Invalid request contents.
        ]
      });
    });

    test('reuse writable', function (done) {
      var svc = Service.forProtocol({
        protocol: 'Ping',
        messages: {ping: {request: [], response: 'null'}}
      }).on('ping', function (req, ee, cb) {
        cb(null, null);
      });
      var payload = [
        services.HANDSHAKE_REQUEST_TYPE.toBuffer({
          clientHash: svc.hash,
          serverHash: svc.hash
        }),
        utils.bufferFrom('\x00\x08ping')
      ];
      var objs = [];
      var readable = new stream.PassThrough({objectMode: true});
      var writable = new stream.PassThrough({objectMode: true})
        .on('data', function (obj) { objs.push(obj); });
      svc.createServer({silent: true}).createChannel(function (cb) {
        cb(null, writable);
        return readable;
      }, {endWritable: false, noPing: true, objectMode: true})
        .on('eot', function () {
          assert.deepEqual(objs.length, 2);
          done();
        });
      readable.write({id: 0, payload: payload});
      readable.end({id: 1, payload: payload});
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
            serverHash: p2.hash
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
                  assert(/invalid "negate" request/.test(err), err);
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
            assert(/invalid "negate" request/.test(err), err);
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
            assert(/must be non-negative/.test(err.message), err);
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
            assert(/internal server error/.test(err), err);
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
            // In reality the server wouldn't be used (since this is the old
            // API), but this makes this test do its job.
            ee.server.on('error', function (err) {
              assert(/duplicate handler call/.test(err), err);
              done();
            });
            cb(null, null);
            cb(null, null);
          });
          ptcl.emit('ping', {}, ee); // No error on the emitter side.
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
            assert(/not implemented/.test(err), err);
            done();
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
            assert.equal(pending, 2);
            setTimeout(function () {
              assert.equal(interrupted, 2);
              done();
            }, 5);
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
            assert.strictEqual(ee.getProtocol(), ptcl);
            ee.destroy();
            this.emit('negate', {n: 'hi'}, ee, function (err) {
              assert(/no active channels/.test(err.message), err);
              done();
            });
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
              assert(/internal server error/.test(err), err);
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

    test('no emitters without buffering', function (done) {
      var svc = Service.forProtocol({
        protocol: 'Ping',
        messages: {ping: {request: [], response: 'boolean'}}
      });
      var client = svc.createClient({buffering: false})
        .on('error', function (err) {
          assert(/no active channels/.test(err), err);
          done();
        });
      assert.strictEqual(client.service, svc);
      // With callback.
      client.ping(function (err) {
        assert(/no active channels/.test(err), err);
        assert.deepEqual(this.locals, {});
        assert.strictEqual(this.channel, undefined);
        // Without (triggering the error above).
        client.ping();
      });
    });

    test('destroy emitters', function (done) {
      var svc = Service.forProtocol({
        protocol: 'Ping',
        messages: {ping: {request: [], response: 'boolean'}}
      });
      var transport = {
        readable: new stream.PassThrough(),
        writable: new stream.PassThrough()
      };
      var client = svc.createClient();
      client.createChannel(transport)
        .on('eot', function () {
          done();
        });
      client.destroyChannels({noWait: true});
    });

    test('default policy', function (done) {
      var svc = Service.forProtocol({
        protocol: 'Ping',
        messages: {ping: {request: [], response: 'null', 'one-way': true}}
      });
      var transport = {
        readable: new stream.PassThrough(),
        writable: new stream.PassThrough()
      };
      var client = svc.createClient();
      client.createChannel(transport, {noPing: true});
      client.createChannel(transport, {noPing: true});
      client.ping(function (err) {
        assert(!err, err);
        assert.strictEqual(this.channel.client, client);
        done();
      });
    });

    test('custom policy', function (done) {
      var svc = Service.forProtocol({
        protocol: 'Ping',
        messages: {ping: {request: [], response: 'boolean'}}
      });
      var transport = {
        readable: new stream.PassThrough(),
        writable: new stream.PassThrough()
      };

      var client = svc.createClient({channelPolicy: policy});
      var channels = [
        client.createChannel(transport, {noPing: true}),
        client.createChannel(transport, {noPing: true})
      ];
      client.ping();

      function policy(channels_) {
        assert.deepEqual(channels_, channels);
        done();
      }
    });

    test('remote protocols existing', function () {
      var ptcl1 = Service.forProtocol({protocol: 'Empty1'});
      var ptcl2 = Service.forProtocol({protocol: 'Empty2'});
      var remotePtcls = {abc: ptcl2.protocol};
      var client = ptcl1.createClient({
        remoteFingerprint: 'abc',
        remoteProtocols: remotePtcls
      });
      assert.deepEqual(client.remoteProtocols(), remotePtcls);
    });

    test('invalid response', function (done) {
      var svc = Service.forProtocol({
        protocol: 'Ping',
        messages: {ping: {request: [], response: 'int'}}
      });
      var opts = {noPing: true, objectMode: true};
      var transport = {
        readable: new stream.PassThrough(opts),
        writable: new stream.PassThrough(opts)
      };
      var client = svc.createClient();
      client.createChannel(transport, opts);
      client.ping(function (err) {
        assert(/truncated/.test(err), err);
        done();
      });
      setTimeout(function () {
        // "Send" an invalid payload (negative union offset). We wait to allow
        // the callback for the above message to be registered.
        transport.readable.write({id: 1, payload: [utils.bufferFrom([45])]});
      }, 0);
    });
  });

  suite('Server', function () {

    test('get channels', function (done) {
      var svc = Service.forProtocol({protocol: 'Empty1'});
      var server = svc.createServer();
      var transport = {
        readable: new stream.PassThrough(),
        writable: new stream.PassThrough()
      };
      var channels = [
        server.createChannel(transport),
        server.createChannel(transport)
      ];
      assert.deepEqual(server.activeChannels(), channels);
      channels[0]
        .on('eot', function () {
          assert.deepEqual(server.activeChannels(), [channels[1]]);
          done();
        })
        .destroy();
    });

    test('remote protocols', function () {
      var svc1 = Service.forProtocol({protocol: 'Empty1'});
      var svc2 = Service.forProtocol({protocol: 'Empty2'});
      var remotePtcls = {abc: svc2.protocol};
      var server = svc1.createServer({remoteProtocols: remotePtcls});
      assert.deepEqual(server.remoteProtocols(), remotePtcls);
    });

    test('no capitalization', function () {
      var svc = Service.forProtocol({
        protocol: 'Ping',
        messages: {ping: {request: [], response: 'boolean'}}
      });
      var server = svc.createServer({noCapitalize: true});
      assert(!server.onPing);
      assert(typeof server.onping == 'function');
    });

    test('stateful transport reuse', function (done) {
      var svc = Service.forProtocol({
        protocol: 'Ping',
        messages: {ping: {request: [], response: 'int'}}
      });
      var transports = createPassthroughTransports(true);
      var server = svc.createServer()
        .onPing(function (cb) {
          cb(null, 1);
        });
      var channel = server.createChannel(transports[0], {endWritable: false});
      var client = svc.createClient()
        .once('channel', function () {
          this.ping(function (err, n) {
            // At this point the handshake has succeeded.
            // Check that the response is as expected.
            assert(!err, err);
            assert.equal(n, 1);
            channel.destroy(); // Destroy the server's channel (instant).
            this.channel.destroy(); // Destroy the client's channel (instant).
            // We can now reuse the transports.
            server.createChannel(transports[0]);
            client
              .once('channel', function () {
                this.ping(function (err, n) {
                  assert(!err, err);
                  assert.equal(n, 1);
                  done();
                });
              })
              .createChannel(transports[1]);
          });
        });
      // Create the first channel.
      client.createChannel(transports[1], {endWritable: false});
    });

    test('interrupted', function (done) {
      var svc = Service.forProtocol({
        protocol: 'Ping',
        messages: {ping: {request: [], response: 'int'}}
      });
      var server = svc.createServer()
        .onPing(function (cb) {
          this.channel.destroy(true);
          cb(null, 1); // Still call the callback to make sure it is ignored.
        });
      svc.createClient({server: server})
        .once('channel', function () {
          this.ping({timeout: 10}, function (err) {
            assert(/interrupted/.test(err), err);
            done();
          });
        });
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
      svc.createClient({buffering: true, server: server})
        .echo(123, function (err, n) {
          assert(!err, err);
          assert.equal(n, 123);
          done();
        });
    });

    test('client context call options', function (done) {
      var svc = Service.forProtocol({
        protocol: 'Math',
        messages: {
          neg: {request: [{name: 'n', type: 'int'}], response: 'int'}
        }
      });
      var server = svc.createServer()
        .onNeg(function (n, cb) { cb(null, -n); });
      var opts = {id: 123};
      var client = svc.createClient({server: server})
        .once('channel', function (channel) {
          channel.on('outgoingCall', function (ctx, opts) {
            ctx.locals.id = opts.id;
          });
          client.neg(1, opts, function (err, n) {
            assert(!err, err);
            assert.equal(n, -1);
            assert.equal(this.locals.id, 123);
            done();
          });
        });
    });

    test('server call constant context', function (done) {
      var svc = Service.forProtocol({
        protocol: 'Math',
        messages: {
          neg: {request: [{name: 'n', type: 'int'}], response: 'int'}
        }
      });
      var numCalls = 0;
      var server = svc.createServer()
        .use(function (wreq, wres, next) {
          // Check that middleware have the right context.
          this.locals.id = 123;
          assert.equal(numCalls++, 0);
          next(null, function (err, prev) {
            assert(!err, err);
            assert.equal(this.locals.id, 456);
            assert.equal(numCalls++, 2);
            prev(err);
          });
        })
        .onNeg(function (n, cb) {
          assert.equal(this.locals.id, 123);
          this.locals.id = 456;
          assert.equal(numCalls++, 1);
          cb(null, -n);
        });
      svc.createClient({buffering: true, server: server})
        .neg(1, function (err, n) {
          assert(!err, err);
          assert.equal(n, -1);
          assert.equal(numCalls, 3);
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
      var locals = {num: 123};
      var server = svc.createServer()
        .on('channel', function (channel) {
          channel.on('incomingCall', function (ctx) {
            ctx.locals.num = locals.num;
          });
        })
        .use(function (wreq, wres, next) {
          assert.deepEqual(this.locals, locals);
          next();
        })
        .onNeg(function (n, cb) {
          assert.deepEqual(this.locals, locals);
          cb(null, -n);
        });
      svc.createClient({server: server})
        .once('channel', function () {
          this.neg(1, function (err, n) {
            assert(!err, err);
            assert.equal(n, -1);
            done();
          });
        });
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

      svc.createClient({server: server})
        .once('channel', function () {
          this.neg(1, function (err, n) {
            assert(!err, err);
            assert.equal(n, -1);
            this.channel.client.abs(5, function (err, n) {
              assert(!err, err);
              assert.equal(n, 10);
              done();
            });
          });
        });

      function defaultHandler(wreq, wres, cb) {
        // jshint -W040
        assert.equal(this.message.name, 'abs');
        wres.response = 10;
        cb();
      }
    });

    test('client middleware bypass', function (done) {
      var svc = Service.forProtocol({
        protocol: 'Math',
        messages: {
          neg: {request: [{name: 'n', type: 'int'}], response: 'int'}
        }
      });
      var server = svc.createServer()
        .onNeg(function (n, cb) { cb(null, -n); });
      var isCalled = false;
      svc.createClient({server: server})
        .use(function (wreq, wres, next) {
          wres.response = -3;
          next();
        })
        .use(function (wreq, wres, next) {
          isCalled = true;
          next();
        })
        .once('channel', function () {
          this.neg(1, function (err, n) {
            assert(!err, err);
            assert.equal(n, -3);
            assert(!isCalled);
            done();
          });
        });
    });

    test('client middleware override error', function (done) {
      var svc = Service.forProtocol({
        protocol: 'Math',
        messages: {
          neg: {request: [{name: 'n', type: 'int'}], response: 'int'}
        }
      });
      var server = svc.createServer()
        .onNeg(function (n, cb) { cb(null, -n); });
      svc.createClient({buffering: true, server: server})
        .use(function (wreq, wres, next) {
          next(null, function (err, prev) {
            assert(/bar/.test(err), err);
            prev(null); // Substitute `null` as error.
          });
        })
        .use(function (wreq, wres, next) {
          next(null, function (err, prev) {
            assert(!err, err);
            assert.equal(wres.response, -2);
            prev(new Error('bar'));
          });
        })
        .neg(2, function (err) {
          assert.strictEqual(err, null);
          done();
        });
    });

    test('server middleware bypass', function (done) {
      var svc = Service.forProtocol({
        protocol: 'Math',
        messages: {
          neg: {request: [{name: 'n', type: 'int'}], response: 'int'}
        }
      });
      var handlerCalled = false;
      var errorTriggered = false;
      var server = svc.createServer()
        .on('error', function (err) {
          assert(/foobar/.test(err.cause), err);
          errorTriggered = true;
        })
        .use(function (wreq, wres, next) {
          wres.error = 'foobar';
          next();
        })
        .onNeg(function (n, cb) {
          handlerCalled = true;
          cb(null, -n);
        });
      svc.createClient({server: server})
        .once('channel', function () {
          this.neg(1, function (err) {
            assert(/foobar/.test(err), err);
            assert(!handlerCalled);
            setTimeout(function () {
              assert(errorTriggered);
              done();
            }, 0);
          });
        });
    });

    test('dynamic middleware', function (done) {
      var svc = Service.forProtocol({
        protocol: 'Math',
        messages: {
          neg: {request: [{name: 'n', type: 'int'}], response: 'int'}
        }
      });
      var server = svc.createServer()
        .use(function (server) {
          server.on('channel', function (channel) {
            channel.on('incomingCall', function (ctx) {
              ctx.locals.foo = 'bar';
            });
          });
          return function (wreq, wres, next) {
            wreq.request.n = 3;
            next();
          };
        })
        .onNeg(function (n, cb) {
          assert.equal(this.locals.foo, 'bar');
          cb(null, -n);
        });
      svc.createClient({buffering: true, server: server})
        .use(function (client) {
          client.on('channel', function (channel) {
            channel.on('outgoingCall', function (ctx, opts) {
              ctx.locals.two = opts.two;
            });
          });
          return function (wreq, wres, next) { next(); };
        })
        .neg(1, {two: 2}, function (err, n) {
          assert(!err, err);
          assert.equal(n, -3);
          assert.equal(this.locals.two, 2);
          done();
        });
    });

    test('server non-strict error', function (done) {
      var svc = Service.forProtocol({
        protocol: 'Math',
        messages: {
          neg: {request: [{name: 'n', type: 'int'}], response: 'int'}
        }
      });
      var errorTriggered = false;
      var server = svc.createServer()
        .on('error', function () {
          errorTriggered = true;
        })
        .onNeg(function (n, cb) {
          cb(null, -n);
        });
      svc.createClient({server: server})
        .once('channel', function () {
          this.neg(1, function (err, n) {
            assert(!err, err);
            assert.equal(n, -1);
            setTimeout(function () {
              assert(!errorTriggered);
              done();
            }, 0);
          });
        });
    });

    test('server one-way middleware error', function (done) {
      var svc = Service.forProtocol({
        protocol: 'Push',
        messages: {
          push: {
            request: [{name: 'n', type: 'int'}],
            response: 'null',
            'one-way': true
          }
        }
      });
      var server = svc.createServer()
        .on('error', function (err) {
          assert(/foobar/.test(err), err);
          done();
        })
        .use(function (wreq, wres, next) {
          next(new Error('foobar'));
        });
      svc.createClient({server: server})
        .once('channel', function () { this.push(1); });
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
        client.createChannel({readable: pt1, writable: pt2});
        var server = serverPtcl.createServer(opts);
        server.createChannel({readable: pt2, writable: pt1}, opts);
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
        client.createChannel(writableFactory);
        var server = serverPtcl.createServer(opts);
        cb(client, server);

        function writableFactory(transportCt) {
          var reqPt = new stream.PassThrough()
            .on('finish', function () {
              server.createChannel(function (channelCb) {
                var resPt = new stream.PassThrough()
                  .on('finish', function () { transportCt(null, resPt); });
                channelCb(null, resPt);
                return reqPt;
              }, opts);
            });
          return reqPt;
        }
      });

    });

    function run(setupFn) {

      test('primitive types', function (done) {
        var svc = Service.forProtocol({
          protocol: 'Math',
          messages: {
            negateFirst: {
              request: [{name: 'ns', type: {type: 'array', items: 'int'}}],
              response: 'long'
            }
          }
        });
        setupFn(svc, svc, function (client, server) {
          server
            .onNegateFirst(function (ns, cb) {
              assert.strictEqual(this.channel.server, server);
              cb(null, -ns[0]);
            });
          var channel = client.activeChannels()[0];
          channel.on('eot', function () {
              done();
            })
            .once('handshake', function (hreq, hres) {
              // Allow the initial ping to complete.
              assert.equal(hres.match, 'BOTH');
              process.nextTick(function () {
                client.negateFirst([20], function (err, res) {
                  assert.equal(this.channel, channel);
                  assert.strictEqual(err, null);
                  assert.equal(res, -20);
                  client.negateFirst([-10, 'ni'],  function (err) {
                    assert(/invalid "negateFirst" request/.test(err), err);
                    this.channel.destroy();
                  });
                });
              });
            });
        });
      });

      test('invalid strict error', function (done) {
        var svc = Service.forProtocol({
          protocol: 'Math',
          messages: {
            sqrt: {
              request: [{name: 'n', type: 'float'}],
              response: 'float'
            }
          }
        });
        setupFn(svc, svc, {strictTypes: true}, function (client, server) {
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
            assert(/internal server error/.test(err), err);
            client.sqrt(-2, function (err) {
              assert(/internal server error/.test(err), err);
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

      test('non-strict response', function (done) {
        var svc = Service.forProtocol({
          protocol: 'Ping',
          messages: {
            ping: {request: [], response: 'null'}
          }
        });
        setupFn(svc, svc, function (client, server) {
          server.onPing(function (cb) { cb(); });
          client.ping(function (err) {
            assert(!err, err);
            done();
          });
        });
      });

      test('invalid strict response', function (done) {
        var svc = Service.forProtocol({
          protocol: 'Ping',
          messages: {
            ping: {request: [], response: 'null'}
          }
        });
        setupFn(svc, svc, {strictTypes: true}, function (client, server) {
          server.onPing(function (cb) { cb(); });
          client.ping(function (err) {
            assert(/internal server error/.test(err), err);
            done();
          });
        });
      });

      test('client middleware', function (done) {
        var svc = Service.forProtocol({
          protocol: 'Math',
          messages: {
            neg: {request: [{name: 'n', type: 'int'}], response: 'int'}
          }
        });
        setupFn(svc, svc, function (client, server) {
          server.onNeg(function (n, cb) { cb(null, -n); });
          var buf = utils.bufferFrom([0, 1]);
          var isDone = false;
          var channel = client.activeChannels()[0];
          client
            .use(function (wreq, wres, next) {
              // No callback.
              assert.strictEqual(this.channel, channel);
              assert.deepEqual(wreq.headers, {});
              wreq.headers.buf = buf;
              assert.deepEqual(wreq.request, {n: 2});
              next();
            })
            .use(function (wreq, wres, next) {
              // Callback here.
              assert.deepEqual(wreq.headers, {buf: buf});
              wreq.request.n = 3;
              next(null, function (err, prev) {
                assert(!err, err);
                assert.strictEqual(this.channel, channel);
                assert.deepEqual(wres.response, -3);
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
        var svc = Service.forProtocol({
          protocol: 'Math',
          messages: {
            neg: {request: [{name: 'n', type: 'int'}], response: 'int'}
          }
        });
        setupFn(svc, svc, function (client, server) {
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
        var svc = Service.forProtocol({
          protocol: 'Math',
          messages: {
            neg: {request: [{name: 'n', type: 'int'}], response: 'int'}
          }
        });
        setupFn(svc, svc, function (client, server) {
          server.onNeg(function (n, cb) { cb(null, -n); });
          var chn = client.activeChannels()[0];
          client
            .on('error', function (err, chn_) {
              assert(/duplicate forward middleware/.test(err), err);
              assert.strictEqual(chn_, chn);
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
        var svc = Service.forProtocol({
          protocol: 'Math',
          messages: {
            neg: {request: [{name: 'n', type: 'int'}], response: 'int'}
          }
        });
        setupFn(svc, svc, function (client, server) {
          var isDone = false;
          var buf = utils.bufferFrom([0, 1]);
          // The server's channel won't be ready right away in the case of
          // stateless transports.
          var channel;
          server
            .use(function (wreq, wres, next) {
              channel = this.channel;
              assert.strictEqual(channel.server, server);
              assert.deepEqual(wreq.request, {n: 2});
              next(null, function (err, prev) {
                assert.strictEqual(this.channel, channel);
                wres.headers.buf = buf;
                prev();
              });
            })
            .onNeg(function (n, cb) { cb(null, -n); });
          client
            .use(function (wreq, wres, next) {
              next(null, function (err, prev) {
                assert.deepEqual(wres.headers, {buf: buf});
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
        var svc = Service.forProtocol({
          protocol: 'Math',
          messages: {
            neg: {request: [{name: 'n', type: 'int'}], response: 'int'}
          }
        });
        setupFn(svc, svc, function (client, server) {
          server
            .use(function (wreq, wres, next) {
              // Attach error handler to channel.
              server.on('error', function (err, chn) {
                assert(/duplicate backward middleware/.test(err), err);
                assert.strictEqual(chn.server. server);
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
        var svc = Service.forProtocol({
          protocol: 'Math',
          messages: {
            neg: {request: [{name: 'n', type: 'int'}], response: 'int'}
          }
        });
        setupFn(svc, svc, function (client, server) {
          var fooErr = new Error('foobar');
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
              wres.headers.id = 123;
              next();
            })
            .onNeg(function () { throw fooErr; });
          client
            .neg(2, function (err) {
              assert(/internal server error/.test(err), err);
            });
        });
      });

      test('error formatter', function (done) {
        var svc = Service.forProtocol({
          protocol: 'Math',
          messages: {
            neg: {request: [{name: 'n', type: 'int'}], response: 'int'}
          }
        });
        var opts = {systemErrorFormatter: formatter};
        var barErr = new Error('baribababa');
        setupFn(svc, svc, opts, function (client, server) {
          server
            .onNeg(function () { throw barErr; });
          client
            .neg(2, function (err) {
              assert(/FOO/.test(err));
              done();
            });
        });

        function formatter(err) {
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
              assert(!err, err);
              assert.equal(res, -2);
              var remotePtcl;
              // Client.
              remotePtcl = {};
              remotePtcl[serverSvc.hash] = serverPtcl;
              assert.deepEqual(client.remoteProtocols(), remotePtcl);
              // Server.
              remotePtcl = {};
              remotePtcl[clientSvc.hash] = clientPtcl;
              assert.deepEqual(server.remoteProtocols(), remotePtcl);
              done();
            });
        });
      });

      test('client timeout', function (done) {
        var svc = Service.forProtocol({
          protocol: 'Sleep',
          messages: {
            sleep: {request: [{name: 'ms', type: 'int'}], response: 'int'}
          }
        });
        setupFn(svc, svc, {timeout: 50}, function (client, server) {
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
              assert(/timeout/.test(err), err);
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
          server
            .on('error', function (err) {
              numErrors++;
              assert(/bar/.test(err), err);
            })
            .onNeg(function (n, cb) {
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

      test('client channel destroy no wait', function (done) {
        var svc = Service.forProtocol({
          protocol: 'Delay',
          messages: {
            wait: {
              request: [{name: 'ms', type: 'int'}],
              response: 'string'
            }
          }
        });
        var interrupted = 0;
        setupFn(svc, svc, function (client, server) {
          server.onWait(function (ms, cb) {
            setTimeout(function () { cb(null, 'ok'); }, ms);
          });
          var channel = client.activeChannels()[0];
          channel.on('eot', function (pending) {
            assert.equal(pending, 2);
            setTimeout(function () {
              assert.equal(interrupted, 2);
              done();
            }, 5);
          });
          client.wait(75, interruptedCb);
          client.wait(50, interruptedCb);
          client.wait(10, function (err, res) {
            assert.equal(res, 'ok');
            channel.destroy(true);
          });

          function interruptedCb(err) {
            assert(/interrupted/.test(err), err);
            interrupted++;
          }

        });
      });

      test('out of order requests', function (done) {
        var svc = Service.forProtocol({
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
        });
        var ids = [];
        setupFn(svc, svc, function (client, server) {
          server.onW(function (delay, id, cb) {
            if (delay < 0) {
              cb('delay must be non-negative');
              return;
            }
            setTimeout(function () { cb(null, id); }, delay);
          });
          var channel = client.activeChannels()[0];
          channel.on('eot', function (pending) {
            assert.equal(pending, 0);
            assert.deepEqual(ids, [undefined, 'b', 'a']);
            done();
          }).once('handshake', function (hreq, hres) {
            assert.equal(hres.match, 'BOTH');
            process.nextTick(function () {
              client.w(500, 'a', function (err, res) {
                assert.strictEqual(err, null);
                ids.push(res);
              });
              client.w(10, 'b', function (err, res) {
                assert.strictEqual(err, null);
                ids.push(res);
                channel.destroy();
              });
              client.w(-10, 'c', function (err, res) {
                assert(/non-negative/.test(err));
                ids.push(res);
              });
            });
          });
        });
      });

      test('destroy server channel during handshake', function (done) {
        var svc = Service.forProtocol({
          protocol: 'Sleep',
          messages: {
            sleep: {request: [{name: 'ms', type: 'int'}], response: 'int'}
          }
        });
        setupFn(svc, svc, {timeout: 20}, function (client, server) {
          // For stateful emitters, the channel already exists.
          server.activeChannels().forEach(onChannel);
          // For stateless emitters, the channel won't exist yet.
          server.on('channel', onChannel);
          client.sleep(10, function (err) {
            assert(/interrupted|destroyed/.test(err), err);
            done();
          });

          function onChannel(channel) {
            channel.on('handshake', function () {
              this.destroy(true);
            });
          }
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
      var svc = Service.forProtocol(schema);
      var server = svc.createServer()
        .onUpper(function (str, cb) {
          cb(null, str.toUpperCase());
        });
      var transports = createPassthroughTransports();
      server.createChannel(transports[1]);
      discoverProtocol(transports[0], function (err, actualAttrs) {
        assert.strictEqual(err, null);
        assert.deepEqual(actualAttrs, schema);
        // Check that the transport is still usable.
        var client = svc.createClient();
        client.createChannel(transports[0])
          .on('eot', function() {
            done();
          });
        client.upper('foo', function (err, res) {
          assert.strictEqual(err, null);
          assert.equal(res, 'FOO');
          this.channel.destroy();
        });
      });
    });

    test('legacy stateless ok', function (done) {
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
      var svc = Service.forProtocol(schema)
        .on('upper', function (req, ee, cb) {
          cb(null, req.str.toUpperCase());
        });
      discoverProtocol(writableFactory, function (err, actual) {
        assert.strictEqual(err, null);
        assert.deepEqual(actual, schema);
        // Check that the transport is still usable.
        var me = svc.createEmitter(writableFactory).on('eot', function() {
          done();
        });
        svc.emit('upper', {str: 'foo'}, me, function (err, res) {
          assert.strictEqual(err, null);
          assert.equal(res, 'FOO');
          me.destroy();
        });
      });

      function writableFactory(emitterCb) {
        var reqPt = new stream.PassThrough()
          .on('finish', function () {
            svc.createListener(function (listenerCb) {
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
      var svc = Service.forProtocol(schema);
      var scope = 'bar';
      var transports = createPassthroughTransports();
      svc.createServer({silent: true})
        .createChannel(transports[1], {scope: scope});
      discoverProtocol(transports[0], {timeout: 5}, function (err) {
        assert(/timeout/.test(err), err);
        // Check that the transport is still usable.
        var client = svc.createClient();
        var chn = client.createChannel(transports[0], {scope: scope})
          .on('eot', function() { done(); });
        client.upper('foo', function (err) {
          assert(/not implemented/.test(err), err);
          chn.destroy();
        });
      });
    });
  });
});

// Helpers.

// Message framing.
function frame(buf) {
  var framed = utils.newBuffer(buf.length + 4);
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
