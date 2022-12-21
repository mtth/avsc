'use strict';

let types = require('../lib/types'),
    services = require('../lib/services'),
    utils = require('../lib/utils'),
    assert = require('assert'),
    stream = require('stream');


let Service = services.Service;


suite('services', () => {

  suite('Service', () => {

    test('get name, types, and protocol', () => {
      let p = {
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
      let s = Service.forProtocol(p);
      assert.equal(s.name, 'foo.HelloWorld');
      assert.equal(s.type('foo.Greeting').getTypeName(), 'record');
      assert.equal(s.type('string').getTypeName(), 'string');
      assert.equal(s.types.length, 4);
      assert.deepEqual(s.protocol, p);
    });

    test('missing message', () => {
      let svc = Service.forProtocol({
        namespace: 'com.acme',
        protocol: 'Hello'
      });
      assert.throws(() => {
        svc.on('add', () => {});
      }, /unknown/);
    });

    test('missing name', () => {
      assert.throws(() => {
        Service.forProtocol({namespace: 'com.acme', messages: {}});
      });
    });

    test('missing type', () => {
      assert.throws(() => {
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

    test('multiple references to namespaced types', () => {
      // This test is a useful sanity check for hoisting implementations.
      let n = 0;
      let s = Service.forProtocol({
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

    test('special character in name', () => {
      assert.throws(() => {
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

    test('get messages', () => {
      let svc;
      svc = Service.forProtocol({protocol: 'Empty'});
      assert.deepEqual(svc.messages, []);
      svc = Service.forProtocol({
        protocol: 'Ping',
        messages: {
          ping: {
            request: [],
            response: 'string'
          }
        }
      });
      let messages = svc.messages;
      assert.deepEqual(messages, [svc.message('ping')]);
    });

    test('subprotocol', () => {
      let svc = Service.forProtocol({
        namespace: 'com.acme',
        protocol: 'Hello',
        types: [{name: 'Id', type: 'fixed', size: 2}],
        messages: {ping: {request: [], response: 'null'}}
      });
      let subptcl = svc.subprotocol();
      assert(subptcl.getFingerprint().equals(svc.getFingerprint()));
      assert.strictEqual(subptcl._emitterResolvers, svc._emitterResolvers);
      assert.strictEqual(subptcl._listenerResolvers, svc._listenerResolvers);
    });

    test('invalid emitter', (done) => {
      let svc1 = Service.forProtocol({protocol: 'Hey'});
      let svc2 = Service.forProtocol({protocol: 'Hi'});
      let ee = svc2.createEmitter(new stream.PassThrough(), {noPing: true});
      assert.throws(
        () => { svc1.emit('hi', {}, ee); },
        /invalid emitter/
      );
      done();
    });

    test('getSchema', () => {
      let schema = {
        protocol: 'Hello',
        messages: {
          ping: {request: [], response: 'boolean', doc: ''},
          pong: {request: [], response: 'null', 'one-way': true}
        },
        doc: 'Hey'
      };
      let svc = Service.forProtocol(schema);
      assert.deepEqual(svc.getSchema({exportAttrs: true}), schema);
    });

    test('getSchema no top-level type references', () => {
      let schema = {
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
      let svc = Service.forProtocol(schema);
      let t = svc.type('Foo');
      // Bar's reference shouldn't be included in the returned types array.
      assert.deepEqual(svc.getSchema().types, [t.getSchema()]);
    });

    test('get documentation', () => {
      let svc = Service.forProtocol({protocol: 'Hello', doc: 'Hey'});
      assert.equal(svc.doc, 'Hey');
    });

    test('getFingerprint', () => {
      let svc = Service.forProtocol({
        namespace: 'hello',
        protocol: 'World',
      });
      assert.deepEqual(svc.getFingerprint('md5'), svc.getFingerprint());
    });

    test('isService', () => {
      let svc = Service.forProtocol({
        namespace: 'hello',
        protocol: 'World',
      });
      assert(Service.isService(svc));
      assert(!Service.isService(undefined));
      assert(!Service.isService({protocol: 'bar'}));
    });

    test('equals', () => {
      let svc = Service.forProtocol({
        namespace: 'hello',
        protocol: 'World',
      });
      assert(svc.equals(svc));
      assert(!svc.equals(undefined));
      assert(!svc.equals(Service.forProtocol({protocol: 'Foo'})));
    });

    test('inspect', () => {
      let svc = Service.forProtocol({
        namespace: 'hello',
        protocol: 'World',
      });
      assert.equal(svc.inspect(), '<Service "hello.World">');
    });

    test('using constructor', () => {
      let svc = new services.Service({protocol: 'Empty'});
      assert.equal(svc.name, 'Empty');
      assert.deepEqual(svc.messages, []);
    });

    test('namespacing', () => {
      let svc;

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

    test('createClient transport option', (done) => {
      let svc = Service.forProtocol({protocol: 'Empty'});
      svc.createClient({transport: new stream.PassThrough()})
        .on('channel', () => { done(); });
    });

    test('createListener strict', () => {
      let svc = Service.forProtocol({protocol: 'Empty'});
      assert.throws(() => {
        svc.createListener(new stream.PassThrough(), {strictErrors: true});
      });
    });

    test('compatible', () => {
      let emptySvc = Service.forProtocol({protocol: 'Empty'});
      let pingSvc = Service.forProtocol({
        protocol: 'Ping',
        messages: {
          ping: {request: [], response: 'boolean'},
          pong: {request: [], response: 'int'}
        }
      });
      let pongSvc = Service.forProtocol({
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

  suite('Message', () => {

    let Message = services.Message;

    test('empty errors', () => {
      let m = Message.forSchema('Hi', {
        request: [{name: 'greeting', type: 'string'}],
        response: 'int'
      });
      assert.deepEqual(m.errorType.toString(), '["string"]');
    });

    test('non-array request', () => {
      assert.throws(() => {
        Message.forSchema('Hi', {
          request: 'string',
          response: 'int'
        });
      }, /invalid \w* request/);
    });

    test('missing response', () => {
      assert.throws(() => {
        Message.forSchema('Hi', {
          request: [{name: 'greeting', type: 'string'}]
        });
      }, /invalid \w* response/);
    });

    test('non-array errors', () => {
      assert.throws(() => {
        Message.forSchema('Hi', {
          request: [{name: 'greeting', type: 'string'}],
          response: 'int',
          errors: 'int'
        });
      }, /invalid \w* error/);
    });

    test('invalid one-way', () => {
      // Non-null response.
      assert.throws(() => {
        Message.forSchema('Hi', {
          request: [{name: 'greeting', type: 'string'}],
          response: 'string',
          'one-way': true
        });
      }, /inapplicable/);
      // Non-empty errors.
      assert.throws(() => {
        Message.forSchema('Hi', {
          request: [{name: 'greeting', type: 'string'}],
          response: 'null',
          errors: ['int'],
          'one-way': true
        });
      }, /inapplicable/);
    });

    test('getters', () => {
      let s = {
        request: [{name: 'ping', type: 'string'}],
        response: 'null'
      };
      let m = Message.forSchema('Ping', s);
      assert.equal(m.name, 'Ping');
      assert.equal(m.requestType.getFields()[0].getName(), 'ping');
      assert.equal(m.responseType.getName(true), 'null');
      assert.strictEqual(m.oneWay, false);
      assert.strictEqual(m.isOneWay(), false);
      assert.deepEqual(m.schema(), s);
    });

    test('get documentation', () => {
      let schema = {
        request: [{name: 'ping', type: 'string'}],
        response: 'null',
        doc: 'Pong'
      };
      let m = Message.forSchema('Ping', schema);
      assert.equal(m.doc, 'Pong');
    });

    test('invalid types', () => {
      assert.throws(() => {
        new Message('intRequest', types.Type.forSchema('int'));
      }, /invalid request type/);
      assert.throws(() => {
        new Message(
          'intError',
          types.Type.forSchema({type: 'record', fields: []}),
          types.Type.forSchema('int')
        );
      }, /invalid error type/);
    });

    test('schema multiple errors', () => {
      let s = {
        request: [{name: 'ping', type: 'string'}],
        response: 'null',
        errors: ['int', 'bytes']
      };
      let m = Message.forSchema('Ping', s);
      assert.deepEqual(m.schema(), s);
    });
  });

  suite('FrameDecoder & FrameEncoder', () => {

    let FrameDecoder = services.streams.FrameDecoder;
    let FrameEncoder = services.streams.FrameEncoder;

    test('decode', (done) => {
      let frames = [
        utils.bufferFrom([0, 1]),
        utils.bufferFrom([2]),
        utils.bufferFrom([]),
        utils.bufferFrom([3, 4]),
        utils.bufferFrom([])
      ].map(frame);
      let messages = [];
      createReadableStream(frames)
        .pipe(new FrameDecoder())
        .pipe(createWritableStream(messages))
        .on('finish', () => {
          assert.deepEqual(
            messages,
            [
              {id: null, payload: [
                utils.bufferFrom([0, 1]),
                utils.bufferFrom([2])
              ]},
              {id: null, payload: [utils.bufferFrom([3, 4])]}
            ]
          );
          done();
        });
    });

    test('decode with trailing data', (done) => {
      let frames = [
        utils.bufferFrom([0, 1]),
        utils.bufferFrom([2]),
        utils.bufferFrom([]),
        utils.bufferFrom([3])
      ].map(frame);
      let messages = [];
      createReadableStream(frames)
        .pipe(new FrameDecoder())
        .on('error', () => {
          assert.deepEqual(
            messages,
            [{id: null, payload: [
              utils.bufferFrom([0, 1]),
              utils.bufferFrom([2])
            ]}]
          );
          done();
        })
        .pipe(createWritableStream(messages));
    });

    test('decode empty', (done) => {
      createReadableStream([])
        .pipe(new FrameDecoder())
        .pipe(createWritableStream([]))
        .on('finish', () => {
          done();
        });
    });

    test('encode empty', (done) => {
      let frames = [];
      createReadableStream([])
        .pipe(new FrameEncoder())
        .pipe(createWritableStream(frames))
        .on('finish', () => {
          assert.deepEqual(frames, []);
          done();
        });
    });

    test('encode', (done) => {
      let messages = [
        {id: 1, payload: [
          utils.bufferFrom([1, 3, 5]),
          utils.bufferFrom([6, 8])
        ]},
        {id: 4, payload: [utils.bufferFrom([123, 23])]}
      ];
      let frames = [];
      createReadableStream(messages)
        .pipe(new FrameEncoder())
        .pipe(createWritableStream(frames))
        .on('finish', () => {
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

    test('roundtrip', (done) => {
      let type = types.Type.forSchema({
        type: 'record',
        name: 'Record',
        fields: [
          {name: 'id', type: 'null'},
          {name: 'payload', type: {type: 'array', items: 'bytes'}}
        ]
      });
      let n = 100;
      let src = [];
      while (n--) {
        let record = type.random();
        record.payload = record.payload.filter((arr) => {
          return arr.length;
        });
        src.push(record);
      }
      let dst = [];
      let encoder = new FrameEncoder();
      let decoder = new FrameDecoder();
      createReadableStream(src)
        .pipe(encoder)
        .pipe(decoder)
        .pipe(createWritableStream(dst))
        .on('finish', () => {
          assert.deepEqual(dst, src);
          done();
        });
    });
  });

  suite('NettyDecoder & NettyEncoder', () => {

    let NettyDecoder = services.streams.NettyDecoder;
    let NettyEncoder = services.streams.NettyEncoder;

    test('decode with trailing data', (done) => {
      let src = [
        utils.bufferFrom([0, 0, 0, 2, 0, 0, 0]),
        utils.bufferFrom([1, 0, 0, 0, 5, 1, 3, 4, 2, 5, 1])
      ];
      let dst = [];
      createReadableStream(src)
        .pipe(new NettyDecoder())
        .on('error', () => {
          assert.deepEqual(
            dst,
            [{id: 2, payload: [utils.bufferFrom([1, 3, 4, 2, 5])]}]
          );
          done();
        })
        .pipe(createWritableStream(dst));
    });

    test('roundtrip', (done) => {
      let type = types.Type.forSchema({
        type: 'record',
        name: 'Record',
        fields: [
          {name: 'id', type: 'int'},
          {name: 'payload', type: {type: 'array', items: 'bytes'}}
        ]
      });
      let n = 200;
      let src = [];
      while (n--) {
        let record = type.random();
        record.payload = record.payload.filter((arr) => {
          return arr.length;
        });
        src.push(record);
      }
      let dst = [];
      let encoder = new NettyEncoder();
      let decoder = new NettyDecoder();
      createReadableStream(src)
        .pipe(encoder)
        .pipe(decoder)
        .pipe(createWritableStream(dst))
        .on('finish', () => {
          assert.deepEqual(dst, src);
          done();
        });
    });
  });

  suite('Adapter', () => {

    let Adapter = services.Adapter;

    test('truncated request & response', () => {
      let s = Service.forProtocol({
        protocol: 'Echo',
        messages: {
          echo: {request: [{name: 's', type: 'string'}], response: 'string'}
        }
      });
      let a = new Adapter(s, s);
      assert.throws(() => {
        a._decodeRequest(utils.bufferFrom([24]));
      }, /truncated/);
      assert.throws(() => {
        a._decodeResponse(
          utils.bufferFrom([48]),
          {headers: {}},
          s.message('echo')
        );
      }, /truncated/);
    });
  });

  suite('Registry', () => {

    let Registry = services.Registry;

    let id;
    test('get', (done) => {
      let ctx = {one: 1};
      let reg = new Registry(ctx);
      id = reg.add(200, function (err, two) {
        assert.strictEqual(this, ctx);
        assert.strictEqual(err, null);
        assert.equal(two, 2);
        assert.strictEqual(reg.get(id), undefined);
        done();
      });
      setTimeout(() => { reg.get(id)(null, 2); }, 50);
    });

    test('timeout', (done) => {
      let ctx = {one: 1};
      let reg = new Registry(ctx);
      id = reg.add(10, function (err) {
        assert.strictEqual(this, ctx);
        assert(/timeout/.test(err));
        assert.strictEqual(reg.get(id), undefined);
        done();
      });
    });

    test('no timeout', (done) => {
      let ctx = {one: 1};
      let reg = new Registry(ctx);
      id = reg.add(-1, function (err, two) {
        assert.strictEqual(this, ctx);
        assert.strictEqual(err, null);
        assert.equal(two, 2);
        assert.strictEqual(reg.get(id), undefined);
        done();
      });
      reg.get(id)(null, 2);
    });

    test('clear', (done) => {
      let ctx = {one: 1};
      let reg = new Registry(ctx);
      let n = 0;
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

    test('mask', (done) => {
      let ctx = {one: 1};
      let n = 0;
      let reg = new Registry(ctx, 31);
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

  suite('StatefulClientChannel', () => {

    test('connection timeout', (done) => {
      let svc = Service.forProtocol({
        protocol: 'Ping',
        messages: {ping: {request: [], response: 'boolean'}}
      });
      let transport = {
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

    test('ping', (done) => {
      let svc = Service.forProtocol({protocol: 'Ping' });
      let transport = {
        readable: new stream.PassThrough({objectMode: true}),
        writable: new stream.PassThrough({objectMode: true})
      };
      let channel = svc.createClient()
        .createChannel(transport, {noPing: true, objectMode: true, timeout: 5})
        .on('eot', () => { done(); });
      channel.ping((err) => {
        assert(/timeout/.test(err), err);
        channel.destroy();
      });
    });

    test('readable ended', (done) => {
      let svc = Service.forProtocol({
        protocol: 'Ping',
        messages: {ping: {request: [], response: 'boolean'}}
      });
      let transports = createPassthroughTransports();
      svc.createClient()
        .createChannel(transports[0])
        .on('eot', () => { done(); });
      transports[0].readable.push(null);
    });

    test('writable finished', (done) => {
      let svc = Service.forProtocol({
        protocol: 'Ping',
        messages: {ping: {request: [], response: 'boolean'}}
      });
      // We must use object mode here since ending the encoding stream won't
      // end the underlying writable stream.
      let transports = createPassthroughTransports(true);
      svc.createClient()
        .createChannel(transports[0], {noPing: true, objectMode: true})
        .on('eot', () => { done(); });
      transports[0].writable.end();
    });

    test('keep writable open', (done) => {
      let svc = Service.forProtocol({
        protocol: 'Ping',
        messages: {ping: {request: [], response: 'boolean'}}
      });
      // We must use object mode here since ending the encoding stream won't
      // end the underlying writable stream.
      let transports = createPassthroughTransports(true);
      svc.createClient()
        .createChannel(transports[0], {objectMode: true, endWritable: false})
        .on('eot', () => {
          transports[0].writable.write({}); // Doesn't fail.
          done();
        })
        .destroy();
    });

    test('discover service', (done) => {
      // Check that we can interrupt a handshake part-way, so that we can ping
      // a remote server for its service, but still reuse the same connection
      // for a later trasnmission.
      let svc1 = Service.forProtocol({protocol: 'Empty'});
      let svc2 = Service.forProtocol({
        protocol: 'Ping',
        messages: {ping: {request: [], response: 'boolean'}}
      });

      let transports = createPassthroughTransports();
      let server2 = svc2.createServer()
        .onPing((cb) => { cb(null, true); });
      let chn1 = server2.createChannel(transports[0]);
      assert.strictEqual(chn1.getProtocol(), svc2); // Deprecated.
      assert.strictEqual(chn1.server.service, svc2);
      svc1.createClient()
        .createChannel(transports[1], {endWritable: false})
        .on('handshake', function (hreq, hres) {
          this.destroy();
          assert.equal(hres.serverProtocol, JSON.stringify(svc2.protocol));
        })
        .on('eot', () => {
          // The transports are still available for a connection.
          let client = svc2.createClient();
          let chn2 = client.createChannel(transports[1]);
          client.ping((err, res) => {
            assert.strictEqual(err, null);
            assert.strictEqual(res, true);
            chn2.on('eot', () => { done(); }).destroy();
          });
        });
    });

    test('trailing decoder', (done) => {
      let svc = Service.forProtocol({
        protocol: 'Ping',
        messages: {ping: {request: [], response: 'boolean'}}
      });
      let transports = createPassthroughTransports();
      svc.createClient()
        .createChannel(transports[0], {noPing: true})
        .on('eot', (pending, err) => {
          assert.equal(pending, 0);
          assert(/trailing/.test(err), err);
          done();
        });
      transports[0].readable.end(utils.bufferFrom([48]));
    });
  });

  suite('StatelessClientChannel', () => {

    test('factory error', (done) => {
      let svc = Service.forProtocol({
        protocol: 'Ping',
        messages: {ping: {request: [], response: 'boolean'}}
      }, {wrapUnions: true});
      let client = svc.createClient({strictTypes: true});
      let chn = client.createChannel((cb) => {
        return new stream.PassThrough({objectMode: true})
          .on('finish', () => { cb(new Error('foobar')); });
      }, {noPing: true, objectMode: true});
      client.ping((err) => {
        assert(/foobar/.test(err.string), err);
        assert(!chn.destroyed);
        assert(!chn.isDestroyed()); // Deprecated.
        done();
      });
    });

    test('factory error no writable', (done) => {
      let svc = Service.forProtocol({
        protocol: 'Ping',
        messages: {ping: {request: [], response: 'boolean'}}
      }, {wrapUnions: true});
      let client = svc.createClient();
      let chn = client.createChannel(() => {}, {noPing: true});
      client.ping((err) => {
        assert(/invalid writable stream/.test(err), err);
        assert(!chn.destroyed);
        assert(!chn.isDestroyed()); // Deprecated.
        done();
      });
    });

    test('trailing data', (done) => {
      let svc = Service.forProtocol({
        protocol: 'Ping',
        messages: {ping: {request: [], response: 'boolean'}}
      }, {wrapUnions: true});
      let client = svc.createClient();
      let readable = new stream.PassThrough();
      let sawError = false;
      let chn = client.createChannel((cb) => {
        cb(null, readable);
        return new stream.PassThrough();
      }, {noPing: true})
        .on('eot', (pending, err) => {
          assert(/trailing/.test(err), err);
          sawError = true;
        });
      client.ping((err) => {
        assert(/interrupted/.test(err), err);
        assert(chn.destroyed);
        assert(sawError);
        done();
      });
      readable.end(utils.bufferFrom([48]));
    });

    test('default encoder error', (done) => {
      let svc = Service.forProtocol({
        protocol: 'Ping',
        messages: {ping: {request: [], response: 'boolean'}}
      }, {wrapUnions: true});
      let client = svc.createClient({strictTypes: true});
      let chn = client.createChannel((cb) => {
        return new stream.PassThrough()
          .on('finish', () => { cb(new Error('foobar')); });
      }, {noPing: true});
      client.ping((err) => {
        assert(/foobar/.test(err.string));
        assert(!chn.destroyed);
        done();
      });
    });

    test('reuse writable', (done) => {
      let svc = Service.forProtocol({
        protocol: 'Ping',
        messages: {ping: {request: [], response: 'null'}}
      });
      let readable = new stream.PassThrough({objectMode: true});
      let writable = new stream.PassThrough({objectMode: true})
        .on('data', (data) => {
          // Encoded handshake response.
          let hres = utils.bufferFrom([0, 0, 0, 0]);
          // Encoded response (flag and meta).
          let res = utils.bufferFrom([0, 0]);
          readable.write({id: data.id, payload: [hres, res]});
        });
      let client = svc.createClient();
      client.createChannel((cb) => {
        cb(null, readable);
        return writable;
      }, {noPing: true, objectMode: true, endWritable: false});
      client.ping((err) => {
        assert(!err, err);
        client.ping((err) => {
          assert(!err, err); // We can reuse it.
          done();
        });
      });
    });

    test('invalid handshake response', (done) => {
      let svc = Service.forProtocol({
        protocol: 'Ping',
        messages: {ping: {request: [], response: 'null'}}
      });
      let readable = new stream.PassThrough({objectMode: true});
      let writable = new stream.PassThrough({objectMode: true})
        .on('data', (data) => {
          let buf = utils.bufferFrom([0, 0, 0, 2, 48]);
          readable.write({id: data.id, payload: [buf]});
        });
      let client = svc.createClient();
      client.createChannel((cb) => {
        cb(null, readable);
        return writable;
      }, {noPing: true, objectMode: true, endWritable: false});
      client.ping((err) => {
        assert(/truncated.*HandshakeResponse/.test(err), err);
        done();
      });
    });

    test('interrupt writable', (done) => {
      let svc = Service.forProtocol({
        protocol: 'Ping',
        messages: {ping: {request: [], response: 'null'}}
      });
      // Fake handshake response.
      let hres = services.HANDSHAKE_RESPONSE_TYPE.clone({
        match: 'NONE',
        serverService: JSON.stringify(svc.protocol),
        serverHash: svc.hash
      });
      let readable = new stream.PassThrough({objectMode: true});
      let writable = new stream.PassThrough({objectMode: true})
        .on('data', (data) => {
          readable.write({id: data.id, payload: [hres.toBuffer()]});
        });
      let numHandshakes = 0;
      let client = svc.createClient();
      client.createChannel((cb) => {
        cb(null, readable);
        return writable;
      }, {objectMode: true}).on('handshake', function (hreq, actualHres) {
        numHandshakes++;
        assert.deepEqual(actualHres, hres);
        this.destroy(true);
      }).on('error', (err) => {
        assert(/interrupted/.test(err), err);
      });
      setTimeout(() => {
        // Only a single handshake should have occurred.
        assert.equal(numHandshakes, 1);
        done();
      }, 50);
    });
  });

  suite('StatefulServerChannel', () => {

    test('readable ended', (done) => {
      let svc = Service.forProtocol({
        protocol: 'Ping',
        messages: {ping: {request: [], response: 'boolean'}}
      });
      let transport = new stream.PassThrough();
      svc.createServer().createChannel(transport).on('eot', function () {
        assert(this.destroyed);
        assert(this.isDestroyed()); // Deprecated.
        done();
      });
      transport.push(null);
    });

    test('readable trailing data', (done) => {
      let svc = Service.forProtocol({
        protocol: 'Ping',
        messages: {ping: {request: [], response: 'boolean'}}
      });
      let transport = new stream.PassThrough();
      svc.createServer().createChannel(transport)
        .on('eot', function (pending, err) {
          assert(/trailing/.test(err), err);
          assert(this.destroyed);
          assert(this.isDestroyed()); // Deprecated.
          done();
        });
      transport.end(utils.bufferFrom([48]));
    });

    test('writable finished', (done) => {
      let svc = Service.forProtocol({
        protocol: 'Ping',
        messages: {ping: {request: [], response: 'boolean'}}
      });
      // We must use object mode here since ending the encoding stream won't
      // end the underlying writable stream.
      let transports = createPassthroughTransports(true);
      svc.createServer().createChannel(transports[0], {objectMode: true})
        .on('eot', () => { done(); });
      transports[0].writable.end();
    });
  });

  suite('StatelessServerChannel', () => {

    test('factory error', (done) => {
      let svc = Service.forProtocol({
        protocol: 'Ping',
        messages: {ping: {request: [], response: 'boolean'}}
      });
      svc.createServer({silent: true}).createChannel((cb) => {
        cb(new Error('bar'));
        return new stream.PassThrough();
      }).on('eot', (pending, err) => {
        assert(/bar/.test(err), err);
        done();
      });
    });

    test('trailing data', (done) => {
      let svc = Service.forProtocol({
        protocol: 'Ping',
        messages: {ping: {request: [], response: 'boolean', errors: ['int']}}
      });
      let transports = createPassthroughTransports();
      svc.createServer({silent: true}).createChannel((cb) => {
        cb(null, transports[0].writable);
        return transports[1].readable;
      }).on('eot', (pending, err) => {
        assert(/trailing/.test(err), err);
        done();
      });
      transports[1].readable.end(utils.bufferFrom([48]));
    });

    test('delayed writable', (done) => {
      let svc = Service.forProtocol({
        protocol: 'Ping',
        messages: {ping: {request: [], response: 'boolean', errors: ['int']}}
      });
      let objs = [];
      let readable = new stream.PassThrough({objectMode: true});
      let writable = new stream.PassThrough({objectMode: true})
        .on('data', (obj) => { objs.push(obj); });
      svc.createServer({silent: true}).createChannel((cb) => {
        setTimeout(() => { cb(null, writable); }, 50);
        return readable;
      }, {objectMode: true}).on('eot', () => {
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

    test('reuse writable', (done) => {
      let svc = Service.forProtocol({
        protocol: 'Ping',
        messages: {ping: {request: [], response: 'null'}}
      }).on('ping', (req, ee, cb) => {
        cb(null, null);
      });
      let payload = [
        services.HANDSHAKE_REQUEST_TYPE.toBuffer({
          clientHash: svc.hash,
          serverHash: svc.hash
        }),
        utils.bufferFrom('\x00\x08ping')
      ];
      let objs = [];
      let readable = new stream.PassThrough({objectMode: true});
      let writable = new stream.PassThrough({objectMode: true})
        .on('data', (obj) => { objs.push(obj); });
      svc.createServer({silent: true}).createChannel((cb) => {
        cb(null, writable);
        return readable;
      }, {endWritable: false, noPing: true, objectMode: true})
        .on('eot', () => {
          assert.deepEqual(objs.length, 2);
          done();
        });
      readable.write({id: 0, payload: payload});
      readable.end({id: 1, payload: payload});
    });
  });

  suite('emitters & listeners', () => { // <5.0 API.

    suite('stateful', () => {

      run((emitterPtcl, listenerPtcl, opts, cb) => {
        if (!cb) {
          cb = opts;
          opts = undefined;
        }
        let pt1 = new stream.PassThrough();
        let pt2 = new stream.PassThrough();
        cb(
          emitterPtcl.createEmitter({readable: pt1, writable: pt2}, opts),
          listenerPtcl.createListener({readable: pt2, writable: pt1}, opts)
        );
      });

      test('explicit server fingerprint', (done) => {
        let transports = createPassthroughTransports();
        let p1 = Service.forProtocol({
          protocol: 'Math',
          messages: {
            negate: {
              request: [{name: 'n', type: 'int'}],
              response: 'long'
            }
          }
        });
        let p2 = Service.forProtocol({
          protocol: 'Math',
          messages: {
            negate: {
              request: [{name: 'n', type: 'long'}],
              response: 'int'
            }
          }
        });
        let ml1 = p2.createListener(transports[0]);
        let me1 = p1.createEmitter(transports[1]);
        me1.on('handshake', (hreq, hres) => {
          if (hres.match === 'NONE') {
            return;
          }
          // When we reach here a connection has been established, so both
          // emitter and listener caches have been populated with correct
          // adapters.
          let transports = createPassthroughTransports();
          p2.createListener(transports[0], {cache: ml1.getCache()})
            .once('handshake', onHandshake);
          p1.createEmitter(transports[1], {
            cache: me1.getCache(),
            serverHash: p2.hash
          }).once('handshake', onHandshake);

          let n = 0;
          function onHandshake(hreq, hres) {
            // The remote service should be available.
            assert.equal(hres.match, 'BOTH');
            if (++n === 2) {
              done();
            }
          }
        });
      });

      test('cached client fingerprint', (done) => {
        let transports = createPassthroughTransports();
        let p1 = Service.forProtocol({
          protocol: 'Math',
          messages: {
            negate: {
              request: [{name: 'n', type: 'int'}],
              response: 'long'
            }
          }
        });
        let p2 = Service.forProtocol({
          protocol: 'Math',
          messages: {
            negate: {
              request: [{name: 'n', type: 'long'}],
              response: 'int'
            }
          }
        });
        let ml1 = p2.createListener(transports[0]);
        let me1 = p1.createEmitter(transports[1], {timeout: 0});
        me1.on('handshake', (hreq, hres) => {
          if (hres.match === 'NONE') {
            return;
          }
          let transports = createPassthroughTransports();
          // The listener now has the client's service.
          p2.createListener(transports[0], {cache: ml1.getCache()})
            .once('handshake', (hreq, hres) => {
              assert.equal(hres.match, 'CLIENT');
              done();
            });
          p1.createEmitter(transports[1]);
        });
      });

      test('scoped transports', (done) => {
        let transports = createPassthroughTransports();
        let ptcl = Service.forProtocol({
          protocol: 'Case',
          messages: {
            upper: {
              request: [{name: 'str', type: 'string'}],
              response: 'string'
            }
          }
        }).on('upper', (req, ee, cb) => {
          cb(null, req.str.toUpperCase());
        });
        let meA = ptcl.createEmitter(transports[1], {scope: 'a'});
        ptcl.createListener(transports[0], {scope: 'a'});
        let meB = ptcl.createEmitter(transports[0], {scope: 'b'});
        ptcl.createListener(transports[1], {scope: 'b'});
        ptcl.emit('upper', {str: 'hi'}, meA, (err, res) => {
          assert.strictEqual(err, null);
          assert.equal(res, 'HI');
          ptcl.emit('upper', {str: 'hey'}, meB, (err, res) => {
            assert.strictEqual(err, null);
            assert.equal(res, 'HEY');
            done();
          });
        });
      });

    });

    suite('stateless', () => {

      run((emitterPtcl, listenerPtcl, opts, cb) => {
        if (!cb) {
          cb = opts;
          opts = undefined;
        }
        cb(emitterPtcl.createEmitter(writableFactory, opts));

        function writableFactory(emitterCb) {
          let reqPt = new stream.PassThrough()
            .on('finish', () => {
              listenerPtcl.createListener((listenerCb) => {
                let resPt = new stream.PassThrough()
                  .on('finish', () => { emitterCb(null, resPt); });
                listenerCb(null, resPt);
                return reqPt;
              }, opts);
            });
          return reqPt;
        }
      });

    });

    function run(setupFn) {

      test('primitive types', (done) => {
        let ptcl = Service.forProtocol({
          protocol: 'Math',
          messages: {
            negate: {
              request: [{name: 'n', type: 'int'}],
              response: 'long'
            }
          }
        });
        setupFn(ptcl, ptcl, (ee) => {
          ptcl.on('negate', (req, ee, cb) => { cb(null, -req.n); });
          let n1, n2;
          ee.on('eot', () => {
            assert.equal(n1, 1);
            assert.equal(n2, 1);
            done();
          }).once('handshake', (hreq, hres) => {
            // Allow the initial ping to complete.
            assert.equal(hres.match, 'BOTH');
            setTimeout(() => {
              // Also let the pending count go down.
              n1 = ptcl.emit('negate', {n: 20}, ee, function (err, res) {
                assert.equal(this, ptcl);
                assert.strictEqual(err, null);
                assert.equal(res, -20);
                n2 = this.emit('negate', {n: 'hi'}, ee, (err) => {
                  assert(/invalid "negate" request/.test(err), err);
                  process.nextTick(() => { ee.destroy(); });
                });
              });
            }, 0);
          });
        });
      });

      test('emit receive error', (done) => {
        let ptcl = Service.forProtocol({
          protocol: 'Math',
          messages: {
            negate: {
              request: [{name: 'n', type: 'int'}],
              response: 'long',
              errors: [{type: 'map', values: 'string'}]
            }
          }
        });
        setupFn(ptcl, ptcl, (ee) => {
          ee.on('eot', () => { done(); });
          ptcl.on('negate', (req, ee, cb) => { cb({rate: '23'}); });
          ptcl.emit('negate', {n: 20}, ee, function (err) {
            assert.equal(this, ptcl);
            assert.deepEqual(err, {rate: '23'});
            ee.destroy();
          });
        });
      });

      test('complex type', (done) => {
        let ptcl = Service.forProtocol({
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
        setupFn(ptcl, ptcl, (ee) => {
          let type = ptcl.getType('N');
          ee.on('eot', () => { done(); });
          ptcl.on('generate', (req, ee, cb) => {
            let letters = [];
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

      test('invalid request', (done) => {
        let ptcl = Service.forProtocol({
          protocol: 'Math',
          messages: {
            negate: {
              request: [{name: 'n', type: 'int'}],
              response: 'int'
            }
          }
        }).on('negate', () => { assert(false); });
        setupFn(ptcl, ptcl, (ee) => {
          ee.on('eot', () => { done(); });
          ptcl.emit('negate', {n: 'a'}, ee, (err) => {
            assert(/invalid "negate" request/.test(err), err);
            ee.destroy();
          });
        });
      });

      test('error response', (done) => {
        let ptcl = Service.forProtocol({
          protocol: 'Math',
          messages: {
            sqrt: {
              request: [{name: 'n', type: 'float'}],
              response: 'float'
            }
          }
        }).on('sqrt', (req, ee, cb) => {
          let n = req.n;
          if (n < 0) {
            cb(new Error('must be non-negative'));
          } else {
            cb(null, Math.sqrt(n));
          }
        });
        setupFn(ptcl, ptcl, (ee) => {
          ptcl.emit('sqrt', {n: 100}, ee, (err, res) => {
            assert(Math.abs(res - 10) < 1e-5);
            ptcl.emit('sqrt', {n: - 10}, ee, function (err) {
              assert.equal(this, ptcl);
              assert(/must be non-negative/.test(err.message));
              done();
            });
          });
        });
      });

      test('wrapped error response', (done) => {
        let ptcl = Service.forProtocol({
          protocol: 'Math',
          messages: {
            sqrt: {
              request: [{name: 'n', type: 'float'}],
              response: 'null',
              errors: ['float']
            }
          }
        }, {wrapUnions: true}).on('sqrt', (req, ee, cb) => {
          let n = req.n;
          if (n < 0) {
            cb(new Error('must be non-negative'));
          } else {
            cb({float: Math.sqrt(n)});
          }
        });
        setupFn(ptcl, ptcl, (ee) => {
          ptcl.emit('sqrt', {n: -10}, ee, (err) => {
            assert(/must be non-negative/.test(err.message), err);
            ptcl.emit('sqrt', {n: 100}, ee, (err) => {
              assert(Math.abs(err.float - 10) < 1e-5);
              done();
            });
          });
        });
      });

      test('wrapped remote service', (done) => {
        let ptcl1 = Service.forProtocol({
          protocol: 'Math',
          messages: {
            invert: {
              request: [{name: 'n', type: ['int', 'float']}],
              response: ['int', 'float']
            }
          }
        }, {wrapUnions: true});
        let ptcl2 = Service.forProtocol({
          protocol: 'Math',
          messages: {
            invert: {
              request: [{name: 'n', type: ['int', 'float']}],
              response: ['float', 'int']
            }
          }
        }, {wrapUnions: true}).on('invert', (req, ee, cb) => {
          if (req.n.int) {
            cb(null, {float: 1 / req.n.int});
          } else {
            cb(null, {int: (1 / req.n.float) | 0});
          }
        });
        setupFn(ptcl1, ptcl2, (ee) => {
          ptcl1.emit('invert', {n: {int: 10}}, ee, (err, res) => {
            assert(Math.abs(res.float - 0.1) < 1e-5);
            ptcl1.emit('invert', {n: {float: 10}}, ee, (err, res) => {
              assert.equal(res.int, 0);
              done();
            });
          });
        });
      });

      test('invalid response', (done) => {
        let ptcl = Service.forProtocol({
          protocol: 'Math',
          messages: {
            sqrt: {
              request: [{name: 'n', type: 'float'}],
              response: 'float'
            }
          }
        }).on('sqrt', (req, ee, cb) => {
          let n = req.n;
          if (n < 0) {
            cb(null, 'complex'); // Invalid response.
          } else if (n === 0) {
            cb(new Error('zero!')); // Ok error response.
          } else {
            cb(null, Math.sqrt(n));
          }
        });
        setupFn(ptcl, ptcl, (ee) => {
          ptcl.emit('sqrt', {n: - 10}, ee, (err) => {
            assert(/internal server error/.test(err), err);
            ptcl.emit('sqrt', {n: 0}, ee, (err) => {
              assert(/zero!/.test(err.message));
              ptcl.emit('sqrt', {n: 100}, ee, (err, res) => {
                // And the server doesn't die (we can make a new request).
                assert(Math.abs(res - 10) < 1e-5);
                done();
              });
            });
          });
        });
      });

      test('out of order', (done) => {
        let ptcl = Service.forProtocol({
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
        }).on('w', (req, ee, cb) => {
          let delay = req.ms;
          if (delay < 0) {
            cb('delay must be non-negative');
            return;
          }
          setTimeout(() => { cb(null, req.id); }, delay);
        });
        let ids = [];
        setupFn(ptcl, ptcl, (ee) => {
          let n1, n2, n3;
          ee.on('eot', (pending) => {
            assert.equal(pending, 0);
            assert.equal(n1, 1);
            assert.equal(n2, 2);
            assert.equal(n3, 3);
            assert.deepEqual(ids, [undefined, 'b', 'a']);
            done();
          }).once('handshake', (hreq, hres) => {
            assert.equal(hres.match, 'BOTH');
            process.nextTick(() => {
              n1 = ptcl.emit('w', {ms: 500, id: 'a'}, ee, (err, res) => {
                assert.strictEqual(err, null);
                ids.push(res);
              });
              n2 = ptcl.emit('w', {ms: 10, id: 'b'}, ee, (err, res) => {
                assert.strictEqual(err, null);
                ids.push(res);
                ee.destroy();
              });
              n3 = ptcl.emit('w', {ms: -10, id: 'c'}, ee, (err, res) => {
                assert(/non-negative/.test(err));
                ids.push(res);
              });
            });
          });
        });
      });

      test('compatible services', (done) => {
        let emitterPtcl = Service.forProtocol({
          protocol: 'emitterService',
          messages: {
            age: {
              request: [{name: 'name', type: 'string'}],
              response: 'long'
            }
          }
        });
        let listenerPtcl = Service.forProtocol({
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
          (ee) => {
            listenerPtcl.on('age', (req, ee, cb) => {
              assert.equal(req.name, 'Ann');
              cb(null, 23);
            });
            emitterPtcl.emit('age', {name: 'Ann'}, ee, (err, res) => {
              assert.strictEqual(err, null);
              assert.equal(res, 23);
              done();
            });
          }
        );
      });

      test('compatible service with a complex type', (done) => {
        let ptcl1 = Service.forProtocol({
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
        let ptcl2 = Service.forProtocol({
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
        setupFn(ptcl1, ptcl2, (ee) => {
          let type = ptcl2.getType('N2');
          ee.on('eot', () => { done(); });
          ptcl2.on('generate', (req, ee, cb) => {
            let letters = [];
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

      test('cached compatible services', (done) => {
        let ptcl1 = Service.forProtocol({
          protocol: 'emitterService',
          messages: {
            age: {
              request: [{name: 'name', type: 'string'}],
              response: 'long'
            }
          }
        });
        let ptcl2 = Service.forProtocol({
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
        }).on('age', (req, ee, cb) => { cb(null, 48); });
        setupFn(
          ptcl1,
          ptcl2,
          (ee1) => {
            ptcl1.emit('age', {name: 'Ann'}, ee1, (err, res) => {
              assert.equal(res, 48);
              setupFn(
                ptcl1,
                ptcl2,
                (ee2) => { // ee2 has the server's service.
                  ptcl1.emit('age', {name: 'Bob'}, ee2, (err, res) => {
                    assert.equal(res, 48);
                    done();
                  });
                }
              );
            });
          }
        );
      });

      test('incompatible services missing message', (done) => {
        let emitterPtcl = Service.forProtocol({
          protocol: 'emitterService',
          messages: {
            age: {request: [{name: 'name', type: 'string'}], response: 'long'}
          }
        }, {wrapUnions: true});
        let listenerPtcl = Service.forProtocol({protocol: 'serverService'});
        setupFn(
          emitterPtcl,
          listenerPtcl,
          (ee) => {
            ee.on('error', () => {}); // For stateful services.
            emitterPtcl.emit('age', {name: 'Ann'}, ee, (err) => {
              assert(err.message);
              done();
            });
          }
        );
      });

      test('incompatible services', (done) => {
        let emitterPtcl = Service.forProtocol({
          protocol: 'emitterService',
          messages: {
            age: {request: [{name: 'name', type: 'string'}], response: 'long'}
          }
        }, {wrapUnions: true});
        let listenerPtcl = Service.forProtocol({
          protocol: 'serverService',
          messages: {
            age: {request: [{name: 'name', type: 'int'}], response: 'long'}
          }
        }).on('age', (req, ee, cb) => { cb(null, 0); });
        setupFn(
          emitterPtcl,
          listenerPtcl,
          (ee) => {
            ee.on('error', () => {}); // For stateful services.
            emitterPtcl.emit('age', {name: 'Ann'}, ee, (err) => {
              assert(err.message);
              done();
            });
          }
        );
      });

      test('incompatible services one way message', (done) => {
        let ptcl1 = Service.forProtocol({
          protocol: 'ptcl1',
          messages: {ping: {request: [], response: 'null', 'one-way': true}}
        });
        let ptcl2 = Service.forProtocol({
          protocol: 'ptcl2',
          messages: {ping: {request: [], response: 'null'}}
        });
        setupFn(ptcl1, ptcl2, (ee) => {
          ee.on('error', function (err) {
            // This will be called twice for stateful emitters: once when
            // interrupted, then for the incompatible service error.
            assert(err);
            this.destroy();
          }).on('eot', () => { done(); });
          ptcl1.emit('ping', {}, ee);
        });
      });

      test('one way message', (done) => {
        let ptcl = Service.forProtocol({
          protocol: 'ptcl',
          messages: {ping: {request: [], response: 'null', 'one-way': true}}
        });
        setupFn(ptcl, ptcl, (ee) => {
          ptcl.on('ping', (req, ee, cb) => {
            assert.strictEqual(cb, undefined);
            done();
          });
          ptcl.emit('ping', {}, ee);
        });
      });

      test('ignored response', (done) => {
        let ptcl = Service.forProtocol({
          protocol: 'ptcl',
          messages: {ping: {request: [], response: 'null'}} // Not one-way.
        });
        setupFn(ptcl, ptcl, (ee) => {
          ptcl.on('ping', (req, ee, cb) => {
            cb(null, null);
            done();
          });
          ptcl.emit('ping', {}, ee);
        });
      });

      test('duplicate message callback', (done) => {
        let ptcl = Service.forProtocol({
          protocol: 'ptcl',
          messages: {ping: {request: [], response: 'null'}} // Not one-way.
        });
        setupFn(ptcl, ptcl, (ee) => {
          ptcl.on('ping', (req, ee, cb) => {
            // In reality the server wouldn't be used (since this is the old
            // API), but this makes this test do its job.
            ee.server.on('error', (err) => {
              assert(/duplicate handler call/.test(err), err);
              done();
            });
            cb(null, null);
            cb(null, null);
          });
          ptcl.emit('ping', {}, ee); // No error on the emitter side.
        });
      });

      test('unknown message', (done) => {
        let ptcl = Service.forProtocol({protocol: 'Empty'});
        setupFn(ptcl, ptcl, (ee) => {
          assert.throws(
            () => { ptcl.emit('echo', {}, ee); },
            /unknown message/
          );
          done();
        });
      });

      test('unhandled message', (done) => {
        let ptcl = Service.forProtocol({
          protocol: 'Echo',
          messages: {
            echo: {
              request: [{name: 'id', type: 'string'}],
              response: 'string'
            },
            ping: {request: [], response: 'null', 'one-way': true}
          }
        });
        setupFn(ptcl, ptcl, (ee) => {
          ptcl.emit('echo', {id: ''}, ee, (err) => {
            assert(/not implemented/.test(err), err);
            done();
          });
        });
      });

      test('destroy emitter noWait', (done) => {
        let ptcl = Service.forProtocol({
          protocol: 'Delay',
          messages: {
            wait: {
              request: [{name: 'ms', type: 'int'}],
              response: 'string'
            }
          }
        }).on('wait', (req, ee, cb) => {
          setTimeout(() => { cb(null, 'ok'); }, req.ms);
        });
        let interrupted = 0;
        setupFn(ptcl, ptcl, (ee) => {
          ee.on('eot', (pending) => {
            assert.equal(pending, 2);
            setTimeout(() => {
              assert.equal(interrupted, 2);
              done();
            }, 5);
          });
          ptcl.emit('wait', {ms: 75}, ee, interruptedCb);
          ptcl.emit('wait', {ms: 50}, ee, interruptedCb);
          ptcl.emit('wait', {ms: 10}, ee, (err, res) => {
            assert.equal(res, 'ok');
            ee.destroy(true);
          });

          function interruptedCb(err) {
            assert(/interrupted/.test(err.message));
            interrupted++;
          }
        });
      });

      test('destroy emitter', (done) => {
        let ptcl = Service.forProtocol({
          protocol: 'Math',
          messages: {
            negate: {
              request: [{name: 'n', type: 'int'}],
              response: 'int'
            }
          }
        });
        setupFn(ptcl, ptcl, (ee) => {
          ptcl.on('negate', (req, ee, cb) => { cb(null, -req.n); });
          ptcl.emit('negate', {n: 20}, ee, function (err, res) {
            assert.strictEqual(err, null);
            assert.equal(res, -20);
            assert.strictEqual(ee.getProtocol(), ptcl);
            ee.destroy();
            this.emit('negate', {n: 'hi'}, ee, (err) => {
              assert(/no active channels/.test(err.message), err);
              done();
            });
          });
        });
      });


      test('catch server error', (done) => {
        let ptcl = Service.forProtocol({
          protocol: 'Math',
          messages: {
            error1: {request: [], response: 'null'},
            negate: {
              request: [{name: 'n', type: 'int'}],
              response: 'int'
            }
          }
        });
        setupFn(ptcl, ptcl, (ee) => {
          ptcl
            .on('error1', () => { throw new Error('foobar'); })
            .on('negate', (req, ee, cb) => { cb(null, -req.n); })
            .emit('error1', {}, ee, function (err) {
              assert(/internal server error/.test(err), err);
              // But the server doesn't die.
              this.emit('negate', {n: 20}, ee, (err, res) => {
                assert.strictEqual(err, null);
                assert.equal(res, -20);
                done();
              });
            });
        });
      });

    }
  });

  suite('Client', () => {

    test('no emitters without buffering', (done) => {
      let svc = Service.forProtocol({
        protocol: 'Ping',
        messages: {ping: {request: [], response: 'boolean'}}
      });
      let client = svc.createClient({buffering: false})
        .on('error', (err) => {
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

    test('destroy emitters', (done) => {
      let svc = Service.forProtocol({
        protocol: 'Ping',
        messages: {ping: {request: [], response: 'boolean'}}
      });
      let transport = {
        readable: new stream.PassThrough(),
        writable: new stream.PassThrough()
      };
      let client = svc.createClient();
      client.createChannel(transport)
        .on('eot', () => {
          done();
        });
      client.destroyChannels({noWait: true});
    });

    test('default policy', (done) => {
      let svc = Service.forProtocol({
        protocol: 'Ping',
        messages: {ping: {request: [], response: 'null', 'one-way': true}}
      });
      let transport = {
        readable: new stream.PassThrough(),
        writable: new stream.PassThrough()
      };
      let client = svc.createClient();
      client.createChannel(transport, {noPing: true});
      client.createChannel(transport, {noPing: true});
      client.ping(function (err) {
        assert(!err, err);
        assert.strictEqual(this.channel.client, client);
        done();
      });
    });

    test('custom policy', (done) => {
      let svc = Service.forProtocol({
        protocol: 'Ping',
        messages: {ping: {request: [], response: 'boolean'}}
      });
      let transport = {
        readable: new stream.PassThrough(),
        writable: new stream.PassThrough()
      };

      let client = svc.createClient({channelPolicy: policy});
      let channels = [
        client.createChannel(transport, {noPing: true}),
        client.createChannel(transport, {noPing: true})
      ];
      client.ping();

      function policy(channels_) {
        assert.deepEqual(channels_, channels);
        done();
      }
    });

    test('remote protocols existing', () => {
      let ptcl1 = Service.forProtocol({protocol: 'Empty1'});
      let ptcl2 = Service.forProtocol({protocol: 'Empty2'});
      let remotePtcls = {abc: ptcl2.protocol};
      let client = ptcl1.createClient({
        remoteFingerprint: 'abc',
        remoteProtocols: remotePtcls
      });
      assert.deepEqual(client.remoteProtocols(), remotePtcls);
    });

    test('invalid response', (done) => {
      let svc = Service.forProtocol({
        protocol: 'Ping',
        messages: {ping: {request: [], response: 'int'}}
      });
      let opts = {noPing: true, objectMode: true};
      let transport = {
        readable: new stream.PassThrough(opts),
        writable: new stream.PassThrough(opts)
      };
      let client = svc.createClient();
      client.createChannel(transport, opts);
      client.ping((err) => {
        assert(/truncated/.test(err), err);
        done();
      });
      setTimeout(() => {
        // "Send" an invalid payload (negative union offset). We wait to allow
        // the callback for the above message to be registered.
        transport.readable.write({id: 1, payload: [utils.bufferFrom([45])]});
      }, 0);
    });
  });

  suite('Server', () => {

    test('get channels', (done) => {
      let svc = Service.forProtocol({protocol: 'Empty1'});
      let server = svc.createServer();
      let transport = {
        readable: new stream.PassThrough(),
        writable: new stream.PassThrough()
      };
      let channels = [
        server.createChannel(transport),
        server.createChannel(transport)
      ];
      assert.deepEqual(server.activeChannels(), channels);
      channels[0]
        .on('eot', () => {
          assert.deepEqual(server.activeChannels(), [channels[1]]);
          done();
        })
        .destroy();
    });

    test('remote protocols', () => {
      let svc1 = Service.forProtocol({protocol: 'Empty1'});
      let svc2 = Service.forProtocol({protocol: 'Empty2'});
      let remotePtcls = {abc: svc2.protocol};
      let server = svc1.createServer({remoteProtocols: remotePtcls});
      assert.deepEqual(server.remoteProtocols(), remotePtcls);
    });

    test('no capitalization', () => {
      let svc = Service.forProtocol({
        protocol: 'Ping',
        messages: {ping: {request: [], response: 'boolean'}}
      });
      let server = svc.createServer({noCapitalize: true});
      assert(!server.onPing);
      assert(typeof server.onping == 'function');
    });

    test('stateful transport reuse', (done) => {
      let svc = Service.forProtocol({
        protocol: 'Ping',
        messages: {ping: {request: [], response: 'int'}}
      });
      let transports = createPassthroughTransports(true);
      let server = svc.createServer()
        .onPing((cb) => {
          cb(null, 1);
        });
      let channel = server.createChannel(transports[0], {endWritable: false});
      let client = svc.createClient()
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
                this.ping((err, n) => {
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

    test('interrupted', (done) => {
      let svc = Service.forProtocol({
        protocol: 'Ping',
        messages: {ping: {request: [], response: 'int'}}
      });
      let server = svc.createServer()
        .onPing(function (cb) {
          this.channel.destroy(true);
          cb(null, 1); // Still call the callback to make sure it is ignored.
        });
      svc.createClient({server: server})
        .once('channel', function () {
          this.ping({timeout: 10}, (err) => {
            assert(/interrupted/.test(err), err);
            done();
          });
        });
    });
  });

  suite('clients & servers', () => { // >=5.0 API.

    test('create client with server', (done) => {
      let svc = Service.forProtocol({
        protocol: 'Echo',
        messages: {
          echo: {request: [{name: 'n', type: 'int'}], response: 'int'}
        }
      });
      let server = svc.createServer()
        .onEcho((n, cb) => { cb(null, n); });
      svc.createClient({buffering: true, server: server})
        .echo(123, (err, n) => {
          assert(!err, err);
          assert.equal(n, 123);
          done();
        });
    });

    test('client context call options', (done) => {
      let svc = Service.forProtocol({
        protocol: 'Math',
        messages: {
          neg: {request: [{name: 'n', type: 'int'}], response: 'int'}
        }
      });
      let server = svc.createServer()
        .onNeg((n, cb) => { cb(null, -n); });
      let opts = {id: 123};
      let client = svc.createClient({server: server})
        .once('channel', (channel) => {
          channel.on('outgoingCall', (ctx, opts) => {
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

    test('server call constant context', (done) => {
      let svc = Service.forProtocol({
        protocol: 'Math',
        messages: {
          neg: {request: [{name: 'n', type: 'int'}], response: 'int'}
        }
      });
      let numCalls = 0;
      let server = svc.createServer()
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
        .neg(1, (err, n) => {
          assert(!err, err);
          assert.equal(n, -1);
          assert.equal(numCalls, 3);
          done();
        });
    });

    test('server call context options', (done) => {
      let svc = Service.forProtocol({
        protocol: 'Math',
        messages: {
          neg: {request: [{name: 'n', type: 'int'}], response: 'int'}
        }
      });
      let locals = {num: 123};
      let server = svc.createServer()
        .on('channel', (channel) => {
          channel.on('incomingCall', (ctx) => {
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
          this.neg(1, (err, n) => {
            assert(!err, err);
            assert.equal(n, -1);
            done();
          });
        });
    });

    test('server default handler', (done) => {
      let svc = Service.forProtocol({
        protocol: 'Math',
        messages: {
          neg: {request: [{name: 'n', type: 'int'}], response: 'int'},
          abs: {request: [{name: 'n', type: 'int'}], response: 'int'}
        }
      });
      let server = svc.createServer({defaultHandler: defaultHandler})
        .onNeg((n, cb) => { cb(null, -n); });

      svc.createClient({server: server})
        .once('channel', function () {
          this.neg(1, function (err, n) {
            assert(!err, err);
            assert.equal(n, -1);
            this.channel.client.abs(5, (err, n) => {
              assert(!err, err);
              assert.equal(n, 10);
              done();
            });
          });
        });

      function defaultHandler(wreq, wres, cb) {
        assert.equal(this.message.name, 'abs');
        wres.response = 10;
        cb();
      }
    });

    test('client middleware bypass', (done) => {
      let svc = Service.forProtocol({
        protocol: 'Math',
        messages: {
          neg: {request: [{name: 'n', type: 'int'}], response: 'int'}
        }
      });
      let server = svc.createServer()
        .onNeg((n, cb) => { cb(null, -n); });
      let isCalled = false;
      svc.createClient({server: server})
        .use((wreq, wres, next) => {
          wres.response = -3;
          next();
        })
        .use((wreq, wres, next) => {
          isCalled = true;
          next();
        })
        .once('channel', function () {
          this.neg(1, (err, n) => {
            assert(!err, err);
            assert.equal(n, -3);
            assert(!isCalled);
            done();
          });
        });
    });

    test('client middleware override error', (done) => {
      let svc = Service.forProtocol({
        protocol: 'Math',
        messages: {
          neg: {request: [{name: 'n', type: 'int'}], response: 'int'}
        }
      });
      let server = svc.createServer()
        .onNeg((n, cb) => { cb(null, -n); });
      svc.createClient({buffering: true, server: server})
        .use((wreq, wres, next) => {
          next(null, (err, prev) => {
            assert(/bar/.test(err), err);
            prev(null); // Substitute `null` as error.
          });
        })
        .use((wreq, wres, next) => {
          next(null, (err, prev) => {
            assert(!err, err);
            assert.equal(wres.response, -2);
            prev(new Error('bar'));
          });
        })
        .neg(2, (err) => {
          assert.strictEqual(err, null);
          done();
        });
    });

    test('server middleware bypass', (done) => {
      let svc = Service.forProtocol({
        protocol: 'Math',
        messages: {
          neg: {request: [{name: 'n', type: 'int'}], response: 'int'}
        }
      });
      let handlerCalled = false;
      let errorTriggered = false;
      let server = svc.createServer()
        .on('error', (err) => {
          assert(/foobar/.test(err.cause), err);
          errorTriggered = true;
        })
        .use((wreq, wres, next) => {
          wres.error = 'foobar';
          next();
        })
        .onNeg((n, cb) => {
          handlerCalled = true;
          cb(null, -n);
        });
      svc.createClient({server: server})
        .once('channel', function () {
          this.neg(1, (err) => {
            assert(/foobar/.test(err), err);
            assert(!handlerCalled);
            setTimeout(() => {
              assert(errorTriggered);
              done();
            }, 0);
          });
        });
    });

    test('dynamic middleware', (done) => {
      let svc = Service.forProtocol({
        protocol: 'Math',
        messages: {
          neg: {request: [{name: 'n', type: 'int'}], response: 'int'}
        }
      });
      let server = svc.createServer()
        .use((server) => {
          server.on('channel', (channel) => {
            channel.on('incomingCall', (ctx) => {
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
        .use((client) => {
          client.on('channel', (channel) => {
            channel.on('outgoingCall', (ctx, opts) => {
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

    test('server non-strict error', (done) => {
      let svc = Service.forProtocol({
        protocol: 'Math',
        messages: {
          neg: {request: [{name: 'n', type: 'int'}], response: 'int'}
        }
      });
      let errorTriggered = false;
      let server = svc.createServer()
        .on('error', () => {
          errorTriggered = true;
        })
        .onNeg((n, cb) => {
          cb(null, -n);
        });
      svc.createClient({server: server})
        .once('channel', function () {
          this.neg(1, (err, n) => {
            assert(!err, err);
            assert.equal(n, -1);
            setTimeout(() => {
              assert(!errorTriggered);
              done();
            }, 0);
          });
        });
    });

    test('server one-way middleware error', (done) => {
      let svc = Service.forProtocol({
        protocol: 'Push',
        messages: {
          push: {
            request: [{name: 'n', type: 'int'}],
            response: 'null',
            'one-way': true
          }
        }
      });
      let server = svc.createServer()
        .on('error', (err) => {
          assert(/foobar/.test(err), err);
          done();
        })
        .use((wreq, wres, next) => {
          next(new Error('foobar'));
        });
      svc.createClient({server: server})
        .once('channel', function () { this.push(1); });
    });

    suite('stateful', () => {

      run((clientPtcl, serverPtcl, opts, cb) => {
        if (!cb) {
          cb = opts;
          opts = undefined;
        }
        opts = opts || {};
        opts.silent = true;
        let pt1 = new stream.PassThrough();
        let pt2 = new stream.PassThrough();
        let client = clientPtcl.createClient(opts);
        client.createChannel({readable: pt1, writable: pt2});
        let server = serverPtcl.createServer(opts);
        server.createChannel({readable: pt2, writable: pt1}, opts);
        cb(client, server);
      });

    });

    suite('stateless', () => {

      run((clientPtcl, serverPtcl, opts, cb) => {
        if (!cb) {
          cb = opts;
          opts = undefined;
        }
        opts = opts || {};
        opts.silent = true;
        let client = clientPtcl.createClient(opts);
        client.createChannel(writableFactory);
        let server = serverPtcl.createServer(opts);
        cb(client, server);

        function writableFactory(transportCt) {
          let reqPt = new stream.PassThrough()
            .on('finish', () => {
              server.createChannel((channelCb) => {
                let resPt = new stream.PassThrough()
                  .on('finish', () => { transportCt(null, resPt); });
                channelCb(null, resPt);
                return reqPt;
              }, opts);
            });
          return reqPt;
        }
      });

    });

    function run(setupFn) {

      test('primitive types', (done) => {
        let svc = Service.forProtocol({
          protocol: 'Math',
          messages: {
            negateFirst: {
              request: [{name: 'ns', type: {type: 'array', items: 'int'}}],
              response: 'long'
            }
          }
        });
        setupFn(svc, svc, (client, server) => {
          server
            .onNegateFirst(function (ns, cb) {
              assert.strictEqual(this.channel.server, server);
              cb(null, -ns[0]);
            });
          let channel = client.activeChannels()[0];
          channel.on('eot', () => {
            done();
          })
            .once('handshake', (hreq, hres) => {
              // Allow the initial ping to complete.
              assert.equal(hres.match, 'BOTH');
              process.nextTick(() => {
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

      test('invalid strict error', (done) => {
        let svc = Service.forProtocol({
          protocol: 'Math',
          messages: {
            sqrt: {
              request: [{name: 'n', type: 'float'}],
              response: 'float'
            }
          }
        });
        setupFn(svc, svc, {strictTypes: true}, (client, server) => {
          server.onSqrt((n, cb) => {
            if (n === -1) {
              cb(new Error('no i')); // Invalid error (should be a string).
            } else if (n < 0) {
              throw new Error('negative');
            } else {
              cb(undefined, Math.sqrt(n));
            }
          });
          client.sqrt(-1, (err) => {
            assert(/internal server error/.test(err), err);
            client.sqrt(-2, (err) => {
              assert(/internal server error/.test(err), err);
              client.sqrt(100, (err, res) => {
                // The server still doesn't die (we can make a new request).
                assert.strictEqual(err, undefined);
                assert(Math.abs(res - 10) < 1e-5);
                done();
              });
            });
          });
        });
      });

      test('non-strict response', (done) => {
        let svc = Service.forProtocol({
          protocol: 'Ping',
          messages: {
            ping: {request: [], response: 'null'}
          }
        });
        setupFn(svc, svc, (client, server) => {
          server.onPing((cb) => { cb(); });
          client.ping((err) => {
            assert(!err, err);
            done();
          });
        });
      });

      test('invalid strict response', (done) => {
        let svc = Service.forProtocol({
          protocol: 'Ping',
          messages: {
            ping: {request: [], response: 'null'}
          }
        });
        setupFn(svc, svc, {strictTypes: true}, (client, server) => {
          server.onPing((cb) => { cb(); });
          client.ping((err) => {
            assert(/internal server error/.test(err), err);
            done();
          });
        });
      });

      test('client middleware', (done) => {
        let svc = Service.forProtocol({
          protocol: 'Math',
          messages: {
            neg: {request: [{name: 'n', type: 'int'}], response: 'int'}
          }
        });
        setupFn(svc, svc, (client, server) => {
          server.onNeg((n, cb) => { cb(null, -n); });
          let buf = utils.bufferFrom([0, 1]);
          let isDone = false;
          let channel = client.activeChannels()[0];
          client
            .use(function (wreq, wres, next) {
              // No callback.
              assert.strictEqual(this.channel, channel);
              assert.deepEqual(wreq.headers, {});
              wreq.headers.buf = buf;
              assert.deepEqual(wreq.request, {n: 2});
              next();
            })
            .use((wreq, wres, next) => {
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
            .neg(2, (err, res) => {
              assert.strictEqual(err, null);
              assert.equal(res, -3);
              assert(isDone);
              done();
            });
        });
      });

      test('client middleware forward error', (done) => {
        let svc = Service.forProtocol({
          protocol: 'Math',
          messages: {
            neg: {request: [{name: 'n', type: 'int'}], response: 'int'}
          }
        });
        setupFn(svc, svc, (client, server) => {
          server.onNeg((n, cb) => { cb(null, -n); });
          let fwdErr = new Error('forward!');
          let bwdErr = new Error('backward!');
          let called = false;
          client
            .use((wreq, wres, next) => {
              next(null, (err, prev) => {
                assert.strictEqual(err, fwdErr);
                assert(!called);
                prev(bwdErr); // Substitute the error.
              });
            })
            .use((wreq, wres, next) => {
              next(fwdErr, (err, prev) => {
                called = true;
                prev();
              });
            })
            .neg(2, (err) => {
              assert.strictEqual(err, bwdErr);
              done();
            });
        });
      });

      test('client middleware duplicate forward calls', (done) => {
        let svc = Service.forProtocol({
          protocol: 'Math',
          messages: {
            neg: {request: [{name: 'n', type: 'int'}], response: 'int'}
          }
        });
        setupFn(svc, svc, (client, server) => {
          server.onNeg((n, cb) => { cb(null, -n); });
          let chn = client.activeChannels()[0];
          client
            .on('error', (err, chn_) => {
              assert(/duplicate forward middleware/.test(err), err);
              assert.strictEqual(chn_, chn);
              setTimeout(() => { done(); }, 0);
            });
          client
            .use((wreq, wres, next) => {
              next();
              next();
            })
            .neg(2, (err, res) => {
              assert.equal(res, -2);
            });
        });
      });

      test('server middleware', (done) => {
        let svc = Service.forProtocol({
          protocol: 'Math',
          messages: {
            neg: {request: [{name: 'n', type: 'int'}], response: 'int'}
          }
        });
        setupFn(svc, svc, (client, server) => {
          let isDone = false;
          let buf = utils.bufferFrom([0, 1]);
          // The server's channel won't be ready right away in the case of
          // stateless transports.
          let channel;
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
            .onNeg((n, cb) => { cb(null, -n); });
          client
            .use((wreq, wres, next) => {
              next(null, (err, prev) => {
                assert.deepEqual(wres.headers, {buf: buf});
                isDone = true;
                prev();
              });
            })
            .neg(2, (err, res) => {
              assert.strictEqual(err, null);
              assert.equal(res, -2);
              assert(isDone);
              done();
            });
        });
      });

      test('server middleware duplicate backward calls', (done) => {
        let svc = Service.forProtocol({
          protocol: 'Math',
          messages: {
            neg: {request: [{name: 'n', type: 'int'}], response: 'int'}
          }
        });
        setupFn(svc, svc, (client, server) => {
          server
            .use((wreq, wres, next) => {
              // Attach error handler to channel.
              server.on('error', (err, chn) => {
                assert(/duplicate backward middleware/.test(err), err);
                assert.strictEqual(chn.server, server);
                setTimeout(() => { done(); }, 0);
              });
              next(null, (err, prev) => {
                prev();
                prev();
              });
            })
            .onNeg((n, cb) => { cb(null, -n); });
          client
            .neg(2, (err, res) => {
              assert.strictEqual(err, null);
              assert.equal(res, -2);
            });
        });
      });

      test('server middleware invalid response header', (done) => {
        let svc = Service.forProtocol({
          protocol: 'Math',
          messages: {
            neg: {request: [{name: 'n', type: 'int'}], response: 'int'}
          }
        });
        setupFn(svc, svc, (client, server) => {
          let fooErr = new Error('foobar');
          let sawFoo = 0;
          server
            .on('error', (err) => {
              if (err === fooErr) {
                sawFoo++;
                return;
              }
              assert.equal(sawFoo, 1);
              assert(/invalid "bytes"/.test(err.message), err);
              setTimeout(() => { done(); }, 0);
            })
            .use((wreq, wres, next) => {
              wres.headers.id = 123;
              next();
            })
            .onNeg(() => { throw fooErr; });
          client
            .neg(2, (err) => {
              assert(/internal server error/.test(err), err);
            });
        });
      });

      test('error formatter', (done) => {
        let svc = Service.forProtocol({
          protocol: 'Math',
          messages: {
            neg: {request: [{name: 'n', type: 'int'}], response: 'int'}
          }
        });
        let opts = {systemErrorFormatter: formatter};
        let barErr = new Error('baribababa');
        setupFn(svc, svc, opts, (client, server) => {
          server
            .onNeg(() => { throw barErr; });
          client
            .neg(2, (err) => {
              assert(/FOO/.test(err));
              done();
            });
        });

        function formatter(err) {
          assert.strictEqual(err, barErr);
          return 'FOO';
        }
      });

      test('remote protocols', (done) => {
        let clientPtcl = {
          protocol: 'Math1',
          foo: 'bar', // Custom attribute.
          doc: 'hi',
          messages: {
            neg: {request: [{name: 'n', type: 'int'}], response: 'long'}
          }
        };
        let serverPtcl = {
          protocol: 'Math2',
          doc: 'hey',
          bar: 'foo', // Custom attribute.
          messages: {
            neg: {request: [{name: 'n', type: 'long'}], response: 'int'}
          }
        };
        let clientSvc = Service.forProtocol(clientPtcl);
        let serverSvc = Service.forProtocol(serverPtcl);
        setupFn(clientSvc, serverSvc, (client, server) => {
          server
            .onNeg((n, cb) => { cb(null, -n); });
          client
            .neg(2, (err, res) => {
              assert(!err, err);
              assert.equal(res, -2);
              let remotePtcl;
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

      test('client timeout', (done) => {
        let svc = Service.forProtocol({
          protocol: 'Sleep',
          messages: {
            sleep: {request: [{name: 'ms', type: 'int'}], response: 'int'}
          }
        });
        setupFn(svc, svc, {timeout: 50}, (client, server) => {
          server
            .onSleep((n, cb) => {
              // Delay response by the number requested.
              setTimeout(() => { cb(null, n); }, n);
            });
          client.sleep(10, (err, res) => {
            // Default timeout used here, but delay is short enough.
            assert.strictEqual(err, null);
            assert.equal(res, 10);
            client.sleep(100, (err) => {
              // Default timeout used here, but delay is _not_ short enough.
              assert(/timeout/.test(err), err);
              client.sleep(100, {timeout: 200}, (err, res) => {
                // Custom timeout, high enough for the delay.
                assert.strictEqual(err, null);
                assert.equal(res, 100);
                done();
              });
            });
          });
        });
      });

      test('server error after handler', (done) => {
        let svc = Service.forProtocol({
          protocol: 'Math',
          messages: {
            neg: {request: [{name: 'n', type: 'int'}], response: 'int'}
          }
        });
        setupFn(svc, svc, (client, server) => {
          let numErrors = 0;
          server
            .on('error', (err) => {
              numErrors++;
              assert(/bar/.test(err), err);
            })
            .onNeg((n, cb) => {
              cb(null, -n);
              throw new Error('bar');
            });
          client.neg(2, (err, n) => {
            assert(!err, err);
            assert.equal(n, -2);
            setTimeout(() => {
              assert.equal(numErrors, 1);
              done();
            }, 0);
          });
        });
      });

      test('client channel destroy no wait', (done) => {
        let svc = Service.forProtocol({
          protocol: 'Delay',
          messages: {
            wait: {
              request: [{name: 'ms', type: 'int'}],
              response: 'string'
            }
          }
        });
        let interrupted = 0;
        setupFn(svc, svc, (client, server) => {
          server.onWait((ms, cb) => {
            setTimeout(() => { cb(null, 'ok'); }, ms);
          });
          let channel = client.activeChannels()[0];
          channel.on('eot', (pending) => {
            assert.equal(pending, 2);
            setTimeout(() => {
              assert.equal(interrupted, 2);
              done();
            }, 5);
          });
          client.wait(75, interruptedCb);
          client.wait(50, interruptedCb);
          client.wait(10, (err, res) => {
            assert.equal(res, 'ok');
            channel.destroy(true);
          });

          function interruptedCb(err) {
            assert(/interrupted/.test(err), err);
            interrupted++;
          }

        });
      });

      test('out of order requests', (done) => {
        let svc = Service.forProtocol({
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
        let ids = [];
        setupFn(svc, svc, (client, server) => {
          server.onW((delay, id, cb) => {
            if (delay < 0) {
              cb('delay must be non-negative');
              return;
            }
            setTimeout(() => { cb(null, id); }, delay);
          });
          let channel = client.activeChannels()[0];
          channel.on('eot', (pending) => {
            assert.equal(pending, 0);
            assert.deepEqual(ids, [undefined, 'b', 'a']);
            done();
          }).once('handshake', (hreq, hres) => {
            assert.equal(hres.match, 'BOTH');
            process.nextTick(() => {
              client.w(500, 'a', (err, res) => {
                assert.strictEqual(err, null);
                ids.push(res);
              });
              client.w(10, 'b', (err, res) => {
                assert.strictEqual(err, null);
                ids.push(res);
                channel.destroy();
              });
              client.w(-10, 'c', (err, res) => {
                assert(/non-negative/.test(err));
                ids.push(res);
              });
            });
          });
        });
      });

      test('destroy server channel during handshake', (done) => {
        let svc = Service.forProtocol({
          protocol: 'Sleep',
          messages: {
            sleep: {request: [{name: 'ms', type: 'int'}], response: 'int'}
          }
        });
        setupFn(svc, svc, {timeout: 20}, (client, server) => {
          // For stateful emitters, the channel already exists.
          server.activeChannels().forEach(onChannel);
          // For stateless emitters, the channel won't exist yet.
          server.on('channel', onChannel);
          client.sleep(10, (err) => {
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

  suite('discover attributes', () => {

    let discoverProtocol = services.discoverProtocol;

    test('stateful ok', (done) => {
      let schema = {
        protocol: 'Case',
        messages: {
          upper: {
            request: [{name: 'str', type: 'string'}],
            response: 'string'
          }
        }
      };
      let svc = Service.forProtocol(schema);
      let server = svc.createServer()
        .onUpper((str, cb) => {
          cb(null, str.toUpperCase());
        });
      let transports = createPassthroughTransports();
      server.createChannel(transports[1]);
      discoverProtocol(transports[0], (err, actualAttrs) => {
        assert.strictEqual(err, null);
        assert.deepEqual(actualAttrs, schema);
        // Check that the transport is still usable.
        let client = svc.createClient();
        client.createChannel(transports[0])
          .on('eot', () => {
            done();
          });
        client.upper('foo', function (err, res) {
          assert.strictEqual(err, null);
          assert.equal(res, 'FOO');
          this.channel.destroy();
        });
      });
    });

    test('legacy stateless ok', (done) => {
      // Using old API.
      let schema = {
        protocol: 'Case',
        messages: {
          upper: {
            request: [{name: 'str', type: 'string'}],
            response: 'string'
          }
        }
      };
      let svc = Service.forProtocol(schema)
        .on('upper', (req, ee, cb) => {
          cb(null, req.str.toUpperCase());
        });
      discoverProtocol(writableFactory, (err, actual) => {
        assert.strictEqual(err, null);
        assert.deepEqual(actual, schema);
        // Check that the transport is still usable.
        let me = svc.createEmitter(writableFactory).on('eot', () => {
          done();
        });
        svc.emit('upper', {str: 'foo'}, me, (err, res) => {
          assert.strictEqual(err, null);
          assert.equal(res, 'FOO');
          me.destroy();
        });
      });

      function writableFactory(emitterCb) {
        let reqPt = new stream.PassThrough()
          .on('finish', () => {
            svc.createListener((listenerCb) => {
              let resPt = new stream.PassThrough()
                .on('finish', () => { emitterCb(null, resPt); });
              listenerCb(null, resPt);
              return reqPt;
            });
          });
        return reqPt;
      }
    });

    test('stateful wrong scope', (done) => {
      let schema = {
        protocol: 'Case',
        messages: {
          upper: {
            request: [{name: 'str', type: 'string'}],
            response: 'string'
          }
        }
      };
      let svc = Service.forProtocol(schema);
      let scope = 'bar';
      let transports = createPassthroughTransports();
      svc.createServer({silent: true})
        .createChannel(transports[1], {scope: scope});
      discoverProtocol(transports[0], {timeout: 5}, (err) => {
        assert(/timeout/.test(err), err);
        // Check that the transport is still usable.
        let client = svc.createClient();
        let chn = client.createChannel(transports[0], {scope: scope})
          .on('eot', () => { done(); });
        client.upper('foo', (err) => {
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
  let framed = utils.newBuffer(buf.length + 4);
  framed.writeInt32BE(buf.length);
  buf.copy(framed, 4);
  return framed;
}

function createPassthroughTransports(objectMode) {
  let pt1 = stream.PassThrough({objectMode: objectMode});
  let pt2 = stream.PassThrough({objectMode: objectMode});
  return [{readable: pt1, writable: pt2}, {readable: pt2, writable: pt1}];
}

// Simplified stream constructor API isn't available in earlier node versions.
// TODO: can these be removed now?

function createReadableStream(bufs) {
  let n = 0;
  class Stream extends stream.Readable {
    constructor () {
      super({objectMode: true});
    }

    _read () {
      this.push(bufs[n++] || null);
    }
  }
  return new Stream();
}

function createWritableStream(bufs) {
  class Stream extends stream.Writable {
    constructor () {
      super({objectMode: true});
    }

    _write (buf, encoding, cb) {
      bufs.push(buf);
      cb();
    }
  }
  return new Stream();
}
