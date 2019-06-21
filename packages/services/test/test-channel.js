/* jshint esversion: 6, mocha: true, node: true */

'use strict';

const {Client, Server} = require('../lib/call');
const {Channel, Packet, RoutingChannel, SelfRefreshingChannel} = require('../lib/channel');
const {Deadline} = require('../lib/deadline');
const {Service} = require('../lib/service');

const assert = require('assert');
const backoff = require('backoff');
const sinon = require('sinon');

const echoSvc = new Service({
  protocol: 'Echo',
  messages: {
    echo: {
      request: [{name: 'message', type: 'string'}],
      response: 'string',
    },
  },
});
const upperSvc = new Service({
  protocol: 'foo.Upper',
  messages: {
    upper: {
      request: [{name: 'message', type: 'string'}],
      response: 'string',
      errors: [
        {name: 'UpperError', type: 'error', fields: []},
      ],
    },
  },
});

suite('Channel', () => {
  test('is channel', () => {
    assert(Channel.isChannel(new Channel(() => { assert(false); })));
    assert(!Channel.isChannel(null));
    assert(!Channel.isChannel({}));
  });

  test('emits events', () => {
    const evts = [];
    const chan = new Channel((deadline, preq, cb) => {
      evts.push('handle');
      cb(null, Packet.ping(null));
    }).on('requestPacket', () => { evts.push('req'); })
      .on('responsePacket', () => { evts.push('res'); });
    chan.call(Deadline.infinite(), {}, (err) => {
      assert.ifError(err);
      evts.push('done');
    });
    assert.deepEqual(evts, ['req', 'handle', 'res', 'done']);
  });

  test('expired deadline ping', () => {
    new Channel(() => { assert(false); })
      .ping(Deadline.forMillis(-1), null, {}, () => {
      assert(false);
    });
  });

  test('expired deadline after', () => {
    new Channel((deadline, preq, cb) => {
      deadline.expire();
      cb(null);
    }).call(Deadline.infinite(), {}, () => {
      assert(false);
    });
  });

  test('buffer', (done) => {
    const chan = new Channel();
    const boom = new Error('boom');
    chan.call(Deadline.infinite(), {}, (err) => {
      assert.strictEqual(err.cause, boom);
      done();
    });
    process.nextTick(() => { chan.open((deadline, preq, cb) => { cb(boom); }); });
  });
});

suite('RoutingChannel', () => {
  test('two servers found', (done) => {
    const echoServer = new Server(echoSvc)
      .onMessage().echo((str, cb) => { cb(null, str); });
    const upperServer = new Server(upperSvc);
    const chan = RoutingChannel.forServers([echoServer, upperServer]);
    const client = new Client(echoSvc).channel(chan);
    client.emitMessage().echo('foo', (err, str) => {
      assert.ifError(err);
      assert.equal(str, 'foo');
      done();
    });
  });

  test('one server not found', (done) => {
    const upperServer = new Server(upperSvc);
    const chan = RoutingChannel.forServers([upperServer]);
    const client = new Client(echoSvc).channel(chan);
    client.emitMessage().echo('foo', (err) => {
      assert.equal(err.code, 'ERR_INCOMPATIBLE_PROTOCOL');
      done();
    });
  });

  test('no server', (done) => {
    const chan = new RoutingChannel();
    const client = new Client(echoSvc).channel(chan);
    client.emitMessage().echo('foo', (err) => {
      assert.equal(err.code, 'ERR_INCOMPATIBLE_PROTOCOL');
      done();
    });
  });

  test('close downstream', (done) => {
    const upperChan = new Server(upperSvc).channel();
    const chan = new RoutingChannel([upperChan]);
    const client = new Client(echoSvc).channel(chan);
    client.emitMessage().echo('foo', (err) => {
      assert.equal(err.code, 'ERR_INCOMPATIBLE_PROTOCOL');
      upperChan.close();
      const echoServer = new Server(echoSvc);
      chan.addDownstream(echoServer.channel());
      client.emitMessage().echo('bar', (err) => {
        assert.equal(err.code, 'ERR_NOT_IMPLEMENTED');
        echoServer.onMessage().echo((str, cb) => { cb(null, str); });
        client.emitMessage().echo('baz', (err, str) => {
          assert.ifError(err);
          assert.equal(str, 'baz');
          done();
        });
      });
    });
  });
});

suite('SelfRefreshingChannel', () => {
  let clock;
  setup(() => { clock = sinon.useFakeTimers(); });
  teardown(() => { clock.restore(); });

  test('simple', (done) => {
    const echoServer = new Server(echoSvc)
      .onMessage().echo((str, cb) => { cb(null, str); });

    const chan = new SelfRefreshingChannel(provider);
    chan.once('close', () => { done(); });

    const echoClient = new Client(echoSvc).channel(chan);
    echoClient.emitMessage().echo('foo', (err, res) => {
      assert(!err, err);
      assert.equal(res, 'foo');
      chan.close();
    });
    clock.tick(100);

    function provider(cb) {
      cb(null, echoServer.channel());
    }
  });

  test('flaky connection', (done) => {
    const echoServer = new Server(echoSvc)
      .onMessage().echo((str, cb) => { cb(null, str); });

    const refreshBackoff = backoff.fibonacci()
      .on('backoff', () => { clock.tick(1000); });
    const opts = {refreshBackoff};

    const chan = new SelfRefreshingChannel(flaky(1, provider), opts);
    chan.once('close', () => { done(); });

    const echoClient = new Client(echoSvc).channel(chan);
    echoClient.emitMessage().echo('foo', (err, res) => {
      assert(!err, err);
      assert.equal(res, 'foo');
      chan.close();
    });

    clock.tick(1000);

    function provider(cb) {
      cb(null, echoServer.channel());
    }
  });

  test('changes service', (done) => {
    const upperServer = new Server(upperSvc);
    const upperChan = upperServer.channel();

    const echoServer = new Server(echoSvc)
      .onMessage().echo((str, cb) => { cb(null, str); });

    const chan = new SelfRefreshingChannel(provider);
    chan.once('close', () => { done(); });

    const echoClient = new Client(echoSvc).channel(chan);
    const deadline = Deadline.infinite();
    echoClient.emitMessage(deadline).echo('foo', (err) => {
      assert.equal(err.code, 'ERR_INCOMPATIBLE_PROTOCOL');
      upperChan.close();
      echoClient.emitMessage(deadline).echo('bar', (err, str) => {
        assert.ifError(err);
        assert.equal(str, 'bar');
        chan.close();
      });
    });

    function provider(cb) {
      cb(null, upperChan.closed ? echoServer.channel() : upperChan);
    }
  });

  function flaky(n, fn) {
    return (cb) => {
      if (n--) {
        cb(new Error('flake'));
        return;
      }
      fn(cb);
    };
  }
});
