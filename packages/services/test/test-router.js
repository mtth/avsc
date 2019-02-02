/* jshint esversion: 6, mocha: true, node: true */

'use strict';

const {Trace} = require('../lib/channel');
const {Client, Server} = require('../lib/call');
const {Service} = require('../lib/service');
const {Router} = require('../lib/router');

const {Type} = require('@avro/types');
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

suite('server routers', () => {
  test('single service', (done) => {
    const echoServer = new Server(echoSvc)
      .onMessage().echo((str, cb) => { cb(null, str); });
    const router = Router.forServers([echoServer]);
    const echoClient = new Client(echoSvc);
    echoClient.channel = router.channel;
    const upperClient = new Client(upperSvc);
    upperClient.channel = router.channel;
    echoClient.emitMessage(new Trace()).echo('foo', (err, res) => {
      assert(!err, err);
      assert.equal(res, 'foo');
      upperClient.emitMessage(new Trace()).upper('bar', (err, res) => {
        assert.equal(err.code, 'ERR_AVRO_SERVICE_NOT_FOUND');
        done();
      });
    });
  });

  test('two services', (done) => {
    const echoServer = new Server(echoSvc)
      .onMessage().echo((str, cb) => { cb(null, str); });
    const upperServer = new Server(upperSvc);
    const router = Router.forServers([echoServer, upperServer]);
    const echoClient = new Client(echoSvc);
    echoClient.channel = router.channel;
    echoClient.emitMessage(new Trace()).echo('foo', (err, res) => {
      assert(!err, err);
      assert.equal(res, 'foo');
      done();
    });
  });

  test('duplicate service', () => {
    const echoServer = new Server(echoSvc);
    assert.throws(
      () => { Router.forServers([echoServer, echoServer]); },
      /duplicate/
    );
  });

  test('dispatching closing child', (done) => {
    const echoServer = new Server(echoSvc)
      .onMessage().echo((str, cb) => { cb(null, str); });
    const upperServer = new Server(upperSvc);
    const echoRouter = Router.forServers([echoServer]);
    const upperRouter = Router.forServers([upperServer]);
    const router = Router.forRouters([echoRouter, upperRouter])
      .once('close', () => {
        done();
      });
    const echoClient = new Client(echoSvc);
    echoClient.channel = router.channel;
    echoClient.emitMessage(new Trace()).echo('foo', (err, res) => {
      assert(!err, err);
      assert.equal(res, 'foo');
      echoRouter.close();
    });
  });

  test('custom routing key', (done) => {
    const svc1 = new Service({
      protocol: 'EchoService',
      version: 1,
      messages: echoSvc.protocol.messages,
    });
    const svc2 = new Service({
      protocol: 'EchoService',
      version: 2,
      messages: echoSvc.protocol.messages,
    });
    const server1 = new Server(svc1)
      .onMessage().echo((str, cb) => { cb(null, str); });
    const server2 = new Server(svc2);
    const router = Router.forServers([server1, server2], {routingKeys});

    const echoClient = new Client(svc1)
    echoClient.channel = router.channel;
    echoClient.emitMessage(new Trace()).echo('foo', (err, str) => {
      assert.ifError(err);
      assert.equal(str, 'foo');
      done();
    });

    function routingKeys(ptcl) {
      return [`${ptcl.protocol}-v${ptcl.version}`];
    }
  });
});

suite('self-refreshing', () => {
  let clock;
  setup(() => { clock = sinon.useFakeTimers(); });
  teardown(() => { clock.restore(); });

  test('simple', (done) => {
    const echoServer = new Server(echoSvc)
      .onMessage().echo((str, cb) => { cb(null, str); });

    Router.selfRefreshing(routerProvider, (err, router) => {
      assert(!err, err);
      router.once('down', () => { done(); });

      const echoClient = new Client(echoSvc);
      echoClient.channel = router.channel;
      echoClient.emitMessage(new Trace()).echo('foo', (err, res) => {
        assert(!err, err);
        assert.equal(res, 'foo');
        router.close();
      });
    });
    clock.tick(100);

    function routerProvider(cb) {
      cb(null, Router.forServers([echoServer]));
    }
  });

  test('flaky open', (done) => {
    const echoServer = new Server(echoSvc)
      .onMessage().echo((str, cb) => { cb(null, str); });

    const refreshBackoff = backoff.fibonacci()
      .on('backoff', () => { clock.tick(1000); });
    const opts = {refreshBackoff};
    Router.selfRefreshing(flaky(1, routerProvider), opts, (err, router) => {
      assert.ifError(err);
      const echoClient = new Client(echoSvc);
      echoClient.channel = router.channel;
      echoClient.emitMessage(new Trace()).echo('foo', (err, res) => {
        assert(!err, err);
        assert.equal(res, 'foo');
        router.close();
        done();
      });
    });
    clock.tick(1000);

    function routerProvider(cb) {
      cb(null, Router.forServers([echoServer]));
    }
  });

  test('flaky connection', (done) => {
    const echoServer = new Server(echoSvc)
      .onMessage().echo((str, cb) => { cb(null, str); });

    const refreshBackoff = backoff.fibonacci()
      .on('backoff', () => { clock.tick(100); });
    const opts = {refreshBackoff};
    Router.selfRefreshing(routerProvider, opts, (err, router) => {
      assert.ifError(err);
      const echoClient = new Client(echoSvc);
      echoClient.channel = router.channel;
      echoClient.emitMessage(new Trace()).echo('foo', (err, res) => {
        assert.ifError(err);
        assert.equal(res, 'foo');
        clock.tick(500);
        echoClient.emitMessage(new Trace()).echo('bar', (err, res) => {
          assert.ifError(err);
          assert.equal(res, 'bar');
          done();
        });
      });
    });
    clock.tick(100);

    function routerProvider(cb) {
      const router = Router.forServers([echoServer]);
      setTimeout(() => { router.close(); }, 500);
      cb(null, router);
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
