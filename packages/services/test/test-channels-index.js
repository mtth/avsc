/* jshint esversion: 6, mocha: true, node: true */

'use strict';

const {Client, Server} = require('../lib/call');
const {Context} = require('../lib/context');
const {Service} = require('../lib/service');
const {Router} = require('../lib/channels');

const assert = require('assert');
const {Type} = require('avsc');

suite('channels index', () => {
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

  suite('router', () => {
    test('single service', (done) => {
      const echoServer = new Server(echoSvc)
        .onMessage().echo((str, cb) => { cb(null, str); });
      const router = new Router([echoServer.channel]);
      const echoClient = new Client(echoSvc);
      echoClient.channel = router.channel;
      const upperClient = new Client(upperSvc);
      upperClient.channel = router.channel;
      echoClient.emitMessage(new Context()).echo('foo', (err, res) => {
        assert(!err, err);
        assert.equal(res, 'foo');
        upperClient.emitMessage(new Context()).upper('bar', (err, res) => {
          assert.equal(err.code, 'ERR_AVRO_SERVICE_NOT_FOUND');
          done();
        });
      });
    });

    test('two services', (done) => {
      const echoServer = new Server(echoSvc)
        .onMessage().echo((str, cb) => { cb(null, str); });
      const upperServer = new Server(upperSvc);
      const router = new Router([echoServer.channel, upperServer.channel]);
      const echoClient = new Client(echoSvc);
      echoClient.channel = router.channel;
      echoClient.emitMessage(new Context()).echo('foo', (err, res) => {
        assert(!err, err);
        assert.equal(res, 'foo');
        router.channel(null, (err, svcs) => {
          assert(!err, err);
          svcs.sort((svc) => svc.name);
          assert.deepEqual(svcs, [echoSvc, upperSvc]);
          done();
        });
      });
    });

    test('duplicate service', (done) => {
      const echoServer = new Server(echoSvc);
      new Router([echoServer.channel, echoServer.channel])
        .once('error', (err) => {
          assert(/duplicate/.test(err), err);
          done();
        });
    });
  });
});
