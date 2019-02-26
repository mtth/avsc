/* jshint esversion: 6, node: true */

'use strict';

const {Channel, RoutingChannel, SelfRefreshingChannel} = require('./channel');
const {NettyChannel, NettyGateway} = require('./codecs/netty');
const {Message, Service} = require('./service');
const {Trace} = require('./trace');

let Client, Server;
if (process.env.AVRO_SERVICES_NO_PROMISES) {
  const exports = require('./call');
  Client = exports.Client;
  Server = exports.Server;
} else {
  const exports = require('./promises');
  Client = exports.PromisifiedClient;
  Server = exports.PromisifiedServer;
}

module.exports = {
  Channel,
  Client,
  NettyChannel,
  NettyGateway,
  RoutingChannel,
  SelfRefreshingChannel,
  Server,
  Service,
  Trace,
};
