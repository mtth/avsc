/* jshint esversion: 6, node: true */

'use strict';

const {Channel, RoutingChannel, SelfRefreshingChannel} = require('./channel');
const {NettyChannel, NettyGateway} = require('./codecs/netty');
const {Message, Service} = require('./service');
const {Trace} = require('./trace');

let Client, Server;
if (process.env.AVRO_SERVICES_NO_PROMISES) {
  {Client, Server} = require('./call');
} else {
  {PromiseClient: Client, PromiseServer: Server} = require('./promises');
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
