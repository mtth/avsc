/* jshint esversion: 6, node: true */

'use strict';

const {Channel, RoutingChannel, SelfRefreshingChannel} = require('./channel');
const {NettyChannel, NettyGateway} = require('./codecs/netty');
const {Service} = require('./service');
const {SystemError} = require('./utils');

let Client, Server, Deadline;
if (process.env.AVRO_SERVICES_NO_PROMISES) {
  const call = require('./call');
  Client = call.Client;
  Server = call.Server;
  Deadline = require('./deadline').Deadline;
} else {
  const promises = require('./promises');
  Client = promises.PromisifiedClient;
  Server = promises.PromisifiedServer;
  Deadline = promises.PromisifiedDeadline;
}

module.exports = {
  Channel,
  Client,
  Deadline,
  NettyChannel,
  NettyGateway,
  RoutingChannel,
  SelfRefreshingChannel,
  Server,
  Service,
  SystemError,
};
