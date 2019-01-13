/* jshint esversion: 6, node: true */

// TODO: Document (and implement) channel APIs letting ping messages through
// (without any impact on connections). This will let us implement a
// `discoverProtocol` method.

'use strict';

const {NettyProxy, netty} = require('./netty');

module.exports = {
  NettyProxy,
  netty,
};
