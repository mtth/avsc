/* jshint esversion: 6, node: true */

'use strict';

const services = require('./services');

module.exports = {
  Service: services.Service,
  discoverProtocol: services.discoverProtocol
};
