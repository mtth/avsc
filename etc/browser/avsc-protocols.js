'use strict';

/**
 * (Deprecated, in favor of `avsc-services`) optional entry point for browser
 * builds.
 *
 * To use it: `require('avsc/etc/browser/avsc-protocols')`.
 */

let avroServices = require('./avsc-services'),
    utils = require('../../lib/utils');

module.exports = {
  Protocol: avroServices.Service,
  assemble: avroServices.assembleProtocol
};

utils.copyOwnProperties(avroServices, module.exports);
