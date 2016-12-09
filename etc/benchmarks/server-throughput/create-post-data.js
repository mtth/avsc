/* jshint esversion: 6, node: true */

'use strict';

var avro = require('../../../lib/');
var path = require('path');

var STRING_TYPE = avro.Type.forSchema('string');

var fpath = path.join(__dirname, 'Log.avdl');
avro.assembleProtocolSchema(fpath, function (err, schema) {
  var ptcl = avro.Protocol.forSchema(schema);
  var msg = ptcl.getMessage('createEntries');
  var reqType = msg.getRequestType();
  var payload = Buffer.concat([
    // Handshake.
    ptcl.getFingerprint(), // Client hash.
    new Buffer([0]), // No client protocol.
    ptcl.getFingerprint(), // Server hash.
    new Buffer([0]), // No metadata.
    // Body.
    new Buffer([0]), // No header.
    STRING_TYPE.toBuffer('createEntries'), // Message name.
    reqType.toBuffer(reqType.random()) // Random value.
  ]);
  // Frame the payload.
  process.stdout.write(intBuffer(payload.length));
  process.stdout.write(payload);
  process.stdout.write(intBuffer(0));
});

function intBuffer(n) {
  var buf = new Buffer(4);
  buf.writeInt32BE(n);
  return buf;
}
