'use strict';

let avro = require('../../../../../lib'),
    assert = require('assert'),
    net = require('net');


let protocol = avro.parse('./src/main/avro/math.avpr');
let socket = net.createConnection({host: 'localhost', port: 65111});
let ee = protocol.createEmitter(socket)
  .on('eot', function () { socket.destroy(); });

protocol.emit('add', {pair: {left: 2, right: 5}}, ee, function (err, res) {
  assert.strictEqual(err, null);
  assert.equal(res, 7);
  ee.destroy();
});
