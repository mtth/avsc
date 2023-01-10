'use strict';

let avro = require('../../../../../lib'),
    assert = require('assert'),
    net = require('net');


let protocol = avro.parse('./src/main/avro/math.avpr');
let socket = net.createConnection({host: 'localhost', port: 65111});
let ee = protocol.createEmitter(socket)
  .on('eot', () => { socket.destroy(); });

protocol.emit('add', {pair: {left: 2, right: 5}}, ee, (err, res) => {
  assert.strictEqual(err, null);
  assert.equal(res, 7);
  ee.destroy();
});
