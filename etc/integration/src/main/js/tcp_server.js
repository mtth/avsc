'use strict';

let avro = require('../../../../../lib'),
    net = require('net');


let protocol = avro.parse('./src/main/avro/math.avpr')
  .on('add', (req, ee, cb) => {
    let res = req.pair.left + req.pair.right;
    console.log(req);
    console.log(res);
    cb(null, res);
  });

net.createServer((con) => { protocol.createListener(con); })
  .listen(65111, () => { console.log('listening'); });
