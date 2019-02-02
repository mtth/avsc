# Avro IDL parsing

Parse [Avro IDL specs][idl] into JSON protocols. Both synchronous and
asynchronous modes are available:

```javascript
const {assembleProtocol, assembleProtocolSync} = require('@avro/idl');

// Asynchronously:
assembleProtocol('path/to.avdl', (err, protocol) => {
  // protocol is an equivalent JSON representation of the IDL spec.
});

// Synchronously:
const protocol = assembleProtocolSync('path/to.avdl');
```

[idl]: http://avro.apache.org/docs/current/idl.html
