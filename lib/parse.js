'use strict';

var util = require('./util');

function registerRecords(schema) {

  var registry = {};
  register(schema);
  return registry;

  function register(obj, namespace) {

    if (typeof obj == 'string') {
      obj = {type: obj};
    }

    if (obj instanceof Array) {
      obj.map(function (elem) { register(elem, namespace); });
    } else {
      var ns = obj.namespace || namespace;
      switch (obj.type) {
        case 'map':
          register(obj.values, ns);
          break;
        case 'array':
          register(obj.items, ns);
          break;
        case 'record':
          var name = obj.name;
          if (!~name.indexOf('.') && ns) {
            name = ns + '.' + name;
          }
          registry[name] = obj;
          register(obj.fields, ns);
          break;
        default:
      }
    }

  }

}

module.exports = {
  registerRecords: registerRecords
};
