var util = require('util'),
    avro = require('avsc');
function MetaType(attr, opts) {
  avro.types.LogicalType.call(this, attr, opts, [avro.types.RecordType]);
}

util.inherits(MetaType, avro.types.LogicalType);

MetaType.prototype._fromValue = function(val) {
  var obj = val.value;
  return obj[Object.keys(obj)[0]];
}

MetaType.prototype._toValue = function(any) {
  var obj;
  if(typeof any == 'string') {
    if (~primitiveSymbols.indexOf(any)) {
      // Handling primitive names separately from references lets us save a
      // significant amount of space (1 byte per type name instead of 5-8).
      obj = { PrimitiveType: any};
    } else {
      obj = {string: any};
    }
  } else if (any instanceof Array) {
    obj = {array: any};
  } else {
    obj = {};
    obj[capitalize(any.type)] = any;
  }
  return {value: obj};
}

var primitiveSymbols = [
  "boolean",
  "bytes",
  "int",
  "long",
  "double",
  "float",
  "null",
  "string"
];

var metaType = avro.parse({
  "logicalType": "meta",
  "type": "record",
  "name": "Meta",
  "fields": [
   {
     "type": [
       {
         "type": "enum",
         "name": "PrimitiveType",
         "symbols": primitiveSymbols,
       },
       {
         "type": "record",
         "name": "Array",
         "fields": [
           {
             "name": "type",
             "type": {
               "type": "enum",
               "name": "ArrayType",
               "symbols": [
                 "array"
               ]
             }
           },
           {
             "type": "Meta",
             "name": "items"
           }
         ]
       },
       {
         "type": "record",
         "name": "Enum",
         "fields": [
           {
             "type": "string",
             "name": "name"
           },
           {
             "type": {
               "type": "enum",
               "name": "EnumType",
               "symbols": [
                 "enum"
               ]
             },
             "name": "type"
           },
           {
             "type": {
               "type": "array",
               "items": "string"
             },
             "name": "symbols"
           }
         ]
       },
       {
         "type": "record",
         "name": "Fixed",
         "fields": [
           {
             "type": "string",
             "name": "name"
           },
           {
             "type": {
               "type": "enum",
               "name": "FixedType",
               "symbols": [
                 "fixed"
               ]
             },
             "name": "type"
           },
           {
             "type": "int",
             "name": "size"
           }
         ]
       },
       {
         "type": "record",
         "name": "Map",
         "fields": [
           {
             "type": {
               "type": "enum",
               "name": "MapType",
               "symbols": [
                 "map"
               ]
             },
             "name": "type"
           },
           {
             "type": "Meta",
             "name": "values"
           }
         ]
       },
       {
         "type": "record",
         "name": "Record",
         "fields": [
           {
             "type": "string",
             "name": "name"
           },
           {
             "type": {
               "type": "enum",
               "name": "RecordType",
               "symbols": [
                 "record"
               ]
             },
             "name": "type"
           },
           {
             "type": {
               "type": "array",
               "items": {
                 "type": "record",
                 "name": "Field",
                 "fields": [
                   {
                     "type": "string",
                     "name": "name"
                   },
                   {
                     "type": "Meta",
                     "name": "type"
                   }
                 ]
               }
             },
             "name": "fields"
           }
         ]
       },
       "string",
       {
         "type": "array",
         "items": "Meta"
       }
     ],
     "name": "value"
   }
  ]
}, {logicalTypes: {'meta': MetaType}, wrapUnions: true});

function capitalize(s) { return s.charAt(0).toUpperCase() + s.slice(1); }

module.exports = {
  metaType : metaType
}
