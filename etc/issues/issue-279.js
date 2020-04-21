/* jshint esversion: 8, node: true */

'use strict';

const avro = require('../../lib');

const registry = {};
avro.Type.forSchema([
{
"namespace": "com.pojo",
"name": "StatusCode",
"type": "enum",
"symbols": [
"NEW",
"RDY",
"STA",
"AP",
"ENQ",
"DLV",
"CON",
"GF",
"DEL"
]
},

{
"namespace": "com.pojo",
"name": "StatusEvent",
"type": "record",
"fields": [
{
"name": "statusTimestamp",
"type": "string"
},
{
"name": "statusCode",
"type": "com.pojo.StatusCode"
},
{
"name": "userName",
"type": [ "null", "string" ],
"default": null
}
]
},

{
"namespace": "com.pojo",
"name": "Company",
"type": "record",
"fields": [
{
"name": "companyId",
"type": "string"
},
{
"name": "companyName",
"type": "string"
}
]
},

{
"namespace": "com.pojo",
"name": "Error",
"type": "record",
"fields": [
{
"name": "errorCode",
"type": "long"
},
{
"name": "errorMessage",
"type": [ "null", "string" ]
}
]
},

{
"namespace": "com.pojo",
"name": "Max_Notification",
"type": "record",
"fields": [
{
"name": "schemaVersion",
"type": "string",
"default": "1.2.0"
},
{
"name": "domain",
"type": "string"
},
{
"name": "messageId",
"type": "string"
},
{
"name": "messageTimestamp",
"type": "string"
},
{
"name": "aomaDspId",
"type": "string"
},
{
"name": "raasProductId",
"type": "string"
},
{
"name": "groupId",
"type": "long"
},
{
"name": "statusEvent",
"type": "com.pojo.StatusEvent"
},
{
"name": "priorStatusEvents",
"type": {
"type": "array",
"items": "com.pojo.StatusEvent"
},
"default": []
},
{
"name": "territories",
"type": {
"type": "array",
"items": "string"
}
},
{
"name": "repOwner",
"type": "com.pojo.Company"
},
{
"name": "maintOwner",
"type": "com.pojo.Company"
},
{
"name": "url",
"type": "string"
},
{
"name": "error",
"type": [ "null", "com.pojo.Error" ],
"default": null
},
{
"name": "clientProperties",
"type": [
"null",
{
"type": "map",
"values": "string"
}
],
"default": null
}
]
}
], {registry});

const type = registry['GroupRe
