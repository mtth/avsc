/* jshint esversion: 8, node: true */

'use strict';

const avro = require('../../lib');

const schema = [
	{
		"type": "record",
		"namespace": "com.vad.wf.api.task",
		"name": "WfControlKey",
		"doc": [
			"Definition of control key",
			"Created based on TC definition XML."
		],
		"fields": [
			{
				"name": "rootInstanceKey",
				"type": [
					"null",
					"string"
				],
				"default": null
			},
			{
				"name": "instanceKey",
				"type": [
					"null",
					"string"
				],
				"default": null
			},
			{
				"name": "controlKey",
				"type": [
					"null",
					"string"
				],
				"default": null
			}
		]
	},
	{
		"type": "record",
		"namespace": "com.vad.wf.api.task",
		"name": "WfTask",
		"fields": [
			{
				"name": "controlKey",
				"type": "com.vad.wf.api.task.WfControlKey"
			},
			{
				"name": "taskTypeNm",
				"type": [
					"null",
					"string"
				],
				"default": null,
				"doc": "Task type name"
			}
		]
	}
];

const type = avro.parse(schema);
console.log(type);

const message = {
    "controlKey": {
        "rootInstanceKey": "bd1f8011-9341-46d2-bb3b-c88c144cd1",
        "instanceKey": "bd1f8011-9341-46d2-bb3b-c8s8cd144cd0",
        "controlKey": "bd1f8011-9341-46d2-bb3b-c88cd1a49sc2d0"
    },
    "taskTypeNm": "add_sub",
};

console.log(type.types[1].toBuffer(message));
