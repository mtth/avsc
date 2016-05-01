/* jshint browserify: true */

'use strict';

var CodeMirror = require('codemirror/lib/codemirror'),
    avro = require('avsc');

// Force inclusion of CodeMirror resources.
require('codemirror/mode/javascript/javascript');
require('codemirror/addon/display/placeholder');

// To play around with the type, for adventurous people.
window.PROTOCOL = null;

var recordCm, avscCm;
window.onload = function () {
  // Setup editable IDL editor.
  recordCm = CodeMirror.fromTextArea(document.getElementById('record'), {
    extraKeys: {
      Tab: function (cm) {
        var spaces = new Array(cm.getOption('indentUnit') + 1).join(' ');
        cm.replaceSelection(spaces);
      }
    },
    lineNumbers: true,
    lineWrapping: false,
    mode: 'text/javascript', // Mostly for comments.
    placeholder: document.getElementById('placeholder'),
    tabSize: 2
  });
  recordCm.on('change', createChangeHandler(500));

  // Setup JSON schema read-only editor (sic).
  avscCm = CodeMirror.fromTextArea(document.getElementById('avsc'), {
    lineNumbers: true,
    lineWrapping: false,
    mode: 'application/json',
    placeholder: '// ...and the corresponding JSON schema will appear here.',
  });

};

// Helpers.

function createChangeHandler(delay) {
  var timeout;
  return function () {
    window.PROTOCOL = null;
    if (timeout) {
      clearTimeout(timeout);
    }
    timeout = setTimeout(function () {
      timeout = undefined;
      var str = recordCm.getValue();
      if (!str) {
        avscCm.setValue('');
        return;
      }
      infer(str, function (schemaJson) {
        avscCm.setValue(JSON.stringify(schemaJson, null, 2));
      });
    }, delay);
  };
}

function infer(str, cb) {
  var inferredSchema = avro.infer(JSON.parse(str));
  cb(JSON.parse(inferredSchema.getSchema({exportAttrs: true})));
}
