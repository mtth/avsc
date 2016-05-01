/* jshint browserify: true */

'use strict';

var CodeMirror = require('codemirror/lib/codemirror'),
    avro = require('avsc');


// Force inclusion of CodeMirror resources.
require('codemirror/mode/javascript/javascript');
require('codemirror/addon/display/placeholder');

// To play around with the type, for adventurous people.
window.PROTOCOL = null;

var idlCm, avscCm;
window.onload = function () {
  // Setup editable IDL editor.
  idlCm = CodeMirror.fromTextArea(document.getElementById('idl'), {
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
  idlCm.on('change', createChangeHandler(500));

  // Setup JSON schema read-only editor (sic).
  avscCm = CodeMirror.fromTextArea(document.getElementById('avsc'), {
    lineNumbers: true,
    lineWrapping: false,
    mode: 'application/json',
    placeholder: '// ...and the corresponding JSON schema will appear here.',
    readOnly: true
  });

  // Setup example links.
  var els = document.getElementsByClassName('example');
  var i, l;
  for (i = 0, l = els.length; i < l; i++) {
    els[i].onclick = onExampleClick;
  }
  function onExampleClick(evt) {
    var id = evt.target.getAttribute('data-example-id');
    var example = document.getElementById(id);
    idlCm.setValue(example.textContent);
    return false;
  }
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
      var str = idlCm.getValue();
      if (!str) {
        avscCm.setValue('');
        return;
      }
      assemble(str, function (err, attrs) {
        if (err) {
          attrs = {'@error': {
            message: err.message,
            lineNum: err.lineNum,
            colNum: err.colNum
          }};
        }
        avscCm.setValue(JSON.stringify(attrs, null, 2));
      });
    }, delay);
  };
}

function assemble(str, cb) {
  var opts = {importHook: importHook, reassignJavadoc: true};
  avro.assemble('', opts, function (err, attrs) {
    if (err) {
      cb(err);
      return;
    }
    // Make sure the protocol is valid.
    try {
      window.PROTOCOL = avro.parse(attrs, {wrapUnions: true});
    } catch (parseErr) {
      cb(parseErr);
      return;
    }
    cb(null, attrs);
  });

  function importHook(path, kind, cb) {
    if (path) {
      cb(new Error('imports are not supported in the browser'));
      return;
    }
    cb(null, str);
  }
}
