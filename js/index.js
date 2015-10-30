/* jshint browser: true, browserify: true */
(function () {
  'use strict';
  global.jQuery = require("jquery")
  require('jquery-ui');
  var avsc = require('avsc'),
      buffer = require('buffer'),
      $ = require('jquery');
  require('jquery-highlight');
  window.avsc = avsc;
  $( function() {
    resize();
    var encodedErrorElement = $('#encoded-error');
    var decodedErrorElement = $('#decoded-error');
    var encodedValidElement = $('#output-valid');
    var decodedValidElement = $('#input-valid');
    var inputElement = $('#input');
    var outputElement = $('#output');
 
    window.onresize = function(event) {
      resize();
    }
    /* When pasting something into an editable div, it 
     * pastes all the html styles with it too, which need to be cleaned up.
     *copied from: http://stackoverflow.com/questions/2176861/javascript-get-clipboard-data-on-paste-event-cross-browser */
    $('[contenteditable]').on('paste',function(e) {
      e.preventDefault();
      var text = (e.originalEvent || e).clipboardData.getData('text/plain');
      window.document.execCommand('insertText', false, text);
      if(e.target.id === 'schema') {
        validateSchema();
        generateRandom();
      }
    });

    /* Validate schema after each new character. */
    $('#schema').on('keyup', function(e) {
      setTimeout(function(){
         validateSchema();
         generateRandom();
      }, 0);
    });

    $('#input').on('paste keyup', function(event) {
        setTimeout(function() {
        encode();
      }, 0);
    });

    
    $('#output').on('paste keyup', function(event) {
      setTimeout(function() {
        decode();
      }, 0);
    });

    $('#random').click(function () {   
      generateRandom();
    });

    $('#input').on('mouseenter', 'span', function(event) {
      var path = $.trim($(this).attr('class')).split(' ');
      console.log("path is: " + path);
      var current = window.instrumented;
      if (current) {
        for(var i =0; i<path.length; i++){
          var nextKey = path[i];
          if(current.value) 
            current = current.value[nextKey];
          else // The last key can be something like "string", which we don't care about.
            break;
        }
        highlightOutput(current.start, current.end); 
      } else 
        console.log("No instrumented type found");
    }).on('mouseleave', 'span', function(event) {
      clearHighlights();
    });

  function highlightOutput(start, end) {
    console.log(start);
    console.log(end);
    outputElement.children('span').each(function( index ) {
      if (index >= start && index <= end) {
        $(this).addClass("highlight");
      }
    });
  }

  function clearHighlights() {
    
    outputElement.children('span').each(function( index ) {
      $(this).removeClass("highlight");
    });
  }

  function wrapWordsInSpan(element) {
    //element.html(element.text().replace(/\b(\w+)\b/g, "<span>$1</span>"));
    var input = JSON.parse(element.text());
    var output = wrapNodeInSpan(input, "");
    element.html(output);
  }

  function wrapNodeInSpan(node, prefix, indentLevel) {
    var str = "";
    if (node instanceof Array) {
      return writeArray(node, prefix);
    }
    str += "{";
    var commaNeeded = false;
    $.each(node, function(key, value) {
      if(commaNeeded) {
        str += ',';
      }
      console.log ("key : " + key + ", value = " + value);

      str += '<span class="' + prefix + ' ' + key + '">';
      str += '"' + key + '" :';
      str += '</span>';

      if (!value) 
        value = 'null';
      if (isPrimitiveType(value)) {
        str += writeValue(value, prefix + ' ' + key);
      } else if (value instanceof Array) {
        str += writeArray(value, prefix + ' ' + key);
      } else {
        str += wrapNodeInSpan(value, prefix + ' ' + key, indentLevel + 1);
      }
      commaNeeded = true;
    });
    str += "}";
    return str;
  }
  function writeValue(value, prefix, skipQuotation) {
    var str = "";
    console.log('found value: ' +  value);
    str += '<span class="' + prefix + '">';

    if (typeof(value) === 'number' || value === 'null' || skipQuotation)
      str +=  value;
    else 
      str += '"' + value + '"';
    str += '</span>';
    return str;
  }

  function writeArray(array, prefix) {
    var str = "";
    if(array.length == 0) {
      str += '<span class="' + prefix + '">';
      str += '[]';
      str += '</span>';
    } else if (isPrimitiveType(array[0])) {
        var value = "";
        for (var i = 0; i<array.length; i++) {
          if (i > 0) {
            value += ', ';
          }
          value += array[i];
        }
      str += '[' + writeValue(value, prefix, true) + ']';
    } else {
      for (var i = 0; i<array.length; i++) {
        if (i > 0 ) str += ', ';
        str += wrapNodeInSpan(array[i], prefix);
      }
    }
    return str;
  }
  function isPrimitiveType(value) {
    return typeof(value) === 'string' ||
           typeof(value) === 'number' ||
           value === 'null';
  }

   function validateSchema() {
      window.schema = null;
      var elem = $('#schema');
      var valid_elem = $('#schema-valid');
      var error_elem = $('#schema-error');
      try {
        var rawSchema = readSchemaFromInput();
        window.schema = avsc.parse(rawSchema);
        toggleError(error_elem, valid_elem, null);
      } catch (err) {
        toggleError(error_elem, valid_elem, err);
        clearValidIcons();
      }
    }
    function generateRandom() {
      if (window.schema) {
        try{
          var random = window.schema.random();
          var randomStr = window.schema.toString(random);
          var randomJson = JSON.parse(randomStr);
          inputElement.text(JSON.stringify(randomJson, null, 2));
          wrapWordsInSpan(inputElement);
          encode(); /* Update encoded string too. */
        } catch(err) {
          toggleError($('#schema-error'), $('#schema-valid'), err);
        }
      }

    }
    function encode() {
      if (window.schema) {
        try {
          var input = readInput();
          window.instrumented = instrumentObject(window.schema, input);
          var output = window.schema.toBuffer(input);
          outputElement.html(bufferToStr(output));          
          clearErrors();
          toggleError(decodedErrorElement, decodedValidElement, null);
          toggleError(encodedErrorElement, encodedValidElement, null);
        }catch(err) {
          clearErrors();
          clearValidIcons();
          toggleError(decodedErrorElement, decodedValidElement, err);
          clearText(outputElement);
        }
      } else {
        toggleError(decodedErrorElement, decodedValidElement, 'No valid schema found!');
      }
    }

    function decode() {
      if (window.schema) {
        try {
          var input = readBuffer(outputElement);
          var decoded = window.schema.fromBuffer(input);
          var decodedStr = window.schema.toString(decoded);
          var decodedJson = JSON.parse(decodedStr);
          $(inputElement).text(JSON.stringify(decodedJson, null, 2));
          clearErrors();
          toggleError(decodedErrorElement, decodedValidElement, null);
          toggleError(encodedErrorElement, encodedValidElement, null);
        }catch(err) {
          clearErrors();
          clearValidIcons();
          toggleError(encodedErrorElement,encodedValidElement, err);
          clearText(inputElement);
        }
      } else {
        toggleError(encodedErrorElement, encodedValidElement, 'No valid schema found!');
      }
    }

    /* If msg is null, make the valid_element visible, otherwise 
    show `msg` in errorElement. */
    function toggleError(errorElement, valid_element, msg) {
      if(!!msg) {
        errorElement.removeClass('hidden');
        errorElement.text(msg);
        valid_element.addClass('hidden');
      } else {
        errorElement.addClass('hidden');
        errorElement.text("");
        valid_element.show('slow');
      }
    }
 
    /* Clear any error messages shown in input/output boxes. */
    function clearErrors() {
      decodedErrorElement.text('');
      decodedErrorElement.addClass('hidden');
      encodedErrorElement.text('');
      encodedErrorElement.addClass('hidden');
    }

    function clearValidIcons() {
      decodedValidElement.hide("slow");
      encodedValidElement.hide("slow");
    }
    
    function clearText(element) {
      element.text('');
    }
    /* If the schema is pasted with proper json formats, simply json.parse wouldn't work.*/
    function readSchemaFromInput() {
      var trimmedInput = $.trim($('#schema').text()).replace(/\s/g, "");
      return JSON.parse(trimmedInput);
    }

    function readInput() {
      var rawInput = $.trim($(inputElement).text());
      console.log("rawInput");
      console.log(rawInput);
      if(!!window.schema) {
        return window.schema.fromString(rawInput);
      } else {
        return JSON.parse(rawInput);
      }
    }
    /*Used for decoding.
    *Read the text represented as space-seperated hex numbers in elementId
    *and construct a Buffer object*/
    function readBuffer(elementId) {
      var rawInput = $.trim(outputElement.text());
      var hexArray = rawInput.split(/[\s,]+/);
      var i;
      var size = hexArray.length;
      var buffer = [];
      for (i =0; i < size; i++){
        buffer.push(new Buffer(hexArray[i], 'hex'));
      }
      return Buffer.concat(buffer);
    }
    
    function bufferToStr(buffer) {
      var size = buffer.length;
      var outStr = '';
      var i;
      for (i = 1; i <= size; i++) {
        outStr +=  '<span>' + buffer.toString('hex', i-1 , i) + '</span>';
        if (i % 8 == 0 ) {
          outStr += '\n';
        } else {
          outStr += ' ';
        }
      }
      return outStr;
    }
    /* Adjust textbox heights according to current window size */
    function resize() {
      $('#table').removeClass('hidden');
      var vph = $(window).height();
      $('.textbox').css({'height': 0.8 *vph});
    }

    function instrument(schema) {
      if (schema instanceof avsc.types.Type) {
        schema = schema.toString();
      }
      var refs = [];
      return avsc.parse(schema, {typeHook: hook});

      function hook(schema, opts) {
        if (~refs.indexOf(schema)) {
          return;
        }
        refs.push(schema);

        if (schema.type === 'record') {
          schema.fields.forEach(function (f) { f['default'] = undefined; });
        }

        var name = schema.name;
        if (name) {
          schema.name = 'r' + Math.random().toString(36).substr(2, 6);
        }
        var wrappedSchema = {
          name: name || 'r' + Math.random().toString(36).substr(2, 6),
          namespace: schema.namespace,
          type: 'record',
          fields: [{name: 'value', type: schema}]
        };
        refs.push(wrappedSchema);

        var type = avsc.parse(wrappedSchema, opts);
        var read = type._read;
        type._read = function (tap) {
          var pos = tap.pos;
          var obj = read.call(type, tap);
          obj.start = pos;
          obj.end = tap.pos;
          return obj;
        };
        return type;
      }
    }

  /**
   * Convenience method to instrument a single object.
   * 
   * @param type {Type} The type to be instrumented.
   * @param obj {Object} A valid instance of `type`.
   * 
   * Returns an representation of `obj` with start and end markers.
   * 
   */
  function instrumentObject(type, obj) {
    return instrument(type).fromBuffer(type.toBuffer(obj));
  }
 });
})();
