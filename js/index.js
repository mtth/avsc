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
    var encodedErrorElement = $('#encoded-error'),
        decodedErrorElement = $('#decoded-error'),
        encodedValidElement = $('#output-valid'),
        decodedValidElement = $('#input-valid'),
        schemaElement = $('#schema'),
        inputElement = $('#input'),
        outputElement = $('#output'),
        arrayKeyRegex = /-(\d+)-/g,
        schemaTypingTimer,
        inputTypingTimer,
        doneTypingInterval = 500; // wait for some time before processing user input.
 
    window.onresize = function(event) {
      resize();
    }
    /* When pasting something into an editable div, it 
     * pastes all the html styles with it too, which need to be cleaned up.
     *copied from: http://stackoverflow.com/questions/2176861/javascript-get-clipboard-data-on-paste-event-cross-browser */
    $('[contenteditable]').on('paste',function(e) {
      e.preventDefault();
      clearTimeout(schemaTypingTimer);

      var text = (e.originalEvent || e).clipboardData.getData('text/plain');
      window.document.execCommand('insertText', false, text);
      if(e.target.id === 'schema') {
        runOnlyIfContentChanged(schemaElement, function () {
          validateSchema();
        });
      }
    });

    
    /* Validate schema after each new character. */
    $('#schema').on('keyup', function(e) {
      clearTimeout(schemaTypingTimer);
      schemaTypingTimer = setTimeout(function () {
        runOnlyIfContentChanged( schemaElement, function () {
          validateSchema();
        });
      }, doneTypingInterval);
    }).on('keydown', function() {
      clearTimeout(schemaTypingTimer);
    });

    $('#input').on('paste keyup', function(event) {

      clearTimeout(inputTypingTimer);
      inputTypingTimer = setTimeout(function() {
        runOnlyIfContentChanged( inputElement, function() {
          //Get current position.
          var range = window.getSelection().getRangeAt(0);
          var el = document.getElementById('input');
          var position = getCharacterOffsetWithin(range, el);
          // Wrap key values in <span>.
          var rawInput = $.trim($(inputElement).text());
          setInputText(rawInput);
          // Set cursor back to `position`
          setCharacterOffsetWithin(range, el, position);
          // Update encoded text.
          encode();
        });
      }, doneTypingInterval);
    }).on('keydown', function() {
      clearTimeout(inputTypingTimer);
    }).on('mouseover', 'span', function(event) {
      if (window.instrumented) {
         /* It's important to clear it when the mouse moves from one span to another with the same parent,
           * to clear out the parent being highlighted. */
        clearHighlights();

        /*Will also automatically highlight all nested children.*/
        $(this).addClass('highlight'); 

        /*So that the parent won't be highlighted (because we are using mouseover and not mouseenter)*/
        event.stopPropagation(); 


        var path = getPath($(this));
        var position = findPositionOf(path);
        highlightOutput(position.start, position.end); 
      } else 
        console.log("No instrumented type found");
    }).on('mouseleave', 'span', function(event) {
      clearHighlights();
    });

    $('#output').on('paste keyup', function(event) {
      setTimeout(function() {
        decode();
      }, 0);
    });

    $('#random').click(function () {   
      generateRandom();
    });

  
    /*
    * When the input text changes, the whole text is replaced with new <span> elements,
    * and the previous cursor position will be lost. 
    *
    * This function will go through all the child elements of `node` and sets the
    * caret to the `position`th character.
    */ 

    function setCharacterOffsetWithin(range, node, position) {
      var treeWalker = document.createTreeWalker(
          node,
          NodeFilter.SHOW_TEXT
      );
      var charCount = 0, foundRange = false;
      while (treeWalker.nextNode() && !foundRange) {
          if (charCount + treeWalker.currentNode.length < position)
            charCount += treeWalker.currentNode.length;
          else {
            var newRange = document.createRange();
            newRange.setStart(treeWalker.currentNode, position - charCount);
            newRange.setEnd(treeWalker.currentNode, position - charCount);
            newRange.collapse(true);

            var sel = window.getSelection();
            sel.removeAllRanges();
            sel.addRange(newRange);
            foundRange = true;
          }
      }
    }
    
    /**
    * From: http://jsfiddle.net/timdown/2YcaX/
    * http://stackoverflow.com/questions/4767848/get-caret-cursor-position-in-contenteditable-area-containing-html-content
    */
    function getCharacterOffsetWithin(range, node) {
      var treeWalker = document.createTreeWalker(
          node,
          NodeFilter.SHOW_TEXT,
          function(node) {
              var nodeRange = document.createRange();
              nodeRange.selectNode(node);
              return nodeRange.compareBoundaryPoints(Range.END_TO_END, range) < 1 ?
                  NodeFilter.FILTER_ACCEPT : NodeFilter.FILTER_REJECT;
          },
          false
      );
      var charCount = 0;
      while (treeWalker.nextNode()) {
          charCount += treeWalker.currentNode.length;
      }
      if (range.startContainer.nodeType == Node.TEXT_NODE) { 
          charCount += range.startOffset;
      }
      return charCount;
    }

    /**
    * Goes through all parents of an element, and concatenates
    * their classes to generate the path for the given key.  
    */
    function getPath(element) {
      var selfClass = $.trim(element.attr('class').replace('highlight', ''));
      var parents = element.parents('span').map(function () {
        // How should we handle if there is a field called highlight? --> todo: maybe replace all highlights with -highligh? 
        var parentClass = $(this).attr('class').replace('highlight', '');
        return $.trim(parentClass);
      }).get();
      parents.reverse(); /* parents() will go through parents starting from the inner most,
                            so it needs to be reversed to get the correct path. */

      parents.push(selfClass); /* The innermost class is not part of the parents. Adding it here. */
                           
      console.log("found parents: " + parents);
      return parents;
    }

  /**
  * find the start and end index of an entry in its encoded representation
  * using the instrumented type already loaded in window.instrumented.
  *
  */
  function findPositionOf(path) {
    var current = window.instrumented;
    path.forEach(function(entry) {
      var arrayKey = arrayKeyRegex.exec(entry);
      var nextKey = arrayKey ? arrayKey[1] : entry;
      if (nextKey in current.value) {
        current = current.value[nextKey];
      } else {
        $.each(current.value, function(k,v) {
          current = v;
          return false;
        });
      }
    });
    return current;
 }

  /**
  * Find all spans that have the same class, and highlights them,
  * so if a key is selected, its value will be also highlighted, and vice versa.  
  */
  function highlightAllMatching(classesString) {
    var rawClasses = classesString[0] == ' ' ? classesString : ' ' + classesString;
    rawClasses = rawClasses.replace(/ /g, ' .');
    $(rawClasses).each( function(i) {
      $(this).addClass('highlight');
    });
  }

  /**
  * Highlight the entries between `start` and `end` in the output (encoded) text.
  */  
  function highlightOutput(start, end) {
    outputElement.children('span').each(function( index ) {
      if (index >= start && index < end) {
        $(this).addClass("highlight");
      }
    });
  }


  /**
  * Remove `highlight` from all spans. 
  */
  function clearHighlights() {
    $('span').removeClass('highlight');
  }

  /**
  * set the input box's text to inputStr, 
  * where all key, values are wrapped in <span> elements
  * with the 'path' set as the span class. 
  */
  function setInputText(inputStr) {
    var input = JSON.parse(inputStr);
    var stringified = stringify(input, 1); 
    inputElement.html(stringified);
  } 

  /**
  * Similar to JSON.stringify, but will wrap each key and value 
  * with <span> tags. 
  * Does a DFS over the obj, to propagate the parent keys to each 
  * child element to be set in the span's class attribute.
  * @param obj The object to stringify
  * @param depth Current indention level.
  */
  function stringify(obj, depth) {

    var res = '';
    if ( obj == null ) {
      return 'null';
    }
    if (typeof obj === 'number' || typeof obj === 'boolean') {
      return  obj + '';
    }
    if (typeof obj === 'string') {
      // Calling json.stringify here to handle the fixed types.
      // I have no idea why just printing them doesn't work.
      return JSON.stringify(obj) ;
    }
    var comma = false;
    if (obj instanceof Array) {
      res += '[<br/>';
      $.each(obj, function(index, value) {
        if (comma) res += ',<br/>';
        // Use '-' as a special character, which can not exist in schema keys but is a valid character for css class.
        res += '<span class="-' + index +  '-">' + indent(depth) + stringify(value, depth + 1) + '</span>';
        comma = true;
      });
      res += '<br/>' + indent(depth - 1) + ']';
      return res;
    } 
    res += '{<br/>';
    comma = false;
    $.each(obj, function(key, value) {
      if (comma) res += ',<br/>';
      res += '<span class="' + key + '">' + indent(depth) + '"' + key + '":' + stringify(value, depth + 1) + '</span>';
      comma = true;
    });
    res += '<br/>' + indent(depth - 1) + '}';
    return res;

  }

  function indent(depth) { 
    var res = '';
    for (var i = 0 ; i < 2 * depth; i++) res += ' ';
    return res;
  }

   function validateSchema() {
      window.schema = null;
      var elem = $('#schema');
      var valid_elem = $('#schema-valid');
      var error_elem = $('#schema-error');
      try {
        var rawSchema = readSchemaFromInput();
        $(schemaElement).text(JSON.stringify(rawSchema, null, 2));
        clearTimeout(schemaTypingTimer);
        window.schema = avsc.parse(rawSchema);
        generateRandom();
        toggleError(error_elem, valid_elem, null);
      } catch (err) {
        toggleError(error_elem, valid_elem, err);
      }
    }
    function generateRandom() {
      if (window.schema) {
        try{
          var random = window.schema.random();
          var randomStr = window.schema.toString(random);
          //var randomJson = JSON.parse(randomStr);
          //inputElement.text(JSON.stringify(randomJson, null, 2));
          setInputText(randomStr);
          encode(); /* Update encoded string too. */
        } catch(err) {
          toggleError($('#schema-error'), $('#schema-valid'), err);
        }
      }
    }

    /**
    * Read the input as text from inputElement.
    * Instrument it and update window.instrumented.
    * Encode it and set the outputElement's text to the encoded data
    */   
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
        valid_element.show('slow').delay(500).hide('slow');
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

    function runOnlyIfContentChanged(element, callback) {
      var newText = $.trim($(element).text()).replace(/\s+/g, '');
      if (!element.data('oldValue') || 
          element.data('oldValue') != newText) {
        element.data('oldValue', newText);
        callback.call();
      }
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
