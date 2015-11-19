/* jshint browser: true, browserify: true */
var cache = {},
    guidCounter = 1,
    dataKey = 'data'  + (new Date).getTime();
 
(function () {
  'use strict';
  global.jQuery = require("jquery")
  require('jquery-ui');
  var avsc = require('avsc'),
      buffer = require('buffer'),
      $ = require('jquery');
  window.avsc = avsc;
  $( function() {

    var encodedErrorElement = $('#encoded-error'),
        decodedErrorElement = $('#decoded-error'),
        encodedValidElement = $('#output-valid'),
        decodedValidElement = $('#input-valid'),
        schemaErrorElement = $('#schema-error'),
        schemaValidElement = $('#schema-valid'),
        schemaElement = document.getElementById('schema'),
        inputElement = $('#input'),
        outputElement = $('#output'),
        width = 100,
        body = document.getElementsByTagName('body')[0],
        arrayKeyPattern = /-(\d+)-/g,
        reservedKeysPattern = /-[a-z]+-/g,
        typingTimer,
        doneTypingInterval = 500; // wait for some time before processing user input.
    
    resize();
    window.onresize = function(event) {
      resize();
    }
    window.reverseIndexMap = [];  
       
    /* When pasting something into an editable div, it 
     * pastes all the html styles with it too, which need to be cleaned up.
     * copied from: 
     * http://stackoverflow.com/questions/2176861/javascript-get-clipboard-data-on-paste-event-cross-browser */

    $('[contenteditable]').on('paste',function(e) {
      e.preventDefault();
      clearTimeout(typingTimer);

      var text = (e.originalEvent || e).clipboardData.getData('text/plain');
      window.document.execCommand('insertText', false, text);
      if(e.target.id === 'schema') {
        if(updateContent(schemaElement)) {
          triggerEvent(body, 'schema-changed');
          generateRandom();
        };
      }
    });

    $('#schema').on('keyup', function() {
      clearTimeout(typingTimer);
      typingTimer = setTimeout(function () {
        if(updateContent(schemaElement)) {
          triggerEvent(body, 'schema-changed');
        }
      }, doneTypingInterval);
    }).on('keydown', function() {
      clearTimeout(typingTimer);
    });

    $('#input').on('paste keyup', function(event) {

      clearTimeout(typingTimer);
      typingTimer = setTimeout(function() {
        if(updateContent(inputElement)) {
          triggerEvent(body, 'input-changed');
        };
      }, doneTypingInterval);
    }).on('keydown', function() {
      clearTimeout(typingTimer);
    }).on('mouseover', 'span', function(event) {
      if (window.instrumented) {
         /* It's important to clear it when the mouse moves from one span to another with the same parent,
           * to clear out the parent being highlighted. */
        clearHighlights();

        /*Will also automatically highlight all nested children.*/
        $(this).addClass('-highlight-'); 

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
      clearTimeout(typingTimer);
      typingTimer = setTimeout(function() {
        if(updateContent(outputElement)) {
          triggerEvent(body, 'output-changed');
        };
      }, doneTypingInterval);
    }).on('keydown', function(event) {
      clearTimeout(typingTimer);
    });

    $('#random').click(function () {   
      clearTimeout(typingTimer);
      typingTimer = setTimeout(function() {
        generateRandom();
      }, doneTypingInterval);
    });

    addEvent(body, 'schema-changed', function(e) {
      runPreservingCursorPosition( 'schema', validateSchema);
    });

    addEvent(body, 'input-changed', function(e) {
      runPreservingCursorPosition( 'input' , function () {
        var rawInput = $.trim($(inputElement).text());        
        try {
          var inputJson = JSON.parse(rawInput);
          var inputRecord = window.schema._copy(inputJson, {coerce: 2});
          var invalidPaths = [];
          var isValid = window.schema.isValid(inputRecord, {errorHook: function (p) {
            invalidPaths.push(p.join());
          }});
          if(!isValid) {
            triggerEvent(body, 'invalid-input', invalidPaths);
          } else {
            triggerEvent(body, 'valid-input');
          }
          // Wrap key values in <span>.
          setInputText(rawInput);
        } catch (err) {
          triggerEvent(body, 'invalid-input', err);
        }
      });
      encode();
    });

    addEvent(body, 'output-changed', function(e) {
      runPreservingCursorPosition( 'output', function() {
        var rawOutput = $.trim($(outputElement).text());
        setOutputText(rawOutput);
      });
      decode();
    });

    addEvent(body, 'valid-schema', function(event) {
      hideError(schemaErrorElement, schemaValidElement);
    });

    addEvent(body, 'invalid-schema', function (event) {
      showError(schemaErrorElement, event.msg);
    });

    addEvent(body, 'valid-input', function (event) { 
      hideError(decodedErrorElement, decodedValidElement);
    });

    addEvent(body, 'invalid-input', function(event) {
      showError(decodedErrorElement, event.msg);
    });

    addEvent(body, 'valid-output', function (event) { 
      hideError(encodedErrorElement, encodedValidElement);
    });

    addEvent(body, 'invalid-output', function(event) {
      showError(encodedErrorElement, event.msg);
    });

    /**
    * Will save cursor position inside element `elemId` before running callback function f,
    * and restores it after f is finished. 
    */ 
    function runPreservingCursorPosition(elementId, f) {
     //Get current position.
      var range = window.getSelection().getRangeAt(0);
      var el = document.getElementById(elementId);
      var position = getCharacterOffsetWithin(range, el);
      f();
      setCharacterOffsetWithin(range, el, position);
    } 
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
      var selfClass = $.trim(element.attr('class').replace(reservedKeysPattern, ''));
      var parents = element.parents('span').map(function () {
        var parentClass = $(this).attr('class').replace(reservedKeysPattern, '');
        return $.trim(parentClass);
      }).get();
      parents.reverse(); /* parents() will go through parents starting from the inner most,
                            so it needs to be reversed to get the correct path. */

      if (selfClass != '' ) 
        parents.push(selfClass); /* The innermost class is not part of the parents. Adding it here. */
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
      var arrayKey = arrayKeyPattern.exec(entry);
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

  /*
  * Find all spans that have the same class, and highlights them,
  * so if a key is selected, its value will be also highlighted, and vice versa.  
  */
  function highlightAllMatching(classesString) {
    var rawClasses = classesString[0] == ' ' ? classesString : ' ' + classesString;
    rawClasses = rawClasses.replace(/ /g, ' .');
    $(rawClasses).each( function(i) {
      $(this).addClass('-highlight-');
    });
  }

  /**
  * Highlight the entries between `start` and `end` in the output (encoded) text.
  */  
  function highlightOutput(start, end) {
    outputElement.children('div').each(function( index ) {
      if (index >= start && index < end) {
        $(this).addClass("-highlight-");
      }
    });
  }

  function addClassToOutputWithRange(cls, start, end) {
    outputElement.children('span').each(function( index ) {
      if (index >= start && index < end) {
        $(this).addClass(cls);
      }
    });
    for (var i = start; i < end; i++) {
      if (reverseIndexMap[i]) {
        reverseIndexMap[i].push(cls);
      } else {
        reverseIndexMap[i] = [cls];
      }
    }
  }
  

  /**
  * Remove `highlight` from all spans. 
  */
  function clearHighlights() {
    $('span').removeClass('-highlight-');
    $('div').removeClass('-highlight-');
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
  * Set the output box's text to outputStr where each byte is wrapped in <span>
  * elements and each line contains 8 bytes.
  */ 

  function setOutputText(outputStr) { 
    var res = '';
    var str = outputStr.replace(/\s+/g, '');
    var i, len;
    for (i =0, len = str.length; i < len; i += 2){
      res += '<div style="display:inline-block;">' + str[i] + str[i + 1] + '&nbsp;' + '</div>';
    }
    outputElement.html(res);
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
      return createSpan('-null-', 'null');
    }
    if (typeof obj === 'number') {
      return  createSpan('-number-', obj + '');
    }
    if (typeof obj === 'boolean') {
      return createSpan('-boolean-', obj + '');
    }
    if (typeof obj === 'string') {
      // Calling json.stringify here to handle the fixed types.
      // I have no idea why just printing them doesn't work.
      return createSpan('-string-', JSON.stringify(obj));
    }
    var comma = false;
    if (obj instanceof Array) {
      res += '[<br/>';
      $.each(obj, function(index, value) {
        if (comma) res += ',<br/>';
        // Use '-' as a special character, which can not exist in schema keys 
        // but is a valid character for css class.
        res += createSpan('-' + index + '-', indent(depth) + stringify(value, depth + 1));
        comma = true;
      });
      res += '<br/>' + indent(depth - 1) + ']';
      return res;
    } 
    res += '{<br/>';
    comma = false;
    $.each(obj, function(key, value) {
      if (comma) res += ',<br/>';
      res += createSpan(key, indent(depth) + '"' + key + '": ' + stringify(value, depth + 1));
      comma = true;
    });
    res += '<br/>' + indent(depth - 1) + '}';
    return res;
  }

  function createSpan(cl, str) {
    return '<span class="' + cl + '">' + str + '</span>'; 
  }
  function indent(depth) { 
    var res = '';
    for (var i = 0 ; i < 2 * depth; i++) res += ' ';
    return res;
  }

   function validateSchema() {
      window.schema = null;
      try {
        var rawSchema = readSchemaFromInput();
        $(schemaElement).text(JSON.stringify(rawSchema, null, 2));
        window.schema = avsc.parse(rawSchema);
        triggerEvent(body, 'valid-schema');
      } catch (err) {
        triggerEvent(body, 'invalid-schema', err);
      }
    }
    function generateRandom() {
      if (window.schema) {
        var random = window.schema.random();
        var randomStr = window.schema.toString(random);
        setInputText(randomStr);
        triggerEvent(body, 'input-changed');
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
          setOutputText(output.toString('hex'));
          triggerEvent(body, 'valid-output');
        }catch(err) {
          triggerEvent(body, 'invalid-input', err);
        }
      }
    }

    function decode() {
      if (window.schema) {
        try {
          var input = readBuffer(outputElement);
          var decoded = window.schema.fromBuffer(input);
          var decodedStr = window.schema.toString(decoded);
          triggerEvent(body, 'valid-output');
          setInputText(decodedStr);
        }catch(err) {
          triggerEvent(body, 'invalid-output', err);
        }
      }
    }

    function showError(errorElem, msg) {
      errorElem.text(msg);
      errorElem.removeClass('hidden');
    };

    function hideError(errorElem, validElem) {
      errorElem.text("");
      errorElem.addClass('hidden');
      validElem.show('slow').delay(500).hide('slow');
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
    
    /* Adjust textbox heights according to current window size */
    function resize() {
      $('#table').removeClass('hidden');
      var vph = $(window).height();
      $('.textbox').css({'height': 0.8 *vph});
      width = outputElement.width();
    }

    /**
     * Will update the old value of the element to the new one, 
     * and returns true if the content changed. 
    */
    function  updateContent(element) {
      var newText = $.trim($(element).text()).replace(/\s+/g, '');
      if (!element.data) {
        element.data = {};
      }
      if (!element.data['oldValue'] || 
          element.data['oldValue'] != newText) {
        element.data['oldValue'] =  newText;
        return true;
      }
      return false;
    }

    function instrument(schema) {
      if (schema instanceof avsc.types.Type) {
        schema = schema.getSchema();
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

/* Get the associated data of `elem` from the global cache.
 * (Will create a new entry in cache the first time called for an `elem`.)
*/

var getData = function (elem) {
  var guid = elem[dataKey];
  if (!guid) {
    guid = guidCounter;
    elem[dataKey] = guidCounter;
    guidCounter++;
    cache[guid] = {};
  }
  return cache[guid];
};

/**
 *  Remove corresponding data of the given `elem` from cache.
 */
var removeData = function(elem) {
  var guid = elem[dataKey];
  if (!guid) {
    return;
  }
  delete cache[guid];
};

/**
 *  Bind event handlers.
 */

var addEvent = function (elem, eventType, fn) {
  var data = getData(elem);
  // Initialize stuff if it's the first time.
  if (!data.handlers) {
    data.handlers = {};
  }
  if (!data.handlers[eventType]){
    data.handlers[eventType] = [];
  }

  // Using the same global guidCounter because it doesn't really matter.
  // TODO: why was this needed again? :-/
  if (!fn.guid) {
    fn.guid = guidCounter++;
  }

  // Actually add the function as an event handler.
  data.handlers[eventType].push(fn);

  // Initialize the dispatcher.
  if (!data.dispatcher) {
    data.disabled = false;
    data.dispatcher = function(event) {
      if (data.disabled) {
        return; 
      }

      var handlers = data.handlers[event.type];
      if (handlers) {
        for(var i =0; i < handlers.length; i++ ) {
          handlers[i].call(elem, event);
        }
      }
    }
  }

  // Register the dispatcher as the actual event handler.
  if(data.handlers[eventType].length == 1) {
   elem.addEventListener(eventType, data.dispatcher, false); 
  }
};

/**
 * TODO: implement tidy up and unbind methods.
 */

var triggerEvent = function (elem, event, msg) {
  var parent = elem.parentNode,
      elemData = getData(elem);
  if (typeof event === 'string') {
    event = {
      type: event,
      target: elem,
      msg: msg
    }
  }
  if (elemData.dispatcher) {
    elemData.dispatcher.call(elem, event);
  }

  if (parent) {
    triggerEvent(parent, event)
  }

  // TODO: probably do sth here if the element doesn't have a parent.
};
