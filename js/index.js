/* jshint browser: true, browserify: true */

(function () {
  'use strict';
  global.jQuery = require("jquery")
  var avsc = require('avsc'),
      buffer = require('buffer'),
      utils = require('./utils'),
      meta = require('./meta'),
      $ = require('jquery');
  window.avsc = avsc;
  $( function() {

    var outputErrorElement = $('#encoded-error'),
        inputErrorElement = $('#decoded-error'),
        encodedValidElement = $('#output-valid'),
        decodedValidElement = $('#input-valid'),
        schemaErrorElement = $('#schema-error'),
        schemaValidElement = $('#schema-valid'),
        schemaSelect = $('#schema-template'),
        template = $('#template'),
        schemaElement = $('#schema'),
        inputElement = $('#input'),
        outputElement = $('#output'),
        randomElement = $('#random'),
        uploadElement = $('#upload'),
        firstPageElements = $('.-level1-'),
        secondPageElements = $('.-level2-'),
        arrayKeyPattern = /(\d+)/g,
        reservedKeysPattern = /-[a-z]+-/g,
        whiteSpacePattern = /\s+/g,
        typingTimer,
        eventObj = utils.eventObj,
        urlUtils = utils.urlUtils,
        metaType = meta.metaType,
        doneTypingInterval = 500; // wait for some time before processing user input.
    
    window.reverseIndexMap = [];
    window.metaType = metaType;

    eventObj.on('schema-changed', function(schemaJson) {
      template.hide();
      var schemaStr = JSON.stringify(schemaJson, null, 2); 
      runPreservingCursorPosition('schema', schemaElement.text, {context: schemaElement, param: schemaStr});
      // encode schema here.
      eventObj.trigger('update-url', {schema:schemaStr});
      validateSchema(schemaJson);
    }).on('input-changed', function(rawInput) {
      try {
        runPreservingCursorPosition('input' , setInputText, {context: inputElement, param: rawInput});
        // Wrap key values in <span>.
        validateInput(rawInput);
        eventObj.trigger('re-instrument', rawInput);
        encode(rawInput);
      } catch (err) {
        eventObj.trigger('invalid-input', err);
      }
    }).on('output-changed', function(outputStr) {
      decode(outputStr);
    }).on('valid-schema', function() {
      hideError(schemaErrorElement, schemaValidElement, 'schema');
    }).on('invalid-schema', function (message) {
      randomElement.fadeOut('slow');
      showError(schemaErrorElement, message);
    }).on('valid-input', function () { 
      hideError(inputErrorElement, decodedValidElement);
    }).on('invalid-input', function(message) {
      showError(inputErrorElement, message);
    }).on('valid-output', function () { 
      hideError(outputErrorElement, encodedValidElement);
    }).on('invalid-output', function(message) {
      showError(outputErrorElement, message);
    }).on('update-layout', function() {
      if (window.type) {
        firstPageElements.each(function(i, element) {
          $(element).addClass('-hidden-');
        });
        secondPageElements.each(function (i, element) {
          $(element).removeClass('-hidden-');
        });
      }
    }).on('reset-layout', function() {
      $.clearQueue();
      firstPageElements.each(function(i, element) {
        $(element).removeClass('-hidden-');
      });
      secondPageElements.each(function (i, element) {
        $(element).addClass('-hidden-');
      });
      schemaElement.text("");
      inputElement.text("");
      outputElement.text("");
      hideError(schemaErrorElement);
      hideError(inputErrorElement);
      hideError(outputErrorElement);
      randomElement.hide();
      template.show();
    }).on('schema-loaded', function(rawSchema) {
      template.hide();
      eventObj.trigger('update-url', {'schema': rawSchema});
      populateFromQuery();
      eventObj.trigger('update-layout');
    }).on('re-instrument', function(rawInput) {
      window.instrumented = instrumentObject(window.type, window.type.fromString(rawInput));
      window.reverseIndexMap = computeReverseIndex(window.instrumented);
    }).on('update-url', function(data) {
      var state = {};
      var newUrl = location.href;
      if (data.schema) {
        // encode schema here..
        var jsonSchema = JSON.parse(data.schema);
        var encodedSchema = metaType.toBuffer(jsonSchema);
        data.schema = encodedSchema.toString('hex');
      }

      newUrl = urlUtils.updateValues(newUrl, data);
      // Use this so that it doesn't reload the page, but that also means that you need to manually
      // load the schema from url
      window.history.pushState(state, 'AVSC', newUrl);

    }).on('generate-random', function() {
      generateRandom();
    }).on('schema-uploaded', function(files) {
      var file = files[0]; 
      var reader = new FileReader();
      reader.readAsText(file, "UTF-8");
      reader.onload = function (evt) {
        eventObj.trigger('schema-loaded', evt.target.result);
      }
      reader.onerror = function (evt) {
        console.log("error reading file.");
      }
    });

    schemaSelect.on('change', function() {
      loadTemplate(this.value);
    });

       
    /* When pasting something into an editable div, it 
     * pastes all the html styles with it too, which need to be cleaned up.
     * copied from: 
     * http://stackoverflow.com/questions/2176861/javascript-get-clipboard-data-on-paste-event-cross-browser */

    $('[contenteditable]').on('paste',function(e) {
      e.preventDefault();
      //TODO: Find out why sometimes it triggers 'input-changed' twice.
      var text = (e.originalEvent || e).clipboardData.getData('text/plain');
      window.document.execCommand('insertText', false, text);
      if(e.target.id === 'schema') {
        if(updateContent(schemaElement)) {
          var schemaJson = readSchemaFromInput();
          eventObj.trigger('schema-changed', schemaJson);
          eventObj.trigger('generate-random');
        };
      }
    });

    schemaElement.on('keyup', function() {
      clearTimeout(typingTimer);
      typingTimer = setTimeout(function () {
        if(updateContent(schemaElement)) { 
          try {
            var schemaJson = readSchemaFromInput();
            eventObj.trigger('schema-changed', schemaJson);
            validateInput();
          } catch(err) {
            eventObj.trigger('invalid-schema', err);
          }
        }
      }, doneTypingInterval);
    }).on('click keydown', function() {
      template.hide();
    }).on('keydown', function() {
      clearTimeout(typingTimer);
    }).on('drop', function (e) {
      e.preventDefault();
      e.stopPropagation();
      var files = e.originalEvent.dataTransfer.files;
      eventObj.trigger('schema-uploaded', files);
    });

    inputElement.on('paste keyup', function(event) {
      clearTimeout(typingTimer);
      typingTimer = setTimeout(function() {
        if(updateContent(inputElement)) {
          var rawInput = $.trim(inputElement.text());        
          eventObj.trigger('input-changed', rawInput);
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
        highlight($(this)); 

        /*So that the parent won't be highlighted (because we are using mouseover and not mouseenter)*/
        event.stopPropagation(); 

        var path = getPath($(this));
        var position = findPositionOf(path);
        highlight(position); 
      } else {
        console.log("No instrumented type found");
      }
    }).on('mouseleave', 'span', function(event) {
      clearHighlights();
    });

    outputElement.on('paste keyup', function(event) {
      clearTimeout(typingTimer);
      typingTimer = setTimeout(function() {
        if(updateContent(outputElement)) {
          var outputStr = $.trim(outputElement.text());
          eventObj.trigger('output-changed', outputStr);
        };
      }, doneTypingInterval);
    }).on('mouseover', 'div', function(event) {
      if (window.reverseIndexMap) {
        clearHighlights();
        event.stopPropagation();
        
        var path = getPath(this);

        //The .find() and .children() methods are similar, 
        //except that the latter only travels a single level down the DOM tree.
        var inputCandidates = inputElement.find('.' + path.join(' .'));

        // Go through all input candidates and make sure the `full path` matches 
        // (and in the same order) and then find its position to be highlighted.

        $.each(inputCandidates, (function(idx, e) { 
          var cs = getPath(e);
          if (utils.arraysEqual(cs, path)) {
            highlight($(e)); // highlight input
            var p = getPath(e); // find path
            var position = findPositionOf(p); // find the indexes in the output
            highlight(position); // highlight them in output
          }
        }));
      }
    }).on('mouseleave', 'div', function (event) { 
      clearHighlights(); 
    }).on('keydown', function(event) {
      clearTimeout(typingTimer);
    });

    randomElement.click(function () {   
      clearTimeout(typingTimer);
      typingTimer = setTimeout(function() {
        eventObj.trigger('generate-random');
      }, doneTypingInterval);
      return false;
    });

    $("#reset").click(function() {
      eventObj.trigger('update-url', {'schema' : '' , 'record' : ''});
      eventObj.trigger('reset-layout');
      window.type = undefined;
      return false;
    });

    $('#uploadLink').click(function(e) {
      e.preventDefault();
      uploadElement.trigger('click');
      return false; // So that it doesn't show the content of the file.
    });

    uploadElement.on("change", function(e) {
      var files = $(uploadElement)[0].files;
      eventObj.trigger('schema-uploaded', files);
    });

    $(document).click(function(e) {
      if(!$(e.target).closest('#schema').length) {
        if (!schemaElement.text()){
          template.show();
          hideError(schemaErrorElement);
        }
      }
    });

    function populateFromQuery() {
      var s = urlUtils.readValue('schema');
      if(s) {
        // decode schema.
        var encodedSchema = new Buffer(s, 'hex');
        var decodedSchema = metaType.fromBuffer(encodedSchema);
        eventObj.trigger('schema-changed', decodedSchema);
      }
      
      var record = urlUtils.readValue('record');
      if(record) {
        decode(record);
        setOutputText(record);
      }
    }
    
    /**
    * Will save cursor position inside element `elemId` before running callback function f,
    * and restores it after f is finished. 
    */ 
    function runPreservingCursorPosition(elementId, f, options) {
      var context = options && options.context;
      var param = options && options.param;
     //Get current position.
      if (window.getSelection().rangeCount) {

        var range = window.getSelection().getRangeAt(0);
        var el = document.getElementById(elementId);
        var position = getCharacterOffsetWithin(range, el);
        f.call(context, param);
        setCharacterOffsetWithin(range, el, position);
      } else {
        f.call(context, param);
      }
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

    /* Get full path of an element based on its css class attributes. (both inherited 
     * or direct)*/
    function getPath(element) {
      var cs = [];
      /* We can't just read the .attr() parameter, because we need the 
       * class properties of the parents too.*/
      $(element)
        .parentsUntil($('.-textbox-'))
        .andSelf()
        .each(function() {
          if(this.className) {
            /* This should be a concat because the result of split is already an array */
            cs = cs.concat($.trim(this.className.replace(reservedKeysPattern,''))
                            .split(' '));
          }
      });
      return cs;
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
        var nextKey = arrayKey ? arrayKey[1] : entry; // getting the first captured group from regex result if a match was found.
        if (!(nextKey in current.value)) {
          nextKey = nextKey + '_';
        }
        if (nextKey in current.value) {
          current = current.value[nextKey];
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
        highlight($(this));
      });
    }

    /**
    * Add -highlight- to the input element class, 
    * or highlight entries between 'start' and 'end' in the output text.
    */
    function highlight(input) {
      if (input.start !== undefined && input.end !== undefined){
        outputElement.children('div').each(function( index ) {
          if (index >= input.start && index < input.end) {
            highlight($(this));
          }
        });
      } else {
        input.addClass('-highlight-');
      }
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
        res += createDiv(window.reverseIndexMap[i/2], str[i] + str[i + 1] + '&nbsp;');
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
        return createDiv('-null-', 'null');
      }
      if (typeof obj === 'number') {
        return  createDiv('-number-', obj + '');
      }
      if (typeof obj === 'boolean') {
        return createDiv('-boolean-', obj + '');
      }
      if (typeof obj === 'string') {
        // Calling json.stringify here to handle the fixed types.
        // I have no idea why just printing them doesn't work.
        return createDiv('-string-', JSON.stringify(obj));
      }
      var comma = false;
      if (obj instanceof Array) {
        res += '[<br/>';
        $.each(obj, function(index, value) {
          if (comma) res += ',<br/>';
          // Use '-' as a special character, which can not exist in schema keys 
          // but is a valid character for css class.
          res += createSpan(index , indent(depth) + stringify(value, depth + 1));
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

    function createDiv(cl, str) {
      return '<div class="-inline- ' + cl + '">' + str + '</div>';
    }
    function indent(depth) { 
      var res = '';
      for (var i = 0 ; i < 2 * depth; i++) res += ' ';
      return res;
    }

    function validateInput(rawInput) {
      if (window.type) {
        try {
          if (!rawInput) {
            rawInput = $.trim(inputElement.text());
          }
          var attrs = JSON.parse(rawInput);
          // Throw more useful error if not valid.
          window.type.isValid(attrs, {errorHook:
            function(path, any, type) {
              if (
                typeof any == 'string' &&
                ( 
                  type instanceof avsc.types.BytesType ||
                  (
                    type instanceof avsc.types.FixedType &&
                    any.length === type.getSize()
                  )
                )
              ) {
                // This is a string-encoded buffer.
                return;
              }
              throw new Error('invalid ' + type + ' at ' + path.join('.'));
            }
          });
          eventObj.trigger('valid-input');
        } catch (err) {
          eventObj.trigger('invalid-input', err);
        }
      }
    }

    function validateSchema(schemaJson) {
      window.type = undefined;
      try {
        window.type = avsc.parse(schemaJson, {wrapUnions: true});
        eventObj.trigger('valid-schema');
        eventObj.trigger('update-layout');
      } catch (err) {
        eventObj.trigger('invalid-schema', err);
      }
    }
    function generateRandom() {
      if (window.type) {
        try{
          var random = window.type.random();
          var randomStr = window.type.toString(random);
          setInputText(randomStr);
          eventObj.trigger('input-changed', randomStr);
        } catch (err) {
          eventObj.trigger('invalid-input', err);
        }
      }
    }

    /**
    * Read the input as text from inputElement.
    * Instrument it and update window.instrumented.
    * Encode it and set the outputElement's text to the encoded data
    */   
    function encode(inputStr) {
      if (window.type) {
        try {
          var input = window.type.fromString(inputStr);
          var output = window.type.toBuffer(input);
        
          var outputStr = output.toString('hex');
          setOutputText(outputStr);
          eventObj.trigger('update-url', {'record' : outputStr});
          eventObj.trigger('valid-output');
        }catch(err) {
          eventObj.trigger('invalid-input', err);
        }
      }
    }

    function decode(rawInput) {
      if (window.type) {
        try {
          var input = readBuffer(rawInput);
          var decoded = window.type.fromBuffer(input);
          var decodedStr = window.type.toString(decoded);
          setInputText(decodedStr);
          eventObj.trigger('re-instrument', decodedStr);
          eventObj.trigger('update-url', {'record' : rawInput});
          eventObj.trigger('valid-input');
          eventObj.trigger('valid-output');
        }catch(err) {
          eventObj.trigger('invalid-output', err);
        }
      }
    }

    function showError(errorElem, msg) {
      errorElem.text(msg);
      errorElem.show();
    };

    function hideError(errorElem, validElem, elementName) {
      errorElem.text("");
      errorElem.hide();
      if (validElem) {
        if ("schema" === elementName) {
          randomElement.hide();
        }
        validElem.fadeIn('slow').delay(500).fadeOut('slow', function () {
          if ("schema" === elementName) {
            randomElement.fadeIn('slow');
          }
        });
      }
    }
    
    /* If the schema is pasted with proper json formats, simply json.parse wouldn't work.*/
    function readSchemaFromInput() {
      var trimmedInput = $.trim(schemaElement.text()).replace(/\s/g, "");
      return JSON.parse(trimmedInput);
    }

    /*Used for decoding.
    *Read the text represented as space-seperated hex numbers in elementId
    *and construct a Buffer object*/
    function readBuffer(rawInput) { 
      var str = rawInput.replace(whiteSpacePattern,'');
      return new Buffer(str, 'hex');
    }

    /**
     * Will update the old value of the element to the new one, 
     * and returns true if the content changed. 
    */
    function updateContent(element) {
      var newText = $.trim($(element).text()).replace(whiteSpacePattern, '');
      if (!element.data) {
        element.data = {};
      }
      if (newText !== '' && 
        (!element.data['oldValue'] || 
          element.data['oldValue'] != newText)) {
        element.data['oldValue'] =  newText;
        return true;
      }
      return false;
    }

    function instrument(schema) {
      if (schema instanceof avsc.Type) {
        schema = schema.getSchema();
      }
      var refs = [];
      return avsc.parse(schema, {typeHook: hook,
                                 wrapUnions: true});

      function hook(schema, opts) {
        if (~refs.indexOf(schema)) {
          return;
        }
        refs.push(schema);

        if (schema.type === 'record') {
          schema.fields.forEach(function (f) {
            f['default'] = undefined;
          });
        }

        var name = schema.name;
        if (name) {
          schema.name = 'r' + Math.random().toString(36).substr(2, 6);
        }
        var wrappedSchema = {
          name: name || (schema.type ? (schema.type + '_') : 'r' + Math.random().toString(36).substr(2, 6)),
          namespace: schema.namespace,
          type: 'record',
          fields: [{name: 'value', type: schema}]
        };
        refs.push(wrappedSchema);
        opts.wrapUnions = true;

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

    /**
     * Creates an array of size buffer.length, 
     * where each index will contain a string representing
     * the path in the input record corresponding to this byte.
     */
    function computeReverseIndex(obj) {
      if (!obj) {
        return;
      }
      // initialize an array with all empty elements;
      var size = obj.end;
      var res = Array.apply(null, Array(size))
                     .map(String.prototype.valueOf,"");
      assignLabels('', obj, res);
      return res;
    }

    function assignLabels(key, node, res) {
      if (node.hasOwnProperty('start') && node.hasOwnProperty('end')) {
        appendLabel(node.start, node.end, key, res);
      }
      var valueNode = node.value;
      if (valueNode) {
        for (var child in valueNode) {
          if (valueNode.hasOwnProperty(child)) {
            assignLabels(child, valueNode[child], res);
          }
        }
      }
    }

    function appendLabel(start, end, label, arr) {
      var length = label.length;
      if (label[length - 1] == '_')
        label = label.substr(0, length - 1);  
      for (var i = start; i < end; i++) {
        arr[i] += ' ' + label; 
      }
    }

    function loadTemplate(name) { 
      if (name != '') {
        var p = 'schemas/' + name + '.avsc';
        $.ajax({
            url: p,
            success: function(data){
            eventObj.trigger('schema-loaded', data);
            eventObj.trigger('generate-random');
          }
        });
      }
    }

    populateFromQuery();
 });
})();
