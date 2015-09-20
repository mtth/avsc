/* jshint node: true */

'use strict';


function Message(index, data) {
  this.index = index;
  this.data = data;
}

/**
 * Sort messages consecutively.
 *
 */
function ConsecutiveQueue() {
  this._index = 0;
  this._messages = [];
}

ConsecutiveQueue.prototype.size = function () {

  return this._messages.length;

};

// TODO: Make this be async instead?
ConsecutiveQueue.prototype.next = function () {
  var ms = this._messages;
  var len = (ms.length - 1) | 0;
  var msg = ms[0];
  if (!msg || msg.index > this._index) {
    return null;
  }
  this._index++;
  if (!len) {
    ms.pop();
    return msg.data;
  }
  ms[0] = ms.pop();
  var mid = len >> 1;
  var i = 0;
  var i1, i2, j, m, c, c1, c2;
  while (i < mid) {
    m = ms[i];
    i1 = (i >> 1) + 1;
    i2 = (i + 1) >> 1;
    c1 = ms[i1];
    c2 = ms[i2];
    if (!c2 || c1.index <= c2.index) {
      c = c1;
      j = i1;
    } else {
      c = c2;
      j = i2;
    }
    if (c.index >= m.index) {
      break;
    }
    ms[j] = m;
    ms[i] = c;
    i = j;
  }
  return msg.data;
};

ConsecutiveQueue.prototype.add = function (index, data) {
  if (index < this._index) {
    throw new Error('invalid index');
  }
  var ms = this._messages;
  var i = ms.length | 0;
  var j, m;
  ms.push(new Message(index, data));
  while (i > 0 && ms[i].index < ms[i >> 1].index) {
    j = i >> 1;
    m = ms[i];
    ms[i] = ms[j];
    ms[j] = m;
    i = j;
  }
};


module.exports = {
  ConsecutiveQueue: ConsecutiveQueue
};
