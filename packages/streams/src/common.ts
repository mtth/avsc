/**
 * Ordered queue which returns items consecutively.
 *
 * This is actually a heap by index, with the added requirements that elements
 * can only be retrieved consecutively.
 */
class OrderedQueue {
  constructor() {
    this._index = 0;
    this._items = [];
  }

  push(item) {
    const items = this._items;
    let i = items.length | 0;
    let j;
    items.push(item);
    while (i > 0 && items[i].index < items[(j = (i - 1) >> 1)].index) {
      item = items[i];
      items[i] = items[j];
      items[j] = item;
      i = j;
    }
  }

  pop() {
    const items = this._items;
    const len = (items.length - 1) | 0;
    const first = items[0];
    if (!first || first.index > this._index) {
      return null;
    }
    this._index++;
    if (!len) {
      items.pop();
      return first;
    }
    items[0] = items.pop();
    const mid = len >> 1;
    let i = 0;
    let i1, i2, j, item, c, c1, c2;
    while (i < mid) {
      item = items[i];
      i1 = (i << 1) + 1;
      i2 = (i + 1) << 1;
      c1 = items[i1];
      c2 = items[i2];
      if (!c2 || c1.index <= c2.index) {
        c = c1;
        j = i1;
      } else {
        c = c2;
        j = i2;
      }
      if (c.index >= item.index) {
        break;
      }
      items[j] = item;
      items[i] = c;
      i = j;
    }
    return first;
  }
}
