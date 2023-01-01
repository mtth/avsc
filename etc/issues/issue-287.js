'use strict';

const avro = require('../../lib');

let schema = {
    "type": "record",
    "name": "baseRecord",
    "fields": [
        {
            "name": "rate_net_amount",
            "default": null,
            "type": [
                "null",
                {
                    "logicalType": "decimal",
                    "scale": 2,
                    "type": "bytes",
                    "precision": 10
                }
            ]
        }
    ]};


/**
 * Sample decimal logical type implementation.
 *
 * It wraps its values in a very simple custom `Decimal` class.
 *
 */
class DecimalType extends avro.types.LogicalType {
  constructor (attrs, opts) {
    super(attrs, opts);

    var precision = attrs.precision;
    if (precision !== (precision | 0) || precision <= 0) {
      throw new Error('invalid precision');
    }
    var scale = attrs.scale;
    if (scale !== (scale | 0) || scale < 0 || scale > precision) {
      throw new Error('invalid scale');
    }
    var type = this.underlyingType;
    if (avro.Type.isType(type, 'fixed')) {
      var size = type.size;
      var maxPrecision = Math.log(Math.pow(2, 8 * size - 1) - 1) / Math.log(10);
      if (precision > (maxPrecision | 0)) {
        throw new Error('fixed size too small to hold required precision');
      }
    }
    this.Decimal = Decimal;

    class Decimal {
      constructor (unscaled) {
        this.unscaled = unscaled;
      }

      toNumber () {
        return this.unscaled * Math.pow(10, -scale);
      }
    }

    Decimal.prototype.precision = precision;
    Decimal.prototype.scale = scale;
  }

  _fromValue (buf) {
    return new this.Decimal(buf.readIntBE(0, buf.length));
  }

  _toValue (dec) {
    if (!(dec instanceof this.Decimal)) {
      throw new Error('invalid decimal');
    }

    var type = this.underlyingType;
    var buf;
    if (avro.Type.isType(type, 'fixed')) {
      buf = new Buffer(type.size);
    } else {
      var size = Math.log(dec > 0 ? dec : - 2 * dec) / (Math.log(2) * 8) | 0;
      buf = new Buffer(size + 1);
    }
    buf.writeIntBE(dec.unscaled, 0, buf.length);
    return buf;
  }

  _resolve (type) {
    if (
      avro.Type.isType(type, 'logical:decimal') &&
      type.Decimal.prototype.precision === this.Decimal.prototype.precision &&
      type.Decimal.prototype.scale === this.Decimal.prototype.scale
    ) {
      return function (dec) { return dec; };
    }
  }

  _export (attrs) {
    attrs.precision = this.Decimal.prototype.precision;
    attrs.scale = this.Decimal.prototype.scale;
  }
}

// Parse the schema, providing our decimal implementation.
const type = avro.Type.forSchema(schema, {logicalTypes: {decimal: DecimalType}});

const buf = Buffer.from([2, 4, 2, 72]); // A valid encoded record for your schema.
const val = type.fromBuffer(buf); // The parsed value.
const dec = val.rate_net_amount; // A `Decimal` instance.
console.log(dec.toNumber()); // 5.84!
