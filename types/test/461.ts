import * as Avro from '../index'

const e = Avro.Type.forSchema({
    name: "enum",
    type: "enum",
    symbols: ["foo", "bar", "baz"],
});

const record = Avro.Type.forSchema({
    name: "record",
    type: "record",
    fields: [
        { name: "enum", type: ["null", e], default: null },
    ],
});
