#!/usr/bin/env ruby

require 'avro'

buffers = []
file = File.open(ARGV[0], 'r+')
dr = Avro::IO::DatumReader.new
fr = Avro::DataFile::Reader.new(file, dr)
schema = dr.writers_schema
dw = Avro::IO::DatumWriter.new(schema)
fr.each { |record|
  buffer = StringIO.new
  encoder = Avro::IO::BinaryEncoder.new(buffer)
  dw.write(record, encoder)
  buffers << buffer
}

dr = Avro::IO::DatumReader.new(schema)
t = Time.now
n = 0
buffers.each { |buffer|
  n += 1
  buffer.seek(0)
  decoder = Avro::IO::BinaryDecoder.new(buffer)
  dr.read(decoder)
}
puts 1000 * (Time.now - t) / n
