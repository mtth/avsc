#!/usr/bin/env ruby

require 'avro'

records = []
file = File.open(ARGV[0], 'r+')
dr = Avro::IO::DatumReader.new
fr = Avro::DataFile::Reader.new(file, dr)
schema = dr.writers_schema
fr.each { |record| records << record }

dw = Avro::IO::DatumWriter.new(schema)
t = Time.now
n = 0
records.each { |record|
  buffer = StringIO.new
  encoder = Avro::IO::BinaryEncoder.new(buffer)
  dw.write(record, encoder)
  n += 1
}
puts 1000 * (Time.now - t) / n
