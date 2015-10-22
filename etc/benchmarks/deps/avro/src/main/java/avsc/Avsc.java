package avsc;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import org.apache.avro.AvroRuntimeException;
import org.apache.avro.Schema.Parser;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericArray;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.io.Encoder;


public class Avsc {

  private static final DecoderFactory _decoderFactory = DecoderFactory.get();
  private static final EncoderFactory _encoderFactory = EncoderFactory.get();

  private final String _path;
  private final Schema _schema;
  private final GenericDatumReader<GenericRecord> _reader;
  private final GenericDatumWriter<GenericRecord> _writer;

  /**
   * Simple Avoo benchmarking class.
   *
   * @param path Path to an Avro object container file.
   *
   */
  Avsc(String path) throws IOException {

    // Extract schema by opening the file once.
    File file = new File(path);
    DatumReader<GenericRecord> datumReader = new GenericDatumReader<GenericRecord>();
    DataFileReader<GenericRecord> dataFileReader = new DataFileReader<>(file, datumReader);

    _path = path;
    _schema = dataFileReader.getSchema();
    _reader = new GenericDatumReader<>(_schema);
    _writer = new GenericDatumWriter<>(_schema);

    dataFileReader.close();

  }

  /**
   * Decode an array of byte arrays, each representing a valid record.
   *
   */
  double decodeBenchmark() throws IOException {

    List<GenericRecord> records = new ArrayList<>();
    for (GenericRecord record : getRecords()) {
      records.add(record);
    }

    byte[][] encodings = new byte[records.size()][];
    for (int i = 0; i < records.size(); i++) {
      encodings[i] = encode(records.get(i));
    }

    long n = 0;
    long start = System.currentTimeMillis();
    for (int i = 0; i < encodings.length; i++) {
      GenericRecord record = decode(encodings[i]);
      if (record.get("$") == null) {
        n++;
      }
    }
    if (n <= 0) {
      throw new RuntimeException("this shouldn't happen");
    }
    return timePerRecord(start, n);

  }

  /**
   * Decode an array of byte arrays, each representing a valid record.
   *
   */
  double encodeBenchmark() throws IOException {

    List<GenericRecord> records = new ArrayList<>();
    for (GenericRecord record : getRecords()) {
      records.add(record);
    }

    long start = System.currentTimeMillis();
    long n = 0;
    long m = 0;
    for (int i = 0; i < records.size(); i++) {
      byte[] data = encode(records.get(i));
      m += 255 + data[0] + data.length;
      n++;
    }
    if (m <= 0) {
      throw new RuntimeException("this shouldn't happen");
    }
    return timePerRecord(start, n);

  }

  /**
   * Decode an object container file.
   *
   */
  double objectFileBenchmark() throws IOException {

    long n = 0;
    long start = System.currentTimeMillis();
    for (GenericRecord record : getRecords()) {
      if (record.get("$") == null) {
        n++;
      }
    }
    if (n <= 0) {
      throw new RuntimeException("this shouldn't happen");
    }
    return timePerRecord(start, n);

  }

  /**
   * Driver.
   *
   * See usage method below for details.
   *
   */
  public static void main(String[] args) throws IOException {

    if (args.length < 2) {
      usage();
    }

    String command = args[0];
    String path = args[1];
    int loops = 1;
    if (args.length == 3) {
      loops = Integer.valueOf(args[2]);
    }

    Avsc avsc = new Avsc(path);

    double time = 0;
    for (int i = 0; i < loops; i++) {
      switch (command) {
        case "decode":
          time += avsc.decodeBenchmark();
          break;
        case "encode":
          time += avsc.encodeBenchmark();
          break;
        case "object-file":
          time += avsc.objectFileBenchmark();
          break;
        default:
          usage();
      }
    }
    System.out.println(time / loops);

  }

  // Helpers.

  static void usage() {

    System.err.println("usage: java -jar ... COMMAND PATH [N]");
    System.exit(1);

  }

  Iterable<GenericRecord> getRecords() throws IOException {

    File file = new File(_path);
    return new DataFileReader<>(file, _reader);

  }

  GenericRecord decode(byte[] data) throws IOException {

    Decoder decoder = _decoderFactory.createBinaryDecoder(data, null);
    return _reader.read(null, decoder);

  }

  byte[] encode(GenericRecord record) throws IOException {

    ByteArrayOutputStream out = new ByteArrayOutputStream();
    try {
      Encoder encoder = _encoderFactory.binaryEncoder(out, null);
      _writer.write(record, encoder);
      encoder.flush();
    } finally {
      out.close();
    }
    return out.toByteArray();

  }

  double timePerRecord(long start, long n) {

    return ((double) System.currentTimeMillis() - start) / n;

  }

}
