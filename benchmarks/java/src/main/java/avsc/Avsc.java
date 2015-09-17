package avsc;

import java.io.File;
import java.io.IOException;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Parser;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;


public class Avsc {

  public static void main(String[] args) throws IOException {
    Schema schema = new Parser().parse(new File("../dat/user.avsc"));

    File file = new File("../dat/user-100000.avro");
    DatumReader<GenericRecord> datumReader = new GenericDatumReader<GenericRecord>(schema);
    DataFileReader<GenericRecord> dataFileReader = new DataFileReader<GenericRecord>(file, datumReader);

    long start = System.currentTimeMillis();
    int n = 0;
    GenericRecord record = null;
    while (dataFileReader.hasNext()) {
      record = dataFileReader.next();
      if (record.get("name") == null) {
        break;
      }
      n++;
    }
    System.out.println(n + " records [" + (System.currentTimeMillis() - start) / 1000. + "]");

  }

}
