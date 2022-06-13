package com.hufudb.onedb.owner.adapter.csv;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Iterator;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import com.hufudb.onedb.data.schema.Schema;
import com.hufudb.onedb.data.storage.DataSet;
import com.hufudb.onedb.data.storage.DataSetIterator;
import com.hufudb.onedb.proto.OneDBData.ColumnType;

/**
 * A simple dataset interface for csv file
 * require source file contains header record in the first line
 * ColumnType of all columns is string
 */
public class CsvDataSet implements DataSet {
  final Path dataPath;
  final CSVFormat csvFormat;
  final CSVParser csvParser;
  final Schema schema;

  CsvDataSet(Path path) throws IOException {
    csvFormat = CSVFormat.RFC4180.builder().setHeader().setSkipHeaderRecord(true).setIgnoreSurroundingSpaces(true).setNullString("").build();
    Schema.Builder builder = Schema.newBuilder();
    dataPath = path;
    csvParser = CSVParser.parse(dataPath, StandardCharsets.UTF_8, csvFormat);
    csvParser.getHeaderNames().forEach(col -> builder.add(col, ColumnType.STRING));
    schema = builder.build();
  }

  @Override
  public String toString() {
    return schema.toString();
  }

  @Override
  public Schema getSchema() {
    return schema;
  }

  @Override
  public DataSetIterator getIterator() {
    return new CsvIterator(csvParser.iterator());
  }

  @Override
  public void close() {
    try {
      csvParser.close();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  class CsvIterator implements DataSetIterator {
    Iterator<CSVRecord> iter;
    CSVRecord current;

    CsvIterator(Iterator<CSVRecord> iter) {
      this.iter = iter;
      this.current = null;
    }

    @Override
    public Object get(int columnIndex) {
      return current.get(columnIndex);
    }

    @Override
    public int size() {
      return schema.size();
    }

    @Override
    public boolean next() {
      if (iter.hasNext()) {
        current = iter.next();
        return true;
      }
      return false;
    }
  }
}
