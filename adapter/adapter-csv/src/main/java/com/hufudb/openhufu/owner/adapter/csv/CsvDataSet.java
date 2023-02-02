package com.hufudb.openhufu.owner.adapter.csv;

import java.io.IOException;
import java.util.Iterator;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import com.hufudb.openhufu.data.schema.Schema;
import com.hufudb.openhufu.data.storage.DataSet;
import com.hufudb.openhufu.data.storage.DataSetIterator;

/**
 * A simple dataset interface for csv file
 * require source file contains header record in the first line
 * ColumnType of all columns is string
 */
public class CsvDataSet implements DataSet {
  final CSVParser csvParser;
  final Schema schema;

  CsvDataSet(CSVParser csvParser, Schema schema) {
    this.csvParser = csvParser;
    this.schema = schema;
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
      LOG.error("Close csvParser error", e);
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
