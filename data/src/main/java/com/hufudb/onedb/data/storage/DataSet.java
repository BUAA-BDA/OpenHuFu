package com.hufudb.onedb.data.storage;
import com.hufudb.onedb.data.schema.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public interface DataSet {
  static final Logger LOG = LoggerFactory.getLogger(DataSet.class);

  Schema getSchema();

  DataSetIterator getIterator();

  void close();
}
