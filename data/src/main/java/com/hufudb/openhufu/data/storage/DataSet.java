package com.hufudb.openhufu.data.storage;
import com.hufudb.openhufu.data.schema.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public interface DataSet {
  static final Logger LOG = LoggerFactory.getLogger(DataSet.class);

  Schema getSchema();

  DataSetIterator getIterator();

  void close();
}
