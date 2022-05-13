package com.hufudb.onedb.data.storage;

import java.util.Iterator;
import com.hufudb.onedb.data.schema.Schema;
import com.hufudb.onedb.proto.OneDBData.DataSetProto;

public class IteratorDataSet implements DataSet {
  final Schema schema;
  final Iterator<DataSetProto> source;

  public IteratorDataSet(Schema schema, Iterator<DataSetProto> source) {
    this.schema = schema;
    this.source = source;
  }

  @Override
  public Schema getSchema() {
    return schema;
  }

  @Override
  public DataSetIterator getIterator() {
    return null;
  }

  @Override
  public void close() {
    
  }
}
