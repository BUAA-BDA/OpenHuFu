package com.hufudb.onedb.core.data;

import com.hufudb.onedb.data.storage.DataSet;
import com.hufudb.onedb.data.storage.DataSetIterator;
import com.hufudb.onedb.data.storage.ProtoDataSet;
import com.hufudb.onedb.data.storage.Row;
import org.apache.calcite.linq4j.Enumerator;

public class EnumerableDataSet implements Enumerator<Row> {
  DataSetIterator iterator;
  DataSet dataSet;

  public EnumerableDataSet(DataSet source) {
    dataSet = ProtoDataSet.materialize(source);
    iterator = dataSet.getIterator();
  }

  @Override
  public Row current() {
    return iterator;
  }

  @Override
  public boolean moveNext() {
    return iterator.next();
  }

  @Override
  public void reset() {
    iterator = dataSet.getIterator();
  }

  @Override
  public void close() {
    dataSet.close();
  }
}
