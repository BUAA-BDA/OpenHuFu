package com.hufudb.onedb.core.data;

import org.apache.calcite.linq4j.Enumerator;

public abstract class EnumerableDataSet extends DataSet implements Enumerator<Row> {
  EnumerableDataSet(Header header) {
    super(header);
  }
}