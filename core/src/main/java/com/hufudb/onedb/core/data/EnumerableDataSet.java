package com.hufudb.onedb.core.data;

import org.apache.calcite.linq4j.Enumerator;

public interface EnumerableDataSet extends DataSet, Enumerator<Row> {}
