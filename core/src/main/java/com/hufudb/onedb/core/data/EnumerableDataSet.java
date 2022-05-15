package com.hufudb.onedb.core.data;

import com.hufudb.onedb.data.storage.DataSet;
import com.hufudb.onedb.data.storage.Row;
import org.apache.calcite.linq4j.Enumerator;

public interface EnumerableDataSet extends DataSet, Enumerator<Row> {}
