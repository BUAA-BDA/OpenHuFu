package com.hufudb.onedb.core.data.query.aggregate;

import java.util.List;
import com.hufudb.onedb.core.data.FieldType;
import com.hufudb.onedb.core.data.Row;

public interface Aggregator extends AggregateFunction<Row, Row> {
  boolean hasNext();
  void reset();
  List<FieldType> getOutputTypes();

  public static Aggregator create(List<Integer> groups, List<AggregateFunction<Row, Comparable>> aggFunc, List<FieldType> types) {
    if (groups.isEmpty()) {
      return new SingleAggregator(aggFunc, types);
    } else {
      return new GroupAggregator(groups, aggFunc, types);
    }
  }
}
