package com.hufudb.onedb.expression;

import com.hufudb.onedb.data.function.AggregateFunction;
import com.hufudb.onedb.data.function.Aggregator;
import com.hufudb.onedb.data.schema.Schema;
import com.hufudb.onedb.data.storage.ArrayRow;
import com.hufudb.onedb.data.storage.Row;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * aggregator which aggregates multiple rows into a row for each group
 */
public class GroupAggregator implements Aggregator {
  final Schema schema;
  final List<Integer> groups;
  final SingleAggregator baseAggregator;
  final Map<Row, SingleAggregator> aggregatorMap;
  final ArrayRow.Builder keyBuilder;
  Iterator<Map.Entry<Row, SingleAggregator>> iterator;

  GroupAggregator(Schema schema, List<Integer> groups, SingleAggregator baseAggregator) {
    this.schema = schema;
    this.groups = groups;
    this.baseAggregator = baseAggregator;
    this.aggregatorMap = new HashMap<>();
    this.keyBuilder = ArrayRow.newBuilder(groups.size());
    this.iterator = null;
  }

  public GroupAggregator(Schema schema, List<Integer> groups, List<AggregateFunction<Row, Comparable>> aggFunc) {
    this(schema, groups, new SingleAggregator(schema, aggFunc));
  }
  
  void init() {
    iterator = aggregatorMap.entrySet().iterator();
  }

  @Override
  public void add(Row row) {
    keyBuilder.reset();
    for (int i = 0; i < groups.size(); ++i) {
      keyBuilder.set(i, row.get(groups.get(i)));
    }
    Row key = keyBuilder.build();
    if (aggregatorMap.containsKey(key)) {
      aggregatorMap.get(key).add(row);
    } else {
      SingleAggregator agg = baseAggregator.copy();
      agg.add(row);
      aggregatorMap.put(key, agg);
    }
  }

  @Override
  public Row aggregate() {
    if (iterator == null) {
      init();
    }
    Map.Entry<Row, SingleAggregator> entry = iterator.next();
    Row value = entry.getValue().aggregate();
    ArrayRow.Builder builder = ArrayRow.newBuilder(schema.size());
    for (int i = 0; i < schema.size(); ++i) {
      builder.set(i, value.get(i));
    }
    return builder.build();
  }

  @Override
  public GroupAggregator copy() {
    return new GroupAggregator(schema, groups, baseAggregator.copy());
  }

  @Override
  public boolean hasNext() {
    if (iterator == null) {
      init();
    }
    return iterator.hasNext();
  }
}
