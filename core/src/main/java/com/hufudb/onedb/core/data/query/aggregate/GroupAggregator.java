package com.hufudb.onedb.core.data.query.aggregate;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import com.hufudb.onedb.core.data.FieldType;
import com.hufudb.onedb.core.data.Row;
import com.hufudb.onedb.core.data.Row.RowBuilder;

public class GroupAggregator implements Aggregator {
  final List<Integer> groups;
  final SingleAggregator baseAggregator;
  final int length;
  final Map<Row, SingleAggregator> aggregatorMap;
  final RowBuilder keyBuilder;
  final List<FieldType> types;
  Iterator<Map.Entry<Row, SingleAggregator>> iterator;

  public GroupAggregator(List<Integer> groups, List<AggregateFunction<Row, Comparable>> aggFunc, List<FieldType> types) {
    this.groups = groups;
    this.baseAggregator = new SingleAggregator(aggFunc, types.subList(groups.size(), types.size()));
    this.length = groups.size() + aggFunc.size();
    this.aggregatorMap = new HashMap<>();
    this.keyBuilder = Row.newBuilder(groups.size());
    this.types = types;
    this.iterator = null;
  }

  @Override
  public void add(Row row) {
    for (int i = 0; i < groups.size(); ++i) {
      keyBuilder.set(i, row.getObject(groups.get(i)));
    }
    Row key = keyBuilder.build();
    if (aggregatorMap.containsKey(key)) {
      aggregatorMap.get(key).add(row);
    } else {
      SingleAggregator agg = baseAggregator.patternCopy();
      agg.add(row);
      aggregatorMap.put(key, agg);
    }
    keyBuilder.reset();
  }

  @Override
  public Row aggregate() {
    if (iterator == null) {
      reset();
    }
    Map.Entry<Row, SingleAggregator> entry = iterator.next();
    Row key = entry.getKey();
    Row value = entry.getValue().aggregate();
    RowBuilder builder = Row.newBuilder(length);
    // add key
    for (int i = 0; i < groups.size(); ++i) {
      builder.set(i, key.getObject(i));
    }
    // add agg result
    for (int i = groups.size(); i < length; ++i) {
      builder.set(i, value.getObject(i - groups.size()));
    }
    return builder.build();
  }

  @Override
  public AggregateFunction<Row, Row> patternCopy() {
    return new GroupAggregator(groups, null, types);
  }

  @Override
  public boolean hasNext() {
    if (iterator == null) {
      reset();
    }
    return iterator.hasNext();
  }

  @Override
  public void reset() {
    iterator = aggregatorMap.entrySet().iterator();
  }
}
