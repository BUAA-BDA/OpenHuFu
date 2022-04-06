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
  final int aggLength;
  final Map<Row, SingleAggregator> aggregatorMap;
  final RowBuilder keyBuilder;
  final List<FieldType> types;
  Iterator<Map.Entry<Row, SingleAggregator>> iterator;

  GroupAggregator(List<Integer> groups, List<FieldType> groupTypes, SingleAggregator baseAggregator) {
    this.groups = groups;
    this.types = groupTypes;
    this.baseAggregator = baseAggregator;
    this.aggLength = baseAggregator.size();
    this.aggregatorMap = new HashMap<>();
    this.keyBuilder = Row.newBuilder(groups.size());
    this.iterator = null;
  }

  public GroupAggregator(List<Integer> groups, List<FieldType> groupTypes, List<AggregateFunction<Row, Comparable>> aggFunc, List<FieldType> aggOutputTypes) {
    this.groups = groups;
    this.types = groupTypes;
    this.baseAggregator = new SingleAggregator(aggFunc, aggOutputTypes);
    this.aggLength = aggFunc.size();
    this.aggregatorMap = new HashMap<>();
    this.keyBuilder = Row.newBuilder(groups.size());
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
    Row value = entry.getValue().aggregate();
    RowBuilder builder = Row.newBuilder(aggLength);
    // add agg result
    for (int i = 0; i < aggLength; ++i) {
      builder.set(i, value.getObject(i));
    }
    return builder.build();
  }

  @Override
  public AggregateFunction<Row, Row> patternCopy() {
    return new GroupAggregator(groups, types, baseAggregator.patternCopy());
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

  @Override
  public List<FieldType> getOutputTypes() {
    return types;
  }

  @Override
  public int size() {
    return baseAggregator.size();
  }
}
