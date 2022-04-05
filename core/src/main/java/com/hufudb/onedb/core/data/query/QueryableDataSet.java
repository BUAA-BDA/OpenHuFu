package com.hufudb.onedb.core.data.query;

import com.google.common.collect.ImmutableList;
import com.hufudb.onedb.core.data.BasicDataSet;
import com.hufudb.onedb.core.data.FieldType;
import com.hufudb.onedb.core.data.Header;
import com.hufudb.onedb.core.data.query.aggregate.PlaintextAggregation;
import com.hufudb.onedb.core.data.query.calculate.PlaintextCalculator;
import com.hufudb.onedb.core.data.query.filter.PlaintextFilter;
import com.hufudb.onedb.core.data.query.join.PlaintextNestedLoopJoin;
import com.hufudb.onedb.core.sql.expression.OneDBExpression;
import com.hufudb.onedb.core.sql.implementor.utils.OneDBJoinInfo;
import com.hufudb.onedb.core.sql.rel.OneDBQueryContext;
import com.hufudb.onedb.rpc.OneDBCommon.ExpressionProto;
import com.hufudb.onedb.rpc.OneDBCommon.OneDBQueryProto;
import java.util.List;

public class QueryableDataSet extends BasicDataSet {
  public static final QueryableDataSet EMPTY = new QueryableDataSet(Header.EMPTY);

  protected QueryableDataSet(Header header) {
    super(header);
  }

  QueryableDataSet(BasicDataSet dataSet) {
    super(dataSet.getHeader(), dataSet.getRows());
  }

  public static QueryableDataSet fromBasic(BasicDataSet dataSet) {
    return new QueryableDataSet(dataSet);
  }

  public static QueryableDataSet fromHeader(Header header) {
    return new QueryableDataSet(header);
  }

  public static QueryableDataSet fromExpression(List<OneDBExpression> exps) {
    Header header = OneDBExpression.generateHeader(exps);
    return new QueryableDataSet(header);
  }

  public static QueryableDataSet join(QueryableDataSet left, QueryableDataSet right,
      OneDBJoinInfo joinInfo) {
    return PlaintextNestedLoopJoin.apply(left, right, joinInfo);
  }

  public List<FieldType> getTypeList() {
    return header.getTypeList();
  }

  public QueryableDataSet filter(OneDBQueryProto proto) {
    return PlaintextFilter.apply(this, OneDBExpression.fromProto(proto.getWhereExpList()));
  }

  public QueryableDataSet select(OneDBQueryProto proto) {
    List<OneDBExpression> calcs = OneDBExpression.fromProto(proto.getSelectExpList());
    header = OneDBExpression.generateHeader(calcs);
    return PlaintextCalculator.apply(this, calcs);
  }

  public QueryableDataSet aggregate(OneDBQueryProto proto) {
    List<OneDBExpression> aggs = OneDBExpression.fromProto(proto.getAggExpList());
    List<OneDBExpression> selects = OneDBExpression.fromProto(proto.getSelectExpList());
    header = OneDBQueryContext.getOutputHeader(proto);
    return PlaintextAggregation.apply(this, proto.getGroupList(), selects, aggs);
  }

  public QueryableDataSet sort() {
    return EMPTY;
  }

  public QueryableDataSet limit(int offset, int fetch) {
    this.rows.subList(offset, fetch);
    return EMPTY;
  }
}
