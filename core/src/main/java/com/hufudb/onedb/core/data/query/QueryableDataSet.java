package com.hufudb.onedb.core.data.query;

import java.util.List;

import com.google.common.collect.ImmutableList;
import com.hufudb.onedb.core.data.BasicDataSet;
import com.hufudb.onedb.core.data.FieldType;
import com.hufudb.onedb.core.data.Header;
import com.hufudb.onedb.core.data.query.calculator.PlaintextCalculator;
import com.hufudb.onedb.core.data.query.filter.PlaintextFilter;
import com.hufudb.onedb.core.data.query.join.PlaintextNestedLoopJoin;
import com.hufudb.onedb.core.sql.expression.OneDBExpression;
import com.hufudb.onedb.core.sql.implementor.utils.OneDBJoinInfo;
import com.hufudb.onedb.rpc.OneDBCommon.ExpressionProto;

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

  public List<FieldType> getTypeList() {
    return header.getTypeList();
  }

  // todo: now we just implement the equi-join, add theta join implement later
  public static QueryableDataSet join(QueryableDataSet left, QueryableDataSet right, OneDBJoinInfo joinInfo) {
    return PlaintextNestedLoopJoin.apply(left, right, joinInfo);
  }

  public QueryableDataSet filter(List<ExpressionProto> fil) {
    return PlaintextFilter.apply(this, OneDBExpression.fromProto(fil));
  }

  public QueryableDataSet filter(ExpressionProto fil) {
    return PlaintextFilter.apply(this, ImmutableList.of(OneDBExpression.fromProto(fil)));
  }

  public QueryableDataSet select(List<ExpressionProto> sel) {
    List<OneDBExpression> calcs = OneDBExpression.fromProto(sel);
    header = OneDBExpression.generateHeader(calcs);
    return PlaintextCalculator.apply(this, calcs);
  }

  public QueryableDataSet aggregate(List<ExpressionProto> agg) {
    return EMPTY;
  }

  public QueryableDataSet sort() {
    return EMPTY;
  }

  public QueryableDataSet limit(int offset, int fetch) {
    this.rows.subList(offset, fetch);
    return EMPTY;
  }
}