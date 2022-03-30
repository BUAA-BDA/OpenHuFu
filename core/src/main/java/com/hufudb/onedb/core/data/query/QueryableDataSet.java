package com.hufudb.onedb.core.data.query;

import java.util.List;

import com.hufudb.onedb.core.data.BasicDataSet;
import com.hufudb.onedb.core.data.Header;
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

  public static QueryableDataSet join(QueryableDataSet left, QueryableDataSet right, OneDBJoinInfo joinInfo) {
    // todo: implement
    return EMPTY;
  }

  public QueryableDataSet filter(List<ExpressionProto> fil) {
    return EMPTY;
  }

  public QueryableDataSet filter(ExpressionProto fil) {
    return EMPTY;
  }

  public QueryableDataSet select(List<ExpressionProto> sel) {
    return EMPTY;
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