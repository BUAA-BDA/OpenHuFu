package com.hufudb.onedb.core.data.query;

import com.hufudb.onedb.core.data.BasicDataSet;
import com.hufudb.onedb.core.data.FieldType;
import com.hufudb.onedb.core.data.Header;
import com.hufudb.onedb.core.data.query.join.PlaintextNestedLoopJoin;
import com.hufudb.onedb.core.sql.expression.OneDBExpression;
import com.hufudb.onedb.core.sql.implementor.OneDBImplementor;
import com.hufudb.onedb.core.sql.implementor.utils.OneDBJoinInfo;
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

  public static QueryableDataSet join(OneDBImplementor implementor, QueryableDataSet left,
      QueryableDataSet right, OneDBJoinInfo joinInfo) {
    return implementor.join(left, right, joinInfo);
  }

  public QueryableDataSet filter(OneDBImplementor implementor, List<OneDBExpression> filters) {
    return implementor.filter(this, filters);
  }

  public QueryableDataSet project(OneDBImplementor implementor, List<OneDBExpression> projects) {
    return implementor.project(this, projects);
  }

  public QueryableDataSet aggregate(OneDBImplementor implementor, List<Integer> groups,
      List<OneDBExpression> aggs, List<FieldType> inputTypes) {
    return implementor.aggregate(this, groups, aggs, inputTypes);
  }

  public QueryableDataSet sort(OneDBImplementor implementor, List<String> orders) {
    return implementor.sort(this, orders);
  }

  public QueryableDataSet limit(int offset, int fetch) {
    if (fetch == 0) {
      if (offset >= this.getRowCount()) {
        this.rows.clear();
      } else {
        this.rows = this.rows.subList(offset, this.getRowCount());
      }
    } else {
      if (offset >= this.getRowCount()) {
        this.rows.clear();
      } else {
        this.rows = this.rows.subList(offset, Math.min(this.getRowCount(), offset + fetch));
      }
    }
    return this;
  }
}
