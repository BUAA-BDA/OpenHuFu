package com.hufudb.onedb.core.query.implementor.plaintext;


import com.hufudb.onedb.core.data.BasicDataSet;
import com.hufudb.onedb.core.data.FieldType;
import com.hufudb.onedb.core.data.Header;
import com.hufudb.onedb.core.query.QueryableDataSet;
import com.hufudb.onedb.core.query.implementor.OneDBImplementor;
import com.hufudb.onedb.core.query.implementor.utils.OneDBJoinInfo;
import com.hufudb.onedb.core.sql.expression.OneDBExpression;
import java.util.List;

public class PlaintextQueryableDataSet extends BasicDataSet implements QueryableDataSet {
  public static final PlaintextQueryableDataSet EMPTY = new PlaintextQueryableDataSet(Header.EMPTY);

  protected PlaintextQueryableDataSet(Header header) {
    super(header);
  }

  PlaintextQueryableDataSet(BasicDataSet dataSet) {
    super(dataSet.getHeader(), dataSet.getRows());
  }

  public static QueryableDataSet fromBasic(BasicDataSet dataSet) {
    return new PlaintextQueryableDataSet(dataSet);
  }

  public static QueryableDataSet fromHeader(Header header) {
    return new PlaintextQueryableDataSet(header);
  }

  public static QueryableDataSet fromExpression(List<OneDBExpression> exps) {
    Header header = OneDBExpression.generateHeader(exps);
    return new PlaintextQueryableDataSet(header);
  }

  @Override
  public List<FieldType> getTypeList() {
    return header.getTypeList();
  }

  @Override
  public QueryableDataSet join(OneDBImplementor implementor, QueryableDataSet right,
      OneDBJoinInfo joinInfo) {
    return implementor.join(this, right, joinInfo);
  }

  @Override
  public QueryableDataSet filter(OneDBImplementor implementor, List<OneDBExpression> filters) {
    return implementor.filter(this, filters);
  }

  @Override
  public QueryableDataSet project(OneDBImplementor implementor, List<OneDBExpression> projects) {
    return implementor.project(this, projects);
  }

  @Override
  public QueryableDataSet aggregate(OneDBImplementor implementor, List<Integer> groups,
      List<OneDBExpression> aggs, List<FieldType> inputTypes) {
    return implementor.aggregate(this, groups, aggs, inputTypes);
  }

  @Override
  public QueryableDataSet sort(OneDBImplementor implementor, List<String> orders) {
    return implementor.sort(this, orders);
  }

  @Override
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
