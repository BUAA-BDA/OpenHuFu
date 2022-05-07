package com.hufudb.onedb.owner.implementor;

import java.util.List;
import com.hufudb.onedb.core.data.BasicDataSet;
import com.hufudb.onedb.core.data.FieldType;
import com.hufudb.onedb.core.data.Header;
import com.hufudb.onedb.core.implementor.OneDBImplementor;
import com.hufudb.onedb.core.implementor.QueryableDataSet;
import com.hufudb.onedb.core.implementor.utils.OneDBJoinInfo;
import com.hufudb.onedb.core.sql.expression.OneDBExpression;
import com.hufudb.onedb.core.sql.rel.OneDBOrder;

public class OwnerQueryableDataSet extends BasicDataSet implements QueryableDataSet {
  public OwnerQueryableDataSet(BasicDataSet dataSet) {
    super(dataSet.getHeader(), dataSet.getRows());
  }

  public OwnerQueryableDataSet(Header header) {
    super(header);
  }

  @Override
  public List<FieldType> getTypeList() {
    return null;
  }

  @Override
  public QueryableDataSet join(OneDBImplementor implementor, QueryableDataSet right,
      OneDBJoinInfo joinInfo) {
    return null;
  }

  @Override
  public QueryableDataSet filter(OneDBImplementor implementor, List<OneDBExpression> filters) {
    LOG.error("Not support filter in owner side");
    throw new UnsupportedOperationException();
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
  public QueryableDataSet sort(OneDBImplementor implementor, List<OneDBOrder> orders) {
    LOG.error("Not support project in owner side");
    throw new UnsupportedOperationException();
  }

  @Override
  public QueryableDataSet limit(int offset, int fetch) {
    return null;
  }
}
