package com.hufudb.onedb.core.implementor;

import com.hufudb.onedb.core.data.EnumerableDataSet;
import com.hufudb.onedb.core.data.ColumnType;
import com.hufudb.onedb.core.implementor.utils.OneDBJoinInfo;
import com.hufudb.onedb.core.sql.expression.OneDBExpression;
import com.hufudb.onedb.core.sql.rel.OneDBOrder;
import java.util.List;

public interface QueryableDataSet extends EnumerableDataSet {

  List<ColumnType> getTypeList();

  QueryableDataSet join(OneDBImplementor implementor, QueryableDataSet right,
      OneDBJoinInfo joinInfo);

  QueryableDataSet filter(OneDBImplementor implementor, List<OneDBExpression> filters);

  QueryableDataSet project(OneDBImplementor implementor, List<OneDBExpression> projects);

  QueryableDataSet aggregate(OneDBImplementor implementor, List<Integer> groups,
      List<OneDBExpression> aggs, List<ColumnType> inputTypes);

  QueryableDataSet sort(OneDBImplementor implementor, List<OneDBOrder> orders);

  QueryableDataSet limit(int offset, int fetch);
}
