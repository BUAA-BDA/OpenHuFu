package com.hufudb.onedb.core.query;

import com.hufudb.onedb.core.data.EnumerableDataSet;
import com.hufudb.onedb.core.data.FieldType;
import com.hufudb.onedb.core.query.implementor.OneDBImplementor;
import com.hufudb.onedb.core.query.implementor.utils.OneDBJoinInfo;
import com.hufudb.onedb.core.sql.expression.OneDBExpression;
import java.util.List;

public interface QueryableDataSet extends EnumerableDataSet {

  List<FieldType> getTypeList();

  QueryableDataSet join(OneDBImplementor implementor, QueryableDataSet right,
      OneDBJoinInfo joinInfo);

  QueryableDataSet filter(OneDBImplementor implementor, List<OneDBExpression> filters);

  QueryableDataSet project(OneDBImplementor implementor, List<OneDBExpression> projects);

  QueryableDataSet aggregate(OneDBImplementor implementor, List<Integer> groups,
      List<OneDBExpression> aggs, List<FieldType> inputTypes);

  QueryableDataSet sort(OneDBImplementor implementor, List<String> orders);

  QueryableDataSet limit(int offset, int fetch);
}
