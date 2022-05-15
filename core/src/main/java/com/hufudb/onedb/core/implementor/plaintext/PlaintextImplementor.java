package com.hufudb.onedb.core.implementor.plaintext;

import com.hufudb.onedb.core.client.OneDBClient;
import com.hufudb.onedb.core.data.ColumnType;
import com.hufudb.onedb.core.implementor.QueryableDataSet;
import com.hufudb.onedb.core.implementor.UserSideImplementor;
import com.hufudb.onedb.core.implementor.utils.OneDBJoinInfo;
import com.hufudb.onedb.core.sql.expression.OneDBExpression;
import com.hufudb.onedb.core.sql.rel.OneDBOrder;
import java.util.List;

/*
 * plaintext implementor of onedb query proto
 */
public class PlaintextImplementor extends UserSideImplementor {

  public PlaintextImplementor(OneDBClient client) {
    super(client);
  }

  @Override
  public QueryableDataSet join(QueryableDataSet left, QueryableDataSet right,
      OneDBJoinInfo joinInfo) {
    return PlaintextNestedLoopJoin.apply(left, right, joinInfo);
  }

  @Override
  public QueryableDataSet filter(QueryableDataSet in, List<OneDBExpression> filters) {
    return PlaintextFilter.apply(in, filters);
  }

  @Override
  public QueryableDataSet project(QueryableDataSet in, List<OneDBExpression> projects) {
    return PlaintextCalculator.apply(in, projects);
  }

  @Override
  public QueryableDataSet aggregate(QueryableDataSet in, List<Integer> groups,
      List<OneDBExpression> aggs, List<ColumnType> inputTypes) {
    return PlaintextAggregation.apply(in, groups, aggs, inputTypes);
  }

  @Override
  public QueryableDataSet sort(QueryableDataSet in, List<OneDBOrder> orders) {
    return PlaintextSort.apply(in, orders);
  }
}
