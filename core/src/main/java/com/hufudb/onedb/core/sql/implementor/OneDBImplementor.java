package com.hufudb.onedb.core.sql.implementor;

import java.util.List;
import com.hufudb.onedb.core.data.FieldType;
import com.hufudb.onedb.core.data.query.QueryableDataSet;
import com.hufudb.onedb.core.sql.context.OneDBLeafContext;
import com.hufudb.onedb.core.sql.context.OneDBUnaryContext;
import com.hufudb.onedb.core.sql.expression.OneDBExpression;
import com.hufudb.onedb.core.sql.implementor.utils.OneDBJoinInfo;

public interface OneDBImplementor {
  QueryableDataSet join(QueryableDataSet left, QueryableDataSet right, OneDBJoinInfo joinInfo);

  QueryableDataSet filter(QueryableDataSet in, List<OneDBExpression> filters);

  QueryableDataSet project(QueryableDataSet in, List<OneDBExpression> projects);

  QueryableDataSet aggregate(QueryableDataSet in, List<Integer> groups, List<OneDBExpression> aggs,
      List<FieldType> inputTypes);

  QueryableDataSet sort(QueryableDataSet in, List<String> orders);

  OneDBUnaryContext rewriteLeaf(OneDBLeafContext leaf);

  // rewrite leaf for global aggregation, sort and limit, return null if no rewritting is required
  QueryableDataSet leafQuery(OneDBLeafContext leaf);
}
