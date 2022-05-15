package com.hufudb.onedb.core.implementor;

import java.util.List;
import com.hufudb.onedb.core.data.ColumnType;
import com.hufudb.onedb.core.implementor.utils.OneDBJoinInfo;
import com.hufudb.onedb.core.sql.context.OneDBBinaryContext;
import com.hufudb.onedb.core.sql.context.OneDBContext;
import com.hufudb.onedb.core.sql.context.OneDBLeafContext;
import com.hufudb.onedb.core.sql.context.OneDBUnaryContext;
import com.hufudb.onedb.core.sql.expression.OneDBExpression;
import com.hufudb.onedb.core.sql.rel.OneDBOrder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public interface OneDBImplementor {
  static final Logger LOG = LoggerFactory.getLogger(OneDBImplementor.class);

  QueryableDataSet implement(OneDBContext context);

  QueryableDataSet join(QueryableDataSet left, QueryableDataSet right, OneDBJoinInfo joinInfo);

  QueryableDataSet filter(QueryableDataSet in, List<OneDBExpression> filters);

  QueryableDataSet project(QueryableDataSet in, List<OneDBExpression> projects);

  QueryableDataSet aggregate(QueryableDataSet in, List<Integer> groups, List<OneDBExpression> aggs,
      List<ColumnType> inputTypes);

  QueryableDataSet sort(QueryableDataSet in, List<OneDBOrder> orders);

  QueryableDataSet binaryQuery(OneDBBinaryContext binary);

  QueryableDataSet unaryQuery(OneDBUnaryContext unary);

  QueryableDataSet leafQuery(OneDBLeafContext leaf);
}
