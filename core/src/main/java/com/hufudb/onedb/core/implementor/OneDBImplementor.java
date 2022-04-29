package com.hufudb.onedb.core.implementor;

import java.util.List;
import com.hufudb.onedb.core.client.OneDBClient;
import com.hufudb.onedb.core.data.FieldType;
import com.hufudb.onedb.core.implementor.plaintext.PlaintextImplementor;
import com.hufudb.onedb.core.implementor.utils.OneDBJoinInfo;
import com.hufudb.onedb.core.sql.context.OneDBContext;
import com.hufudb.onedb.core.sql.context.OneDBContextType;
import com.hufudb.onedb.core.sql.context.OneDBLeafContext;
import com.hufudb.onedb.core.sql.context.OneDBUnaryContext;
import com.hufudb.onedb.core.sql.context.OneDBRootContext;
import com.hufudb.onedb.core.sql.expression.OneDBExpression;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public interface OneDBImplementor {
  static final Logger LOG = LoggerFactory.getLogger(OneDBImplementor.class);

  default QueryableDataSet implement(OneDBContext context) {
    if (context.getContextType().equals(OneDBContextType.ROOT)) {
      context = ((OneDBRootContext) context).getChild();
    }
    return context.implement(this);
  }

  QueryableDataSet join(QueryableDataSet left, QueryableDataSet right, OneDBJoinInfo joinInfo);

  QueryableDataSet filter(QueryableDataSet in, List<OneDBExpression> filters);

  QueryableDataSet project(QueryableDataSet in, List<OneDBExpression> projects);

  QueryableDataSet aggregate(QueryableDataSet in, List<Integer> groups, List<OneDBExpression> aggs,
      List<FieldType> inputTypes);

  QueryableDataSet sort(QueryableDataSet in, List<String> orders);

  OneDBUnaryContext rewriteLeaf(OneDBLeafContext leaf);

  // rewrite leaf for global aggregation, sort and limit, return null if no rewritting is required
  QueryableDataSet leafQuery(OneDBLeafContext leaf);

  public static OneDBImplementor getImplementor(OneDBContext context, OneDBClient client) {
    switch (context.getContextLevel()) {
      case PUBLIC:
        return new PlaintextImplementor(client);
      default:
        LOG.error("No implementor found for Level {}", context.getContextLevel().name());
        return null;
    }
  }
}
