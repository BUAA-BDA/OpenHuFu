package com.hufudb.onedb.core.implementor.secure;

import java.util.List;
import com.hufudb.onedb.core.client.OneDBClient;
import com.hufudb.onedb.core.data.FieldType;
import com.hufudb.onedb.core.implementor.OneDBImplementor;
import com.hufudb.onedb.core.implementor.QueryableDataSet;
import com.hufudb.onedb.core.implementor.utils.OneDBJoinInfo;
import com.hufudb.onedb.core.sql.context.OneDBLeafContext;
import com.hufudb.onedb.core.sql.context.OneDBUnaryContext;
import com.hufudb.onedb.core.sql.expression.OneDBExpression;
import com.hufudb.onedb.rpc.OneDBCommon.LeafQueryProto;

public class SecureImplementor implements OneDBImplementor {

  private final OneDBClient client;

  public SecureImplementor(OneDBClient client) {
    this.client = client;
  }

  @Override
  public QueryableDataSet join(QueryableDataSet left, QueryableDataSet right,
      OneDBJoinInfo joinInfo) {
    return null;
  }

  @Override
  public QueryableDataSet filter(QueryableDataSet in, List<OneDBExpression> filters) {
    throw new UnsupportedOperationException("Secure Implementor not support join");
  }

  @Override
  public QueryableDataSet project(QueryableDataSet in, List<OneDBExpression> projects) {
    throw new UnsupportedOperationException("Secure Implementor not support project");
  }

  @Override
  public QueryableDataSet aggregate(QueryableDataSet in, List<Integer> groups,
      List<OneDBExpression> aggs, List<FieldType> inputTypes) {
    return null;
  }

  @Override
  public QueryableDataSet sort(QueryableDataSet in, List<String> orders) {
    throw new UnsupportedOperationException("Secure Implementor not support sort");
  }

  @Override
  public OneDBUnaryContext rewriteLeaf(OneDBLeafContext leaf) {
    return null;
  }

  @Override
  public QueryableDataSet leafQuery(OneDBLeafContext leaf) {
    LeafQueryProto proto = leaf.toProto();
    new SecureQueryableDataSet(client);
    return null;
  }
}
