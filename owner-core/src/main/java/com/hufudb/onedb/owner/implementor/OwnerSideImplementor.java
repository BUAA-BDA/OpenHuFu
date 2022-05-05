package com.hufudb.onedb.owner.implementor;

import java.util.List;
import com.hufudb.onedb.core.data.FieldType;
import com.hufudb.onedb.core.data.Level;
import com.hufudb.onedb.core.implementor.OneDBImplementor;
import com.hufudb.onedb.core.implementor.QueryableDataSet;
import com.hufudb.onedb.core.sql.context.OneDBContext;
import com.hufudb.onedb.core.sql.context.OneDBContextType;
import com.hufudb.onedb.core.sql.context.OneDBLeafContext;
import com.hufudb.onedb.core.sql.expression.OneDBExpression;
import com.hufudb.onedb.rpc.Rpc;

public abstract class OwnerSideImplementor implements OneDBImplementor {
  Rpc rpc;

  protected OwnerSideImplementor(Rpc rpc) {
    this.rpc = rpc;
  }

  @Override
  public QueryableDataSet implement(OneDBContext context) {
    return context.implement(this);
  }

  @Override
  public QueryableDataSet aggregate(QueryableDataSet in, List<Integer> groups,
          List<OneDBExpression> aggs, List<FieldType> inputTypes) {
    return null;
  }

  // todo: change database adapter as plugin and implement this method
  @Override
  public QueryableDataSet leafQuery(OneDBLeafContext leaf) {
    return null;
  }
}
