package com.hufudb.onedb.core.sql.context;

import java.util.List;
import com.hufudb.onedb.core.client.OneDBClient;
import com.hufudb.onedb.core.client.OwnerClient;
import com.hufudb.onedb.core.data.FieldType;
import com.hufudb.onedb.core.data.Level;
import com.hufudb.onedb.core.rewriter.OneDBRewriter;
import com.hufudb.onedb.core.sql.expression.OneDBExpression;
import com.hufudb.onedb.rpc.OneDBCommon.QueryContextProto;
import org.apache.commons.lang3.tuple.Pair;

public class OneDBPlaceholderContext extends OneDBBaseContext {
  public static final OneDBPlaceholderContext PLACEHOLDER = new OneDBPlaceholderContext();
  public static final QueryContextProto PLACEHOLDER_PROTO = PLACEHOLDER.toProto();

  public QueryContextProto toProto() {
    return QueryContextProto.newBuilder().setContextType(OneDBContextType.PLACEHOLDER.ordinal())
        .build();
  }

  @Override
  public List<Pair<OwnerClient, QueryContextProto>> generateOwnerContextProto(
      OneDBClient client) {
    LOG.error("not support");
    throw new UnsupportedOperationException();
  }

  @Override
  public OneDBContextType getContextType() {
    return OneDBContextType.PLACEHOLDER;
  }

  @Override
  public List<OneDBExpression> getOutExpressions() {
    LOG.error("not support");
    throw new UnsupportedOperationException();
  }

  @Override
  public List<FieldType> getOutTypes() {
    LOG.error("not support");
    throw new UnsupportedOperationException();
  }

  @Override
  public Level getContextLevel() {
    LOG.error("not support");
    throw new UnsupportedOperationException();
  }

  @Override
  public List<Level> getOutLevels() {
    LOG.error("not support");
    throw new UnsupportedOperationException();
  }

  @Override
  public OneDBContext rewrite(OneDBRewriter rewriter) {
    LOG.error("not support");
    throw new UnsupportedOperationException();
  }
}
