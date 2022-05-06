package com.hufudb.onedb.core.sql.context;

import java.util.List;
import java.util.stream.Collectors;
import com.hufudb.onedb.core.client.OneDBClient;
import com.hufudb.onedb.core.client.OwnerClient;
import com.hufudb.onedb.core.data.FieldType;
import com.hufudb.onedb.core.data.Level;
import com.hufudb.onedb.core.rewriter.OneDBRewriter;
import com.hufudb.onedb.core.sql.expression.OneDBExpression;
import com.hufudb.onedb.core.sql.expression.OneDBReference;
import com.hufudb.onedb.rpc.OneDBCommon.QueryContextProto;
import org.apache.commons.lang3.tuple.Pair;

public class OneDBPlaceholderContext extends OneDBBaseContext {
  List<OneDBExpression> output;

  public OneDBPlaceholderContext(List<OneDBExpression> output) {
    this.output = OneDBReference.fromExps(output);
  }

  public QueryContextProto toProto() {
    return QueryContextProto.newBuilder().setContextType(getContextType().ordinal())
        .addAllSelectExp(OneDBExpression.toProto(output)).build();
  }

  public static OneDBPlaceholderContext fromProto(QueryContextProto proto) {
    return new OneDBPlaceholderContext(OneDBExpression.fromProto(proto.getSelectExpList()));
  }

  @Override
  public List<Pair<OwnerClient, QueryContextProto>> generateOwnerContextProto(OneDBClient client) {
    LOG.error("not support");
    throw new UnsupportedOperationException();
  }

  @Override
  public OneDBContextType getContextType() {
    return OneDBContextType.PLACEHOLDER;
  }

  @Override
  public List<OneDBExpression> getOutExpressions() {
    return output;
  }

  @Override
  public List<FieldType> getOutTypes() {
    return output.stream().map(exp -> exp.getOutType()).collect(Collectors.toList());
  }

  @Override
  public Level getContextLevel() {
    return Level.findDominator(getOutExpressions());
  }

  @Override
  public List<Level> getOutLevels() {
    return output.stream().map(exp -> exp.getLevel()).collect(Collectors.toList());
  }

  @Override
  public OneDBContext rewrite(OneDBRewriter rewriter) {
    LOG.error("not support");
    throw new UnsupportedOperationException();
  }
}
