package com.hufudb.onedb.core.sql.rel;

import com.google.common.collect.ImmutableList;
import com.hufudb.onedb.core.data.Header;
import com.hufudb.onedb.core.sql.expression.OneDBExpression;
import com.hufudb.onedb.core.sql.expression.OneDBOpType;
import com.hufudb.onedb.core.sql.expression.OneDBOperator;
import com.hufudb.onedb.core.sql.schema.OneDBSchema;
import com.hufudb.onedb.rpc.OneDBCommon.ExpressionProto;
import com.hufudb.onedb.rpc.OneDBCommon.OneDBQueryProto;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinInfo;

public interface OneDBRel extends RelNode {
  void implement(Implementor implementor);

  Convention CONVENTION = new Convention.Impl("OneDB", OneDBRel.class);

  class Implementor {
    OneDBSchema schema;
    OneDBQueryContext context;
    OneDBQueryProto.Builder rootContext;
    OneDBQueryProto.Builder currentContext;

    Implementor() {
      rootContext = OneDBQueryProto.newBuilder();
      currentContext = rootContext;
      context = new OneDBQueryContext();
    }

    public OneDBQueryContext buildContext() {
      context = OneDBQueryContext.fromProto(rootContext);
      return context;
    }

    public Long getContextId() {
      return context.getContextId();
    }

    public OneDBQueryProto getQuery() {
      return rootContext.build();
    }

    public Expression getSchemaExpression() {
      return schema.getExpression();
    }

    public void visitChild(RelNode input) {
      ((OneDBRel) input).implement(this);
    }

    public void visitChild(RelNode left, RelNode right) {
      OneDBQueryProto.Builder parentContext = currentContext;
      OneDBQueryProto.Builder leftContext = OneDBQueryProto.newBuilder();
      OneDBQueryProto.Builder rightContext = OneDBQueryProto.newBuilder();
      currentContext = leftContext;
      ((OneDBRel) left).implement(this);
      parentContext.setLeft(leftContext);
      currentContext = rightContext;
      ((OneDBRel) right).implement(this);
      parentContext.setRight(rightContext);
      currentContext = parentContext;
      ImmutableList.Builder<ExpressionProto> builder = ImmutableList.builder();
      int idx = 0;
      for (ExpressionProto exp : currentContext.getLeft().getSelectExpList()) {
        builder.add(
            ExpressionProto.newBuilder()
                .setOpType(OneDBOpType.REF.ordinal())
                .setOutType(exp.getOutType())
                .setRef(idx)
                .build());
        ++idx;
      }
      for (ExpressionProto exp : currentContext.getRight().getSelectExpList()) {
        builder.add(
            ExpressionProto.newBuilder()
                .setOpType(OneDBOpType.REF.ordinal())
                .setOutType(exp.getOutType())
                .setRef(idx)
                .build());
        ++idx;
      }
      setSelectExpProtos(builder.build());
    }

    public void setSchema(OneDBSchema schema) {
      if (this.schema == null) {
        this.schema = schema;
      }
    }

    public void setTableName(String tableName) {
      currentContext.setTableName(tableName);
    }

    // todo: pass proto
    public void setSelectExps(List<OneDBExpression> exps) {
      currentContext.clearSelectExp();
      currentContext.addAllSelectExp(
          exps.stream().map(exp -> exp.toProto()).collect(Collectors.toList()));
    }

    void setSelectExpProtos(List<ExpressionProto> exps) {
      currentContext.clearSelectExp();
      currentContext.addAllSelectExp(exps);
    }

    public void addFilterExps(OneDBExpression exp) {
      currentContext.addWhereExp(exp.toProto());
    }

    public void setAggExps(List<OneDBExpression> exps) {
      currentContext.clearAggExp();
      currentContext.addAllAggExp(
          exps.stream().map(exp -> exp.toProto()).collect(Collectors.toList()));
    }

    public void setOffset(int offset) {
      currentContext.setOffset(offset);
    }

    public void setFetch(int fetch) {
      currentContext.setFetch(fetch);
    }

    public void setJoinCondition(JoinInfo joinInfo) {
      if (!joinInfo.nonEquiConditions.isEmpty()) {
        currentContext.clearJoinCond();
        currentContext.addAllJoinCond(
            OneDBOperator.fromRexNodes(joinInfo.nonEquiConditions, getCurrentOutput()).stream()
                .map(exp -> exp.toProto())
                .collect(Collectors.toList()));
      }
      currentContext.addAllLeftKey(joinInfo.leftKeys).addAllRightKey(joinInfo.rightKeys);
    }

    public boolean hasJoin() {
      return currentContext.hasLeft() && currentContext.hasRight();
    }

    public Header getHeader() {
      return OneDBExpression.generateHeaderFromProto(currentContext.getSelectExpList());
    }

    public List<OneDBExpression> getCurrentOutput() {
      return currentContext.getSelectExpList().stream()
          .map(exp -> OneDBExpression.fromProto(exp))
          .collect(Collectors.toList());
    }
  }
}
