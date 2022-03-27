package com.hufudb.onedb.core.sql.rel;

import java.util.List;
import java.util.Stack;
import java.util.stream.Collectors;

import com.hufudb.onedb.core.data.Header;
import com.hufudb.onedb.core.sql.expression.OneDBExpression;
import com.hufudb.onedb.core.sql.schema.OneDBSchema;
import com.hufudb.onedb.rpc.OneDBCommon.OneDBQueryProto;

import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.rel.RelNode;

public interface OneDBRel extends RelNode {
  void implement(Implementor implementor);

  Convention CONVENTION = new Convention.Impl("OneDB", OneDBRel.class);

  class Implementor {
    OneDBSchema schema;
    OneDBQueryProto.Builder rootContext;
    OneDBQueryProto.Builder currentContext;
    Stack<OneDBQueryProto> contextStack;

    Implementor() {
      rootContext = OneDBQueryProto.newBuilder();
      currentContext = rootContext;
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
      OneDBQueryProto.Builder parentBuilder = currentContext;
      OneDBQueryProto.Builder leftBuilder = OneDBQueryProto.newBuilder();
      OneDBQueryProto.Builder rightBuilder = OneDBQueryProto.newBuilder();
      currentContext = leftBuilder;
      ((OneDBRel) left).implement(this);
      currentContext = rightBuilder;
      ((OneDBRel) right).implement(this);
      currentContext = parentBuilder;
    }

    // public void visitJoin(BiRel join) {
    //   OneDBQueryProto.Builder parentBuilder = currentContext;
    //   OneDBQueryProto.Builder leftBuilder = OneDBQueryProto.newBuilder();
    //   OneDBQueryProto.Builder rightBuilder = OneDBQueryProto.newBuilder();
    //   currentContext = leftBuilder;
    //   ((OneDBRel) join.getLeft()).implement(this);
    //   currentContext = rightBuilder;
    //   ((OneDBRel) join.getRight()).implement(this);
    //   currentContext = parentBuilder;
    // }

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
      currentContext.addAllSelectExp(exps.stream().map(exp -> exp.toProto()).collect(Collectors.toList()));
    }

    public void addFilterExps(OneDBExpression exp) {
      currentContext.addWhereExp(exp.toProto());
    }

    public void setAggExps(List<OneDBExpression> exps) {
      currentContext.addAllAggExp(exps.stream().map(exp -> exp.toProto()).collect(Collectors.toList()));
    }

    public void setOffset(int offset) {
      currentContext.setOffset(offset);
      // currentBuilder.offset = offset;
    }

    public void setFetch(int fetch) {
      currentContext.setFetch(fetch);
    }

    public Header getHeader() {
      return Header.EMPTY;
    }

    public List<OneDBExpression> getCurrentOutput() {
      return currentContext.getSelectExpList().stream().map(exp -> OneDBExpression.fromProto(exp)).collect(Collectors.toList());
    }
  }
}
