package com.hufudb.onedb.core.sql.rel;

import com.google.common.collect.ImmutableList;
import com.hufudb.onedb.core.data.FieldType;
import com.hufudb.onedb.core.sql.context.OneDBLeafContext;
import com.hufudb.onedb.core.sql.context.OneDBBinaryContext;
import com.hufudb.onedb.core.sql.context.OneDBContext;
import com.hufudb.onedb.core.sql.context.OneDBRootContext;
import com.hufudb.onedb.core.sql.context.OneDBUnaryContext;
import com.hufudb.onedb.core.sql.expression.OneDBExpression;
import com.hufudb.onedb.core.sql.expression.OneDBJoinType;
import com.hufudb.onedb.core.sql.expression.OneDBOperator;
import com.hufudb.onedb.core.sql.expression.OneDBReference;
import com.hufudb.onedb.core.sql.implementor.utils.OneDBJoinInfo;
import com.hufudb.onedb.core.sql.schema.OneDBSchema;
import java.util.ArrayList;
import java.util.List;
import java.util.Stack;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinInfo;
import org.apache.calcite.rel.core.JoinRelType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public interface OneDBRel extends RelNode {
  Convention CONVENTION = new Convention.Impl("OneDB", OneDBRel.class);

  void implement(Implementor implementor);

  class Implementor {
    static final Logger LOG = LoggerFactory.getLogger(Implementor.class);
    
    OneDBSchema schema;
    OneDBRootContext rootContext;
    OneDBContext currentContext;
    Stack<OneDBContext> stack;

    Implementor() {
      rootContext = new OneDBRootContext();
      currentContext = rootContext;
      stack = new Stack<>();
    }

    public Long getContextId() {
      return rootContext.getContextId();
    }

    public OneDBRootContext getRootContext() {
      return rootContext;
    }

    public Expression getSchemaExpression() {
      return schema.getExpression();
    }

    public OneDBContext getCurrentContext() {
      return currentContext;
    }

    public void visitChild(OneDBRel input) {
      input.implement(this);
    }

    public void createLeafContext() {
      OneDBContext parent = currentContext;
      currentContext = new OneDBLeafContext();
      currentContext.setParent(parent);
      parent.updateChild(currentContext, null);
    }

    public void createBinaryContext(OneDBContext left, OneDBContext right) {
      OneDBContext parent = currentContext.getParent();
      OneDBBinaryContext joinContext = new OneDBBinaryContext(parent, left, right);
      left.setParent(joinContext);
      right.setParent(joinContext);
      currentContext = joinContext;
      List<OneDBExpression> exps = new ArrayList<>();
      int idx = 0;
      for (OneDBExpression exp: left.getOutExpressions()) {
        exps.add(new OneDBReference(exp.getOutType(), idx));
        ++idx;
      }
      for (OneDBExpression exp : right.getOutExpressions()) {
        exps.add(new OneDBReference(exp.getOutType(), idx));
        ++idx;
      }
      currentContext.setSelectExps(exps);
    }

    public void setSchema(OneDBSchema schema) {
      if (this.schema == null) {
        this.schema = schema;
      }
    }

    public void setTableName(String tableName) {
      currentContext.setTableName(tableName);
    }

    public void setSelectExps(List<OneDBExpression> exps) {
      currentContext.setSelectExps(exps);
    }

    public void setOrderExps(List<String> orderExps) {
      currentContext.setOrders(orderExps);
    }

    public void addFilterExps(OneDBExpression exp) {
      List<OneDBExpression> exps = currentContext.getWhereExps();
      if (exps == null) {
        List<OneDBExpression> filters = new ArrayList<>();
        filters.add(exp);
        currentContext.setWhereExps(filters);
      } else {
        exps.add(exp);
      }
    }

    public void setAggExps(List<OneDBExpression> exps) {
      List<OneDBExpression> currentAggs = currentContext.getAggExps();
      if (currentAggs != null && !currentAggs.isEmpty()) {
        OneDBUnaryContext unary = new OneDBUnaryContext();
        unary.setChildren(ImmutableList.of(currentContext));
        currentContext.getParent().updateChild(unary, currentContext);
        currentContext.setParent(unary);
        currentContext = unary;
      }
      currentContext.setAggExps(exps);
    }

    public List<OneDBExpression> getAggExps() {
      return currentContext.getAggExps();
    }

    public void setGroupSet(List<Integer> groups) {
      currentContext.setGroups(groups);
    }

    public void setOffset(int offset) {
      currentContext.setOffset(offset);
    }

    public void setFetch(int fetch) {
      currentContext.setFetch(fetch);
    }


    public void setJoinInfo(JoinInfo joinInfo, JoinRelType joinRelType) {
      List<OneDBExpression> condition = OneDBOperator.fromRexNodes(joinInfo.nonEquiConditions, currentContext.getOutExpressions());
      currentContext.setJoinInfo(new OneDBJoinInfo(OneDBJoinType.of(joinRelType), joinInfo.leftKeys, joinInfo.rightKeys, condition));
    }

    public List<OneDBExpression> getCurrentOutput() {
      return currentContext.getOutExpressions();
    }

    public List<FieldType> getOutputTypes() {
      return currentContext.getOutTypes();
    }
  }
}
