package com.hufudb.onedb.core.sql.rel;

import com.hufudb.onedb.core.sql.schema.OneDBSchema;
import com.hufudb.onedb.plan.Plan;
import com.hufudb.onedb.plan.RootPlan;
import com.hufudb.onedb.proto.OneDBData.ColumnType;
import com.hufudb.onedb.proto.OneDBData.Modifier;
import com.hufudb.onedb.proto.OneDBPlan.Collation;
import com.hufudb.onedb.proto.OneDBPlan.Expression;
import com.hufudb.onedb.proto.OneDBPlan.JoinCondition;
import java.util.ArrayList;
import java.util.List;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.rel.RelNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public interface OneDBRel extends RelNode {
  Convention CONVENTION = new Convention.Impl("OneDB", OneDBRel.class);

  void implement(Implementor implementor);

  class Implementor {
    static final Logger LOG = LoggerFactory.getLogger(Implementor.class);
    
    OneDBSchema rootSchema;
    RootPlan rootPlan;
    Plan currentPlan;

    Implementor() {
      rootPlan = null;
      currentPlan = null;
    }

    public org.apache.calcite.linq4j.tree.Expression getRootSchemaExpression() {
      return rootSchema.getExpression();
    }

    public Plan getCurrentPlan() {
      return currentPlan;
    }

    public void setCurrentPlan(Plan plan) {
      this.currentPlan = plan;
    }

    public void visitChild(OneDBRel input) {
      input.implement(this);
    }

    public void setRootSchema(OneDBSchema rootSchema) {
      if (this.rootSchema == null) {
        this.rootSchema = rootSchema;
      }
    }

    public void setSelectExps(List<Expression> exps) {
      currentPlan.setSelectExps(exps);
    }

    public void setOrderExps(List<Collation> orderExps) {
      currentPlan.setOrders(orderExps);
    }

    public void addFilterExps(Expression exp) {
      // todo: filter on aggregation
      List<Expression> exps = currentPlan.getWhereExps();
      if (exps == null) {
        List<Expression> filters = new ArrayList<>();
        filters.add(exp);
        currentPlan.setWhereExps(filters);
      } else {
        exps.add(exp);
      }
    }

    public void setAggExps(List<Expression> exps) {
      currentPlan.setAggExps(exps);
    }

    public List<Expression> getAggExps() {
      return currentPlan.getAggExps();
    }

    public void setGroupSet(List<Integer> groups) {
      currentPlan.setGroups(groups);
    }

    public void setOffset(int offset) {
      currentPlan.setOffset(offset);
    }

    public void setFetch(int fetch) {
      currentPlan.setFetch(fetch);
    }


    public void setJoinInfo(JoinCondition condition) {
      currentPlan.setJoinInfo(condition);
    }

    public List<Expression> getCurrentOutput() {
      return currentPlan.getOutExpressions();
    }

    public List<ColumnType> getOutputTypes() {
      return currentPlan.getOutTypes();
    }

    public List<Modifier> getOutputModifier() {
      return currentPlan.getOutModifiers();
    }

    public RootPlan generatePlan() {
      rootPlan = new RootPlan();
      rootPlan.setChild(currentPlan);
      return rootPlan;
    }
  }
}
