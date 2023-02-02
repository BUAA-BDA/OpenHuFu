package com.hufudb.openhufu.core.sql.rel;

import com.hufudb.openhufu.core.sql.schema.FQSchemaManager;
import com.hufudb.openhufu.plan.Plan;
import com.hufudb.openhufu.plan.RootPlan;
import com.hufudb.openhufu.proto.OpenHuFuData.ColumnType;
import com.hufudb.openhufu.proto.OpenHuFuData.Modifier;
import com.hufudb.openhufu.proto.OpenHuFuPlan.Collation;
import com.hufudb.openhufu.proto.OpenHuFuPlan.Expression;
import com.hufudb.openhufu.proto.OpenHuFuPlan.JoinCondition;
import java.util.List;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.rel.RelNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public interface FQRel extends RelNode {
  Convention CONVENTION = new Convention.Impl("OneDB", FQRel.class);

  void implement(Implementor implementor);

  class Implementor {
    static final Logger LOG = LoggerFactory.getLogger(Implementor.class);
    
    FQSchemaManager schemaManager;
    RootPlan rootPlan;
    Plan currentPlan;

    Implementor() {
      rootPlan = null;
      currentPlan = null;
    }

    public org.apache.calcite.linq4j.tree.Expression getRootSchemaExpression() {
      return schemaManager.getExpression();
    }

    public Plan getCurrentPlan() {
      return currentPlan;
    }

    public void setCurrentPlan(Plan plan) {
      this.currentPlan = plan;
    }

    public void visitChild(FQRel input) {
      input.implement(this);
    }

    public void setSchemaManager(FQSchemaManager schemaManager) {
      if (this.schemaManager == null) {
        this.schemaManager = schemaManager;
      }
    }

    public void setSelectExps(List<Expression> exps) {
      currentPlan.setSelectExps(exps);
    }

    public void setOrderExps(List<Collation> orderExps) {
      currentPlan.setOrders(orderExps);
    }

    public void setFilterExps(List<Expression> exp) {
      currentPlan.setWhereExps(exp);
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
