package com.hufudb.onedb.core.sql.rel;

import java.util.List;

import com.hufudb.onedb.core.data.Header;
import com.hufudb.onedb.core.sql.expression.OneDBAggCall;
import com.hufudb.onedb.core.sql.expression.OneDBExpression;
import com.hufudb.onedb.core.sql.expression.OneDBOperator;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.plan.volcano.RelSubset;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Calc;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexProgram;

public class OneDBCalc extends Calc implements OneDBRel {
  public OneDBCalc(RelOptCluster cluster, RelTraitSet traits, List<RelHint> hints, RelNode child, RexProgram program) {
    super(cluster, traits, hints, child, program);
  }

  @Override
  public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
    RelNode child = ((RelSubset)getInput()).getBest();
    if (child instanceof OneDBAggCall) {
      return planner.getCostFactory().makeInfiniteCost();
    }
    return super.computeSelfCost(planner, mq).multiplyBy(0.5);
  }

  @Override
  public void implement(Implementor implementor) {
    implementor.visitChild(getInput());
    Header header = implementor.getHeader();
    RexProgram program = getProgram();
    assert header.size() == program.getInputRowType().getFieldCount();
    List<OneDBExpression> calcs = OneDBOperator.fromRexNodes(program, implementor.getCurrentOutput());
    implementor.setSelectExps(calcs);
  }

  @Override
  public Calc copy(RelTraitSet traits, RelNode child, RexProgram program) {
    return new OneDBCalc(getCluster(), traits, getHints(), child, program);
  }
}
