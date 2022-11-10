package com.hufudb.onedb.core.sql.rel;

import java.util.List;
import com.hufudb.onedb.plan.QueryPlanPool;
import com.hufudb.onedb.plan.RootPlan;
import org.apache.calcite.adapter.enumerable.EnumerableRel;
import org.apache.calcite.adapter.enumerable.EnumerableRelImplementor;
import org.apache.calcite.adapter.enumerable.PhysType;
import org.apache.calcite.adapter.enumerable.PhysTypeImpl;
import org.apache.calcite.linq4j.tree.BlockBuilder;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterImpl;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OneDBToEnumerableConverter extends ConverterImpl implements EnumerableRel {
  private static final Logger LOG = LoggerFactory.getLogger(OneDBToEnumerableConverter.class);

  public OneDBToEnumerableConverter(RelOptCluster cluster, RelTraitSet traits, RelNode input) {
    super(cluster, ConventionTraitDef.INSTANCE, traits, input);
  }

  @Override
  public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
    return new OneDBToEnumerableConverter(getCluster(), traitSet, sole(inputs));
  }

  @Override
  public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
    return planner.getCostFactory().makeZeroCost();
  }

  @Override
  public Result implement(EnumerableRelImplementor implementor, Prefer pref) {
    final OneDBRel.Implementor oImplementor = new OneDBRel.Implementor();
    try {
      oImplementor.visitChild((OneDBRel) getInput());
    } catch (Exception e) {
      e.printStackTrace();
    }
    return implement(implementor, oImplementor, pref);
  }

  public Result implement(EnumerableRelImplementor implementor, OneDBRel.Implementor oImplementor,
      Prefer pref) {
    final BlockBuilder builder = new BlockBuilder();
    final PhysType physType =
        PhysTypeImpl.of(implementor.getTypeFactory(), getRowType(), pref.preferArray());
    // build and save context for the query
    RootPlan root = oImplementor.generatePlan();
    QueryPlanPool.savePlan(root);
    // get OneDBSchema for query
    Expression schema = oImplementor.getRootSchemaExpression();
    // call the query method on onedbschema
    Expression enumerable = builder.append("enumerable",
        Expressions.call(schema, "query", Expressions.constant(root.getPlanId())));
    builder.add(Expressions.return_(null, enumerable));
    try {
      return implementor.result(physType, builder.toBlock());
    } catch (Exception e) {
      LOG.error("Fail to execute query: {}", e.getMessage());
      throw e;
    }
  }
}
