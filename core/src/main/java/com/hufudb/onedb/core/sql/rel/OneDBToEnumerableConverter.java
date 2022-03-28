package com.hufudb.onedb.core.sql.rel;

import java.util.List;

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

public class OneDBToEnumerableConverter extends ConverterImpl implements EnumerableRel {

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
      oImplementor.visitChild(getInput());
    } catch(Exception e) {
      e.printStackTrace();
    }
    return implement(implementor, oImplementor, pref);
  }

  public Result implement(EnumerableRelImplementor implementor, OneDBRel.Implementor oImplementor, Prefer pref) {
    final BlockBuilder builder = new BlockBuilder();
    final PhysType physType = PhysTypeImpl.of(implementor.getTypeFactory(), getRowType(), pref.preferArray());
    String queryProtoStr = oImplementor.getQuery().toString();
    // get OneDBSchema for query
    Expression schema = oImplementor.getSchemaExpression();
    // get queryContext
    Expression queryContext = builder.append("queryContext", Expressions.constant(queryProtoStr, String.class));
    // call the query method on onedbschema
    Expression enumerable = builder.append("enumerable", Expressions.call(schema, "query", queryContext));
    builder.add(Expressions.return_(null, enumerable));
    return implementor.result(physType, builder.toBlock());
  }
}
