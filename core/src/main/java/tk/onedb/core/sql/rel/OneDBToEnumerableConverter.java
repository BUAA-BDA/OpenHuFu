package tk.onedb.core.sql.rel;

import java.util.List;

import org.apache.calcite.adapter.enumerable.EnumerableRel;
import org.apache.calcite.adapter.enumerable.EnumerableRelImplementor;
import org.apache.calcite.adapter.enumerable.JavaRowFormat;
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
import org.apache.calcite.rel.type.RelDataType;

import tk.onedb.core.sql.expression.OneDBQuery;

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
    oImplementor.visitChild(0, getInput());
    return implement(implementor, oImplementor, pref);
  }

  public Result implement(EnumerableRelImplementor implementor, OneDBRel.Implementor oImplementor, Prefer pref) {
    final BlockBuilder builder = new BlockBuilder();
    final RelDataType rowType = getRowType();
    final PhysType physType = PhysTypeImpl.of(implementor.getTypeFactory(), rowType, pref.prefer(JavaRowFormat.ARRAY));
    OneDBQuery oQuery = oImplementor.getQuery();
    final Expression table = builder.append("table", oQuery.table.getExpression(OneDBTable.OneDBQueryable.class));
    final Expression query = builder.append("query", Expressions.constant(oQuery, OneDBQuery.class));
    Expression enumerable = builder.append("enumerable", Expressions.call(table, OneDBMethod.ONEDB_TABLE_QUERY.method, query));
    builder.add(Expressions.return_(null, enumerable));
    return implementor.result(physType, builder.toBlock());
  }
}
