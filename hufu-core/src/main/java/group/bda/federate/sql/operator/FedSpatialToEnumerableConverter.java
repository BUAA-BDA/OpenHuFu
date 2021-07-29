package group.bda.federate.sql.operator;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.calcite.adapter.enumerable.EnumerableRel;
import org.apache.calcite.adapter.enumerable.EnumerableRelImplementor;
import org.apache.calcite.adapter.enumerable.JavaRowFormat;
import org.apache.calcite.adapter.enumerable.PhysType;
import org.apache.calcite.adapter.enumerable.PhysTypeImpl;
import org.apache.calcite.linq4j.tree.BlockBuilder;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.linq4j.tree.MethodCallExpression;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterImpl;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.util.BuiltInMethod;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.Util;

import group.bda.federate.rpc.FederateCommon.IR;
import group.bda.federate.sql.expression.FedSpatialExpression;
import group.bda.federate.sql.functions.AggregateType;
import group.bda.federate.sql.table.FederateTable;

public class FedSpatialToEnumerableConverter extends ConverterImpl implements EnumerableRel {

  public FedSpatialToEnumerableConverter(RelOptCluster cluster, RelTraitSet traits, RelNode input) {
    super(cluster, ConventionTraitDef.INSTANCE, traits, input);
  }

  @Override
  public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
    return new FedSpatialToEnumerableConverter(getCluster(), traitSet, sole(inputs));
  }

  @Override
  public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
    return planner.getCostFactory().makeZeroCost();
  }

  @Override
  public Result implement(EnumerableRelImplementor implementor, Prefer pref) {
    final FedSpatialRel.Implementor fedImplementor = new FedSpatialRel.Implementor();
    fedImplementor.visitChild(0, getInput());
    if (fedImplementor.hasJoin()) {
      return implementJoin(implementor, fedImplementor, pref);
    } else {
      return implementQuery(implementor, fedImplementor, pref);
    }
  }

  private Result implementQuery(EnumerableRelImplementor implementor, FedSpatialRel.Implementor fedImplementor, Prefer pref) {
    final BlockBuilder builder = new BlockBuilder();
    final RelDataType rowType = getRowType();
    final PhysType physType = PhysTypeImpl.of(implementor.getTypeFactory(), rowType, pref.prefer(JavaRowFormat.ARRAY));
    List<Map.Entry<AggregateType, List<Integer>>> aggList = new ArrayList<>();
    List<String> projectList = fedImplementor.selectExps.getExpStrings();

    for (Map.Entry<AggregateType, List<Integer>> entry : Pair.zip(fedImplementor.selectExps.getAggregateMap().keySet(),
            fedImplementor.selectExps.getAggregateMap().values())) {
      aggList.add(entry);
    }

    final Expression table = builder.append("table",
            fedImplementor.table.getExpression(FederateTable.FederateQueryable.class));
    final Expression aggFields = builder.append("aggregateFields", constantArrayList(aggList, Pair.class));
    final Expression filter = builder.append("filter",
            Expressions.constant(fedImplementor.filterExp != null ? fedImplementor.getFilterExp().toString() : ""));
    final Expression project = builder.append("project", constantArrayList(projectList, String.class));
    final Expression offset = builder.append("offset", Expressions.constant(fedImplementor.offset));
    final Expression fetch = builder.append("fetch", Expressions.constant(fedImplementor.fetch));
    final Expression order = builder.append("order", constantArrayList(fedImplementor.order, String.class));
    Expression enumerable = builder.append("enumerable", Expressions.call(table,
            FedSpatialMethod.FED_SPATIAL_QUERY.method, project, filter, aggFields, offset, fetch, order));
    builder.add(Expressions.return_(null, enumerable));
    return implementor.result(physType, builder.toBlock());
  }

  private Result implementJoin(EnumerableRelImplementor implementor, FedSpatialRel.Implementor fedImplementor, Prefer pref) {
    final BlockBuilder builder = new BlockBuilder();
    final RelDataType rowType = getRowType();
    final PhysType physType = PhysTypeImpl.of(implementor.getTypeFactory(), rowType, pref.prefer(JavaRowFormat.ARRAY));
    List<Map.Entry<AggregateType, List<Integer>>> aggList = new ArrayList<>();
    List<Integer> projectList = new ArrayList<>();
    for (FedSpatialExpression exp : fedImplementor.selectExps) {
      IR ir = exp.getLastIR();
      projectList.add(ir.getIn(0).getRef());
    }
    for (Map.Entry<AggregateType, List<Integer>> entry : Pair.zip(fedImplementor.selectExps.getAggregateMap().keySet(),
            fedImplementor.selectExps.getAggregateMap().values())) {
      aggList.add(entry);
    }
    FedSpatialRel.SingleQuery left = fedImplementor.getSingleQuery(0);
    FedSpatialRel.SingleQuery right = fedImplementor.getSingleQuery(1);
    final Expression leftTable = builder.append("leftTable", fedImplementor.getQueryParams(0).getTable().getExpression(FederateTable.FederateQueryable.class));
    final Expression leftQuery = builder.append("leftQuery", Expressions.constant(left, FedSpatialRel.SingleQuery.class));
    final Expression rightQuery = builder.append("rightQuery", Expressions.constant(right, FedSpatialRel.SingleQuery.class));

    final Expression aggFields = builder.append("aggregateFields", constantArrayList(aggList, Pair.class));
    final Expression project = builder.append("project", constantArrayList(projectList, Integer.class));
    final Expression join = builder.append("join", Expressions.constant(fedImplementor.getJoinInfo()));
    final Expression offset = builder.append("offset", Expressions.constant(fedImplementor.offset));
    final Expression fetch = builder.append("fetch", Expressions.constant(fedImplementor.fetch));
    final Expression order = builder.append("order", constantArrayList(fedImplementor.order, String.class));
    Expression enumerable = builder.append("enumerable", Expressions.call(leftTable, FedSpatialMethod.FED_SPATIAL_JOIN.method, leftQuery, rightQuery, join, project, aggFields, offset, fetch, order));
    builder.add(Expressions.return_(null, enumerable));
    return implementor.result(physType, builder.toBlock());
  }

  /**
   * E.g. {@code constantArrayList("x", "y")} returns "Arrays.asList('x', 'y')".
   */
  private static <T> MethodCallExpression constantArrayList(List<T> values, Class clazz) {
    return Expressions.call(BuiltInMethod.ARRAYS_AS_LIST.method, Expressions.newArrayInit(clazz, constantList(values)));
  }

  /**
   * E.g. {@code constantList("x", "y")} returns {@code {ConstantExpression("x"),
   * ConstantExpression("y")}}.
   */
  private static <T> List<Expression> constantList(List<T> values) {
    return Util.transform(values, Expressions::constant);
  }
}
