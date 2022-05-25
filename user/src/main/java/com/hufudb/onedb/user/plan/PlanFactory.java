package com.hufudb.onedb.user.plan;

import java.util.List;
import java.util.stream.Collectors;
import com.google.common.collect.ImmutableList;
import com.hufudb.onedb.core.sql.schema.OneDBSchemaManager;
import com.hufudb.onedb.core.table.OneDBTableSchema;
import com.hufudb.onedb.data.storage.utils.ModifierWrapper;
import com.hufudb.onedb.expression.ExpressionFactory;
import com.hufudb.onedb.plan.BinaryPlan;
import com.hufudb.onedb.plan.LeafPlan;
import com.hufudb.onedb.plan.Plan;
import com.hufudb.onedb.proto.OneDBData.ColumnDesc;
import com.hufudb.onedb.proto.OneDBData.Modifier;
import com.hufudb.onedb.proto.OneDBPlan.Expression;
import com.hufudb.onedb.proto.OneDBPlan.JoinCondition;
import com.hufudb.onedb.proto.OneDBPlan.JoinType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Factory to create template query plan
 */
public class PlanFactory {
  static final Logger LOG = LoggerFactory.getLogger(Plan.class);

  private final OneDBSchemaManager manager;

  public PlanFactory(OneDBSchemaManager manager) {
    this.manager = manager;
  }

  public Plan createEqualJoin(String leftTableName, String rightTableName, List<String> leftKeys, List<String> rightKeys, List<String> leftOut, List<String> rightOut) {
    final OneDBTableSchema leftSchema = manager.getTableSchema(leftTableName);
    final OneDBTableSchema rightSchema = manager.getTableSchema(rightTableName);
    if (leftSchema == null || rightSchema == null) {
      LOG.error("Join table not exists");
      throw new RuntimeException("Join table not exits");
    }
    LeafPlan leftPlan = new LeafPlan();
    LeafPlan rightPlan = new LeafPlan();
    leftPlan.setTableName(leftTableName);
    rightPlan.setTableName(rightTableName);
    List<Expression> leftOutExps = createInputRef(leftSchema, getColumnIds(leftSchema, leftOut));
    List<Expression> rightOutExps = createInputRef(rightSchema, getColumnIds(rightSchema, rightOut));
    leftPlan.setSelectExps(leftOutExps);
    rightPlan.setSelectExps(rightOutExps);
    Modifier mod = ModifierWrapper.dominate(leftPlan.getPlanModifier(), rightPlan.getPlanModifier());
    List<Integer> leftKeyIds = leftKeys.stream().map(key -> leftOut.indexOf(key)).collect(Collectors.toList());
    List<Integer> rightKeyIds = rightKeys.stream().map(key -> rightOut.indexOf(key)).collect(Collectors.toList());
    ImmutableList.Builder<Expression> refBuilder = ImmutableList.builder();
    int idx = 0;
    for (Expression exp: leftPlan.getOutExpressions()) {
      refBuilder.add(ExpressionFactory.createInputRef(idx, exp.getOutType(), exp.getModifier()));
      ++idx;
    }
    for (Expression exp : rightPlan.getOutExpressions()) {
      refBuilder.add(ExpressionFactory.createInputRef(idx, exp.getOutType(), exp.getModifier()));
      ++idx;
    }
    JoinCondition joinCond = JoinCondition.newBuilder().setType(JoinType.INNER).addAllLeftKey(leftKeyIds).addAllRightKey(rightKeyIds).setModifier(mod).build();
    BinaryPlan joinPlan = new BinaryPlan(leftPlan, rightPlan);
    joinPlan.setJoinInfo(joinCond);
    joinPlan.setSelectExps(refBuilder.build());
    return joinPlan;
  }

  static List<Integer> getColumnIds(OneDBTableSchema schema, List<String> columnNames) {
    return columnNames.stream().map(column -> schema.getColumnId(column)).collect(Collectors.toList());
  }

  static List<Expression> createInputRef(OneDBTableSchema schema, List<Integer> columnIds) {
    return columnIds.stream().map(columnId -> {
      ColumnDesc desc = schema.getSchema().getColumnDesc(columnId);
      return ExpressionFactory.createInputRef(columnId, desc.getType(), desc.getModifier());
    }).collect(Collectors.toList());
  }
}
