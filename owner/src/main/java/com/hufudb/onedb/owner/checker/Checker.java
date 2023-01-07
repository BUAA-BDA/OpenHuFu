package com.hufudb.onedb.owner.checker;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableList;
import com.hufudb.onedb.data.schema.SchemaManager;
import com.hufudb.onedb.data.schema.TableSchema;
import com.hufudb.onedb.desensitize.ExpSensitivityConvert;
import com.hufudb.onedb.expression.AggFuncType;
import com.hufudb.onedb.expression.ExpressionFactory;
import com.hufudb.onedb.plan.Plan;
import com.hufudb.onedb.proto.OneDBData;
import com.hufudb.onedb.proto.OneDBData.Modifier;
import com.hufudb.onedb.proto.OneDBPlan.JoinCondition;
import com.hufudb.onedb.proto.OneDBPlan.ExpSensitivity;
import com.hufudb.onedb.proto.OneDBPlan.Expression;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Check the modifier of query plan, the owner side implementor only executes plans that pass the
 * check
 */
public class Checker {
  static final Logger LOG = LoggerFactory.getLogger(Checker.class);


  static boolean dominate(Modifier out, Modifier in) {
    // todo: deal with specific protocol condition
    return out.ordinal() >= in.ordinal();
  }

  static boolean checkRef(Expression ref, List<Modifier> in) {
    int id = ref.getI32();
    if (id >= in.size() || id < 0) {
      LOG.warn("Column reference out of index");
      return false;
    }
    return ref.getModifier().ordinal() >= in.get(id).ordinal();
  }

  static boolean checkAgg(Expression agg, List<Modifier> in) {
    Modifier outModifier = agg.getModifier();
    for (Expression e : agg.getInList()) {
      if (!checkExpression(e, in)) {
        return false;
      }

      if (AggFuncType.isAllowedOnPrivate(agg.getI32())) {
        // if count on private cols, we allow it output a protected
        if (outModifier.equals(Modifier.PROTECTED) &&
            e.getModifier().equals(Modifier.PRIVATE)) {
          return true;
        }
      }

      if (!dominate(outModifier, e.getModifier())) {
        return false;
      }
    }
    return true;
  }

  static boolean checkExpression(Expression exp, List<Modifier> in) {
    switch (exp.getOpType()) {
      case REF:
        return checkRef(exp, in);
      case LITERAL:
        return true;
      case AGG_FUNC:
        return checkAgg(exp, in);
      default:
        Modifier outModifier = exp.getModifier();
        for (Expression e : exp.getInList()) {
          if (!checkExpression(e, in)) {
            return false;
          }
          if (!dominate(outModifier, e.getModifier())) {
            return false;
          }
        }
        return true;
    }
  }

  static boolean checkPlan(Plan plan, List<Modifier> in) {
    for (Expression exp : plan.getSelectExps()) {
      if (!checkExpression(exp, in)) {
        return false;
      }
    }
    for (Expression exp : plan.getAggExps()) {
      if (!checkExpression(exp, in)) {
        return false;
      }
    }
    return true;
  }

  public static boolean check(Plan plan, SchemaManager manager) {
    List<Modifier> in = ImmutableList.of();
    switch (plan.getPlanType()) {
      case LEAF:
        in = manager.getPublishedSchema(plan.getTableName()).getColumnDescs().stream()
            .map(desc -> desc.getModifier()).collect(Collectors.toList());
        break;
      case UNARY:
        if (!check(plan.getChildren().get(0), manager)) {
          return false;
        }
        in = plan.getChildren().get(0).getOutModifiers();
        break;
      case BINARY:
        if (!check(plan.getChildren().get(0), manager)) {
          return false;
        }
        if (!check(plan.getChildren().get(1), manager)) {
          return false;
        }
        in = new ArrayList<>();
        in.addAll(plan.getChildren().get(0).getOutModifiers());
        in.addAll(plan.getChildren().get(1).getOutModifiers());
        break;
      case EMPTY:
        return true;
      default:
        LOG.warn("Find unsupported plan type {}", plan.getPlanType());
        throw new RuntimeException("Unsupported plan type");
    }
    return checkPlan(plan, in);
  }

  public static void checkJoinCond(JoinCondition joinCondition, List<Expression> leftInputs, List<Expression> rightInputs) {
    for (int key : joinCondition.getLeftKeyList()) {
      Expression keyExp = leftInputs.get(key);
      if (keyExp.getSensitivity() != ExpSensitivity.NONE_SENSITIVE) {
        throw new RuntimeException(String.format("\nJoinCondition:%sLeft key is sensitive, can't do join", joinCondition));
      }
    }
    for (int key : joinCondition.getRightKeyList()) {
      Expression keyExp = rightInputs.get(key);
      if (keyExp.getSensitivity() != ExpSensitivity.NONE_SENSITIVE) {
        throw new RuntimeException(String.format("\nJoinCondition:%sRight key is sensitive, can't do join", joinCondition));
      }
    }
  }

  public static Expression sensitivityExp(Expression exp, SchemaManager manager, Plan plan, List<Expression> allInputs) {
    Expression rt = ExpressionFactory.addSensitivity(exp, ExpSensitivity.NONE_SENSITIVE);
    switch (exp.getOpType()) {
      case REF:
        if (allInputs == null) {
          TableSchema desensitizeTable = manager.getDesensitizationMap().get(manager.getActualTableName(plan.getTableName()));
          String refName = manager.getPublishedSchema(plan.getTableName()).getName(exp.getI32());
          OneDBData.Desensitize desensitize = desensitizeTable.getDesensitize(refName);
          if (desensitize.getSensitivity() != OneDBData.Sensitivity.PLAIN) {
            rt = ExpressionFactory.addSensitivity(exp, ExpSensitivity.SINGLE_SENSITIVE);
          }
        } else {
          ExpSensitivity expSensitivity = allInputs.get(exp.getI32()).getSensitivity();
          if (expSensitivity != ExpSensitivity.NONE_SENSITIVE) {
            rt = ExpressionFactory.addSensitivity(exp, ExpSensitivity.SINGLE_SENSITIVE);
          }
        }
        break;
      case LITERAL:
        break;
      case PLUS:
      case MINUS:
      case TIMES:
      case DIVIDE:
      case MOD:
      case GT:
      case GE:
      case LT:
      case LE:
      case EQ:
      case NE:
      case AND:
      case OR:
      case LIKE:
        assert exp.getInList().size() == 2;
        Map<ExpSensitivity, Integer> map = new HashMap<>();
        List<Expression> ins = new ArrayList<>();
        for (Expression e : exp.getInList()) {
          ExpSensitivity tmp = sensitivityExp(e, manager, plan, allInputs).getSensitivity();
          ins.add(exp);
          map.merge(tmp, 1, Integer::sum);
        }
        rt = ExpressionFactory.addSensitivity(exp, ExpSensitivityConvert.convertBinary(map), ins);
        break;

      case AS:
      case NOT:
      case PLUS_PRE:
      case MINUS_PRE:
      case IS_NULL:
      case IS_NOT_NULL:
        assert exp.getInList().size() == 1;
        rt =  sensitivityExp(exp.getIn(0), manager, plan, allInputs);
        break;

      case AGG_FUNC:
        Expression tmp = sensitivityExp(exp.getIn(0), manager, plan, allInputs);
        rt = ExpressionFactory.addSensitivity(exp, ExpSensitivityConvert.convertAggFunctions(tmp.getSensitivity(), exp.getI32()));
        break;
    }
    if (rt.getSensitivity() == ExpSensitivity.ERROR) {
      throw new RuntimeException(String.format("%s  Can't desensitize", exp));
    }
    return rt;
  }

  public static void sensitivityExps(SchemaManager manager, Plan plan) {
    ArrayList<Expression> expList = new ArrayList<>();
    for (Expression exp : plan.getSelectExps()) {
      Expression expression = ExpressionFactory.addSensitivity(exp, sensitivityExp(exp, manager, plan, null).getSensitivity());
      expList.add(expression);
    }
    plan.setSelectExps(expList);
    expList = new ArrayList<>();
    for (Expression exp : plan.getWhereExps()) {
      Expression expression = ExpressionFactory.addSensitivity(exp, sensitivityExp(exp, manager, plan, null).getSensitivity());
      expList.add(expression);
    }
    plan.setWhereExps(expList);
    expList = new ArrayList<>();
    for (Expression exp : plan.getAggExps()) {
      Expression expression = ExpressionFactory.addSensitivity(exp, sensitivityExp(exp, manager, plan, null).getSensitivity());
      expList.add(expression);
    }
    plan.setAggExps(expList);
  }


  public static void sensitivityExps(SchemaManager manager, Plan plan, List<Expression> allInputs) {
    ArrayList<Expression> expList = new ArrayList<>();
    for (Expression exp : plan.getSelectExps()) {
      Expression expression = ExpressionFactory.addSensitivity(exp, sensitivityExp(exp, manager, plan, allInputs).getSensitivity());
      expList.add(expression);
    }
    plan.setSelectExps(expList);
    expList = new ArrayList<>();
    for (Expression exp : plan.getWhereExps()) {
      Expression expression = ExpressionFactory.addSensitivity(exp, sensitivityExp(exp, manager, plan, allInputs).getSensitivity());
      expList.add(expression);
    }
    plan.setWhereExps(expList);
    expList = new ArrayList<>();
    for (Expression exp : plan.getAggExps()) {
      Expression expression = ExpressionFactory.addSensitivity(exp, sensitivityExp(exp, manager, plan, allInputs).getSensitivity());
      expList.add(expression);
    }
    plan.setAggExps(expList);
  }

  public static void checkSensitivity(Plan plan, SchemaManager manager) {
    switch (plan.getPlanType()) {
      case LEAF:
        sensitivityExps(manager, plan);
        break;
      case UNARY:
        checkSensitivity(plan.getChildren().get(0), manager);
        List<Expression> inputs = plan.getChildren().get(0).getOutExpressions();
        sensitivityExps(manager, plan, inputs);
        break;
      case BINARY:
        checkSensitivity(plan.getChildren().get(0), manager);
        checkSensitivity(plan.getChildren().get(1), manager);
        List<Expression> leftInputs = plan.getChildren().get(0).getOutExpressions();
        List<Expression> rightInputs = plan.getChildren().get(1).getOutExpressions();
        List<Expression> allInputs = new ArrayList<>();
        allInputs.addAll(leftInputs);
        allInputs.addAll(rightInputs);
        sensitivityExps(manager, plan, allInputs);
        checkJoinCond(plan.getJoinCond(), leftInputs, rightInputs);
        break;
      case EMPTY:
        break;
    }
  }
}
