package com.hufudb.openhufu.owner.checker;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableList;
import com.hufudb.openhufu.data.schema.SchemaManager;
import com.hufudb.openhufu.expression.AggFuncType;
import com.hufudb.openhufu.plan.Plan;
import com.hufudb.openhufu.proto.OpenHuFuData.Modifier;
import com.hufudb.openhufu.proto.OpenHuFuPlan.Expression;
import com.hufudb.openhufu.proto.OpenHuFuPlan.OperatorType;
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
}
