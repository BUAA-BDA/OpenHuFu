package group.bda.federate.sql.join;

import java.math.BigDecimal;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;

import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.validate.SqlUserDefinedFunction;

import group.bda.federate.sql.expression.FedSpatialExpressions;

public abstract class FedSpatialJoinInfo {
  private static final Set<SqlKind> COMPARISON = EnumSet.of(SqlKind.LESS_THAN, SqlKind.GREATER_THAN, SqlKind.GREATER_THAN_OR_EQUAL, SqlKind.LESS_THAN_OR_EQUAL);
  // for now just support KNN and distance join
  public static enum JoinType {
    KNN, DISTANCE;
  }

  // leftkey is public, rightkey is private/protected

  public abstract JoinType getType();

  public abstract int getLeftSize();

  public abstract int getRightSize();

  public abstract int getLeftKey();

  public abstract int getRightKey();

  public static boolean isSTDistance(RexCall call) {
    SqlUserDefinedFunction func = (SqlUserDefinedFunction) call.op;
    if (!func.getName().equals("Distance")) {
      return false;
    }
    List<RexNode> operands = call.getOperands();
    if (!(operands.get(0) instanceof RexInputRef && operands.get(1) instanceof RexInputRef)) {
      return false;
    }
    return true;
  }

  public static boolean isDWithin(RexCall call) {
    SqlUserDefinedFunction func = (SqlUserDefinedFunction) call.op;
    if (!func.getName().equals("DWithin")) {
      return false;
    }
    List<RexNode> operands = call.getOperands();
    if (!(operands.get(0) instanceof RexInputRef && operands.get(1) instanceof RexInputRef && operands.get(2) instanceof RexLiteral)) {
      return false;
    }
    return true;
  }

  public static boolean isKnn(RexCall call) {
    SqlUserDefinedFunction func = (SqlUserDefinedFunction) call.op;
    if (!func.getName().equals("KNN")) {
      return false;
    }
    List<RexNode> operands = call.getOperands();
    if (!(operands.get(0) instanceof RexInputRef && operands.get(1) instanceof RexInputRef && operands.get(2) instanceof RexLiteral)) {
      return false;
    }
    return true;
  }

  public static boolean support(RexNode condition) {
    if (!(condition instanceof RexCall)) {
      return false;
    }
    RexCall call = (RexCall) condition;
    SqlKind kind = condition.getKind();
    RexNode left = call.operands.get(0);
    RexNode right =  call.operands.get(1);
    if (COMPARISON.contains(condition.getKind())) {
      if (left.getKind().equals(SqlKind.OTHER_FUNCTION) && (kind.equals(SqlKind.LESS_THAN) || kind.equals(SqlKind.LESS_THAN_OR_EQUAL))) {
        if (left instanceof RexCall && right instanceof RexLiteral) {
          return isSTDistance((RexCall) left);
        } else {
          return false;
        }
      } else if (right.getKind().equals(SqlKind.OTHER_FUNCTION) && (kind.equals(SqlKind.GREATER_THAN) || kind.equals(SqlKind.GREATER_THAN_OR_EQUAL))) {
        if (left instanceof RexLiteral && right instanceof RexCall) {
          return isSTDistance((RexCall) right);
        } else {
          return false;
        }
      } else {
        return false;
      }
    } else if (kind.equals(SqlKind.OTHER_FUNCTION)) {
      if (isDWithin(call) || isKnn(call)) {
        return true;
      } else {
        return false;
      }
    }
    return false;
  }

  private static FedSpatialDistanceJoinInfo getInfoFromSTDWithin(RexCall call, FedSpatialExpressions leftExps, FedSpatialExpressions rightExps) {
    SqlUserDefinedFunction func = (SqlUserDefinedFunction) call.op;
    if (!func.getName().equals("DWithin")) {
      return null;
    }
    List<RexNode> operands = call.getOperands();
    if (!(operands.get(2) instanceof RexLiteral)) {
      return null;
    }
    if (!(operands.get(0) instanceof RexInputRef && operands.get(1) instanceof RexInputRef)) {
      return null;
    }
    int key1 = ((RexInputRef) operands.get(0)).getIndex();
    int key2 = ((RexInputRef) operands.get(1)).getIndex();
    int left = Math.min(key1, key2);
    int right = Math.max(key1, key2) - leftExps.size();
    double distance = ((BigDecimal) ((RexLiteral) operands.get(2)).getValue()).doubleValue();
    return new FedSpatialDistanceJoinInfo(left, right, leftExps, rightExps, distance, true);
  }

  private static FedSpatialDistanceJoinInfo getInfoFromSTDistance(RexCall call, RexLiteral distance, boolean equal, FedSpatialExpressions leftExps, FedSpatialExpressions rightExps) {
    if (!(distance instanceof RexLiteral)) {
      return null;
    }
    SqlUserDefinedFunction function = (SqlUserDefinedFunction) call.op;
    if (!function.getName().equals("Distance")) {
      return null;
    }
    List<RexNode> geos = call.getOperands();
    for (RexNode geo : geos) {
      if (!(geo instanceof RexInputRef)) {
        return null;
      }
    }
    List<RexNode> operands = call.getOperands();
    int key1 = ((RexInputRef) operands.get(0)).getIndex();
    int key2 = ((RexInputRef) operands.get(1)).getIndex();
    int left = Math.min(key1, key2);
    int right = Math.max(key1, key2) - leftExps.size();
    double dis = ((BigDecimal) distance.getValue()).doubleValue();
    return new FedSpatialDistanceJoinInfo(left, right, leftExps, rightExps, dis, equal);
  }

  public static FedSpatialKnnJoinInfo getKnn(RexCall call, FedSpatialExpressions leftExps, FedSpatialExpressions rightExps) {
    SqlUserDefinedFunction function = (SqlUserDefinedFunction) call.op;
    if (!function.getName().equals("KNN")) {
      return null;
    }
    List<RexNode> operands = call.getOperands();
    if (!(operands.get(2) instanceof RexLiteral)) {
      return null;
    }
    int key1 = ((RexInputRef) operands.get(0)).getIndex();
    int key2 = ((RexInputRef) operands.get(1)).getIndex();
    int left = Math.min(key1, key2);
    int right = Math.max(key1, key2) - leftExps.size();
    int k = ((BigDecimal) ((RexLiteral) operands.get(2)).getValue()).intValue();
    return new FedSpatialKnnJoinInfo(left, right, leftExps, rightExps, k);
  }

  public static FedSpatialJoinInfo generateJoinInfo(RexNode condition, FedSpatialExpressions leftExps, FedSpatialExpressions rightExps) {
    if (!(condition instanceof RexCall)) {
      return null;
    }
    RexCall call = (RexCall) condition;
    SqlKind kind = condition.getKind();
    RexNode left = call.operands.get(0);
    RexNode right =  call.operands.get(1);
    if (COMPARISON.contains(condition.getKind())) {
      if (left.getKind().equals(SqlKind.OTHER_FUNCTION) && (kind.equals(SqlKind.LESS_THAN) || kind.equals(SqlKind.LESS_THAN_OR_EQUAL))) {
        if (right instanceof RexLiteral) {
          return getInfoFromSTDistance((RexCall) left, (RexLiteral) right, kind.equals(SqlKind.LESS_THAN_OR_EQUAL), leftExps, rightExps);
        } else {
          return null;
        }
      } else if (right.getKind().equals(SqlKind.OTHER_FUNCTION) && (kind.equals(SqlKind.GREATER_THAN) || kind.equals(SqlKind.GREATER_THAN_OR_EQUAL))) {
        if (left instanceof RexLiteral) {
          return getInfoFromSTDistance((RexCall) right, (RexLiteral) left, kind.equals(SqlKind.GREATER_THAN_OR_EQUAL), leftExps, rightExps);
        } else {
          return null;
        }
      } else {
        return null;
      }
    } else if (kind.equals(SqlKind.OTHER_FUNCTION)) {
      FedSpatialJoinInfo join = getInfoFromSTDWithin(call, leftExps, rightExps);
      if (join != null) {
        return join;
      }
      join = getKnn(call, leftExps, rightExps);
      if (join != null) {
        return join;
      }
    }
    return null;
  }
}