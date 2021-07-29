package group.bda.federate.sql.expression;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableList;

import group.bda.federate.sql.plan.FedSpatialRelRowCount;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexProgram;
import org.apache.calcite.rex.RexSlot;

import group.bda.federate.data.Header;
import group.bda.federate.data.Level;
import group.bda.federate.rpc.FederateCommon.Func;
import group.bda.federate.sql.functions.AggregateType;
import group.bda.federate.sql.type.FederateFieldType;

public class FedSpatialExpressions implements Iterable<FedSpatialExpression> {
  List<FedSpatialExpression> exps;
  Header inputHeader;
  Map<AggregateType, List<Integer>> aggregateMap;

  private FedSpatialExpressions(Header inputHeader) {
    this.inputHeader = inputHeader;
    this.exps = new ArrayList<>();
    this.aggregateMap = new LinkedHashMap<>();
  }

  private FedSpatialExpressions(Header inputHeader, List<FedSpatialExpression> exps) {
    this.inputHeader = inputHeader;
    this.exps = exps;
    this.aggregateMap = new LinkedHashMap<>();
  }

  public static FedSpatialExpressions empty() {
    return new FedSpatialExpressions(Header.newBuilder().build());
  }

  public static FedSpatialExpressions create(Header inputHeader, List<RexNode> exps) {
    return new FedSpatialExpressions(inputHeader, FedSpatialExpression.create(inputHeader, exps));
  }

  public static FedSpatialExpressions create(Header inputHeader, RexProgram program) {
    List<FedSpatialExpression> exps = FedSpatialExpression.create(inputHeader,
        program.getProjectList().stream().map(RexSlot::getIndex).collect(Collectors.toList()), program.getExprList());
    return new FedSpatialExpressions(inputHeader, exps);
  }

  public static FedSpatialExpressions create(Header inputHeader) {
    FedSpatialExpressions exps = new FedSpatialExpressions(inputHeader);
    List<FedSpatialExpression> exp = FedSpatialExpression.createInputRef(inputHeader);
    exps.exps.addAll(exp);
    return exps;
  }

  public void appendAggregate(List<AggregateCall> calls) {
    List<FedSpatialExpression> tmp = exps;
    exps = new ArrayList<>();
    for (AggregateCall call : calls) {
      List<Integer> args = call.getArgList();
      Func func = FedSpatialExpression.IRBuilder.getAggFunc(call.getAggregation().getKind());
      int outType = FedSpatialExpression.IRBuilder.sqlType2FedTypeId(call.getType().getSqlTypeName());
      AggregateType aggType = AggregateType.of(call);
      if (args.size() == 0) {
        exps.add(FedSpatialExpression.createAggregate(inputHeader, call));
        aggregateMap.put(aggType, ImmutableList.of(exps.size() - 1));
      } else if (args.size() == 1) {
        final int idx = args.get(0);
        FedSpatialExpression exp = tmp.get(idx).copy();
        Level level = exp.getLevel();
        if (!level.equals(Level.PUBLIC)) {
          if (level.equals(Level.PRIVATE) && (aggType.equals(AggregateType.COUNT)
              || aggType.equals(AggregateType.SUM) || aggType.equals(AggregateType.AVG))) {
            level = Level.PROTECTED;
          }
        }
        if (aggType.equals(AggregateType.AVG)) {
          FedSpatialExpression sumExp = exp.copy();
          sumExp.appendAggregate(Func.kSum, outType, level.ordinal());
          exps.add(sumExp);
          exp.appendAggregate(Func.kCount, FederateFieldType.LONG.ordinal(), level.ordinal());
          exps.add(exp);
          aggregateMap.put(aggType, ImmutableList.of(exps.size() - 2, exps.size() - 1));
        } else {
          exp.appendAggregate(func, outType, level.ordinal());
          exps.add(exp);
          aggregateMap.put(aggType, ImmutableList.of(exps.size() - 1));
        }
      } else {
        throw new RuntimeException("Unknown aggregate func " + call.getName());
      }
    }
  }

  public int size() {
    return exps.size();
  }

  public FedSpatialExpression get(int index) {
    return exps.get(index);
  }

  public List<String> getExpStrings() {
    List<String> result = new ArrayList<>();
    for (FedSpatialExpression exp : exps) {
      result.add(exp.toProto().toString());
    }
    return result;
  }

  public Map<AggregateType, List<Integer>> getAggregateMap() {
    return aggregateMap;
  }

  @Override
  public Iterator<FedSpatialExpression> iterator() {
    return exps.iterator();
  }
}
