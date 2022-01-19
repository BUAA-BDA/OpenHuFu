package com.hufudb.onedb.core.sql.expression;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.calcite.rel.core.AggregateCall;

import com.hufudb.onedb.core.data.FieldType;
import com.hufudb.onedb.core.data.TypeConverter;
import com.hufudb.onedb.rpc.OneDBCommon.ExpressionProto;

public class OneDBAggCall implements OneDBExpression {
  public enum AggregateType {
    COUNT("COUNT"), AVG("AVG"), MAX("MAX"), MIN("MIN"), SUM("SUM"), UNSUPPORT("UNSUPPORT");

    private final String name;
    private static final Map<String, AggregateType> MAP = new HashMap<>();
    static {
      for (AggregateType type : values()) {
        MAP.put(type.name, type);
      }
    }

    AggregateType(String name) {
      this.name = name;
    }

    public static AggregateType of(String typeString) {
      return MAP.get(typeString);
    }

    public static AggregateType of(int id) {
      return values()[id];
    }

    public static AggregateType of(AggregateCall call) {
      switch (call.getAggregation().getKind()) {
      case COUNT:
        return MAP.get("COUNT");
      case AVG:
        return MAP.get("AVG");
      case MAX:
        return MAP.get("MAX");
      case MIN:
        return MAP.get("MIN");
      case SUM:
        return MAP.get("SUM");
      default:
        return MAP.get("UNSUPPORT");
      }
    }
  }

  AggregateType aggType;
  List<Integer> in;
  FieldType outType;

  @Override
  public ExpressionProto toProto() {
    // todo: fix fromIndex type
    return ExpressionProto.newBuilder().setOpType(OneDBOpType.AGG_FUNC.ordinal()).setFunc(aggType.ordinal())
        .setOutType(outType.ordinal())
        .addAllIn(in.stream().map(i -> OneDBReference.fromIndex(outType, i).toProto()).collect(Collectors.toList()))
        .build();
  }

  OneDBAggCall(AggregateType aggType, List<Integer> args, FieldType type) {
    this.aggType = aggType;
    this.in = args;
    this.outType = type;
  }

  public static List<OneDBExpression> fromAggregates(List<AggregateCall> calls) {
    List<OneDBExpression> result = new ArrayList<>();
    calls.stream().forEach(call -> {
      AggregateType aggType = AggregateType.of(call);
      FieldType outType = TypeConverter.convert2OneDBType(call.getType().getSqlTypeName());
      if (aggType.equals(AggregateType.AVG)) {
        result.add(new OneDBAggCall(AggregateType.COUNT, call.getArgList(), FieldType.LONG));
        result.add(new OneDBAggCall(AggregateType.SUM, call.getArgList(), outType));
      } else {
        result.add(new OneDBAggCall(aggType, call.getArgList(), outType));
      }
    });
    return result;
  }

  public static OneDBAggCall fromProto(ExpressionProto proto) {
    if (!OneDBOpType.of(proto.getOpType()).equals(OneDBOpType.AGG_FUNC)) {
      throw new RuntimeException("not aggregate");
    }
    List<Integer> inputs = proto.getInList().stream().map(in -> in.getRef()).collect(Collectors.toList());
    return new OneDBAggCall(AggregateType.of(proto.getFunc()), inputs, FieldType.of(proto.getOutType()));
  }

  @Override
  public FieldType getOutType() {
    return outType;
  }

  @Override
  public OneDBOpType getOpType() {
    return OneDBOpType.AGG_FUNC;
  }
}