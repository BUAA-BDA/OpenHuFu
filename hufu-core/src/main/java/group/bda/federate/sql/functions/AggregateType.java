package group.bda.federate.sql.functions;

import java.util.HashMap;
import java.util.Map;

import org.apache.calcite.rel.core.AggregateCall;

public enum AggregateType {
  COUNT("COUNT"),
  AVG("AVG"),
  MAX("MAX"),
  MIN("MIN"),
  SUM("SUM"),
  UNSUPPORT("UNSUPPORT");

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

  public static AggregateType of(AggregateCall call) {
    switch(call.getAggregation().getKind()) {
      case COUNT:
        return MAP.get("COUNT");
      case AVG:
        return MAP.get("AVG");
      case MAX:
        return MAP.get("MAX");
      case MIN:
        return MAP.get("MIN");
      case SUM:
      case SUM0:
        return MAP.get("SUM");
      default:
        return MAP.get("UNSUPPORT");
    }
  }
}
