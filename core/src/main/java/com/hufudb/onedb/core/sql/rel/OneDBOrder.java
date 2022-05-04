package com.hufudb.onedb.core.sql.rel;

import java.util.List;
import java.util.stream.Collectors;
import com.hufudb.onedb.rpc.OneDBCommon.CollationProto;
import org.apache.calcite.rel.RelFieldCollation;

public class OneDBOrder {
  public Direction direction;
  public int inputRef;

  public static OneDBOrder fromCollation(RelFieldCollation fieldCollation) {
    return new OneDBOrder(fieldCollation);
  }

  public enum Direction {
    ASC, DESC
  }

  public OneDBOrder(Direction direction, int inputRef) {
    this.direction = direction;
    this.inputRef = inputRef;
  }

  private OneDBOrder(RelFieldCollation fieldCollation) {
    this.inputRef = fieldCollation.getFieldIndex();
    if (fieldCollation.direction == RelFieldCollation.Direction.DESCENDING) {
      this.direction = Direction.DESC;
    } else {
      this.direction = Direction.ASC;
    }
  }

  @Override
  public String toString() {
    switch (this.direction) {
      case ASC:
        return inputRef + " ASC";
      case DESC:
        return inputRef + " DESC";
      default:
        return "";
    }
  }

  public CollationProto toProto() {
    return CollationProto.newBuilder().setDirection(direction.ordinal()).setRef(inputRef).build();
  }

  public static List<CollationProto> toProto(List<OneDBOrder> orders) {
    return orders.stream().map(order -> order.toProto()).collect(Collectors.toList());
  }

  public static OneDBOrder fromProto(CollationProto proto) {
    return new OneDBOrder(Direction.values()[proto.getDirection()], proto.getRef());
  }

  public static List<OneDBOrder> fromProto(List<CollationProto> proto) {
    return proto.stream().map(c -> OneDBOrder.fromProto(c)).collect(Collectors.toList());
  }
}
