package com.hufudb.onedb.core.data.query.sort;

import org.apache.calcite.rel.RelFieldCollation;

public class OneDBOrder {
  public Direction direction;
  public int idx;

  public static OneDBOrder fromCollation(RelFieldCollation fieldCollation) {
    return new OneDBOrder(fieldCollation);
  }

  public static OneDBOrder parse(String oneDBOrder) {
    return new OneDBOrder(oneDBOrder);
  }


  public enum Direction {
    ASC,
    DESC
  }

  private OneDBOrder(String federateOrder) {
    String[] tmp = federateOrder.split(" ");
    this.idx = Integer.parseInt(tmp[0]);
    switch (tmp[1]) {
      case "ASC":
        this.direction = Direction.ASC;
        break;
      case "DESC":
        this.direction = Direction.DESC;
        break;
    }
  }

  private OneDBOrder(RelFieldCollation fieldCollation) {
    this.idx = fieldCollation.getFieldIndex();
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
        return idx + " ASC";
      case DESC:
        return idx + " DESC";
      default:
        return "";
    }
  }

}
