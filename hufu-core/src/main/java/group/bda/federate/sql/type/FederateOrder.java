package group.bda.federate.sql.type;

import org.apache.calcite.rel.RelFieldCollation;

public class FederateOrder {

  public Direction direction;
  public int idx;

  public static FederateOrder fromCollation(RelFieldCollation fieldCollation) {
    return new FederateOrder(fieldCollation);
  }

  public static FederateOrder parse(String federateOrder) {
    return new FederateOrder(federateOrder);
  }


  public enum Direction {
    ASC,
    DESC
  }

  private FederateOrder(String federateOrder) {
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

  private FederateOrder(RelFieldCollation fieldCollation) {
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
