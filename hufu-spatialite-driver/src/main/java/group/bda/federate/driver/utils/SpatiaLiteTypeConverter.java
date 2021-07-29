package group.bda.federate.driver.utils;

import group.bda.federate.sql.type.FederateFieldType;

public class SpatiaLiteTypeConverter {
  public static FederateFieldType convert(String typeName) {
    switch (typeName) {
      case "POINT":
        return FederateFieldType.POINT;
      case "FLOAT":
      case "REAL":
        return FederateFieldType.FLOAT;
      case "DOUBLE":
        return FederateFieldType.DOUBLE;
      case "TINYINT":
        return FederateFieldType.BYTE;
      case "INT2":
      case "SMALLINT":
        return FederateFieldType.SHORT;
      case "INTEGER":
      case "INT":
        return FederateFieldType.INT;
      case "INT8":
      case "BIGINT":
        return FederateFieldType.LONG;
      case "VARCHAR":
      case "CHARACTER":
      case "TEXT":
        return FederateFieldType.STRING;
      case "BOOLEAN":
        return FederateFieldType.BOOLEAN;
      case "DATE":
        return FederateFieldType.DATE;
      default:
        return FederateFieldType.STRING;
    }
  }
}
