package group.bda.federate.driver.utils;

import group.bda.federate.sql.type.FederateFieldType;

public class GeomesaTypeConverter {
  public static FederateFieldType convert(String typeName) {
    switch (typeName) {
      case "String":
        return FederateFieldType.STRING;
      case "Boolean":
        return FederateFieldType.BOOLEAN;
      case "Byte":
        return FederateFieldType.BYTE;
      case "Short":
        return FederateFieldType.SHORT;
      case "Integer": 
        return FederateFieldType.INT;
      case "Long":
        return FederateFieldType.LONG;
      case "Float":
        return FederateFieldType.FLOAT;
      case "Double":
        return FederateFieldType.DOUBLE;
      case "Date":
        return FederateFieldType.DATE;
      case "Timestamp":
      case "Time":
        return FederateFieldType.TIMESTAMP;
      case "Point":
        return FederateFieldType.POINT;
      default:
        return FederateFieldType.STRING;
    }
  }
}
