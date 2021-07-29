package group.bda.federate.driver.utils;

import group.bda.federate.sql.type.FederateFieldType;

public class PostgresqlTypeConverter {
  public static FederateFieldType convert(String typeName) {
    switch (typeName) {
      case "geometry":
      case "point":
        return FederateFieldType.POINT;
      case "real":
      case "float4":
        return FederateFieldType.FLOAT;
      case "float8":
        return FederateFieldType.DOUBLE;
      case "TINYINT":
        return FederateFieldType.BYTE;
      case "SMALLINT":
        return FederateFieldType.SHORT;
      case "int2":
      case "int4":
        return FederateFieldType.INT;
      case "oid":
      case "int8":
        return FederateFieldType.LONG;
      case "varchar":
      case "char":
      case "bpchar":
      case "text":
      case "name":
        return FederateFieldType.STRING;
      case "bit":
      case "bool":
        return FederateFieldType.BOOLEAN;
      case "date":
        return FederateFieldType.DATE;
      default:
        return FederateFieldType.STRING;
    }
  }
}
