package com.hufudb.onedb.owner.mysql;

import com.hufudb.onedb.core.data.FieldType;

public class MysqlTypeConverter {
  public static FieldType convert(String typeName) {
    switch (typeName) {
      case "FLOAT":
      case "REAL":
        return FieldType.FLOAT;
      case "DOUBLE":
        return FieldType.DOUBLE;
      case "TINYINT":
        return FieldType.BYTE;
      case "INT2":
      case "SMALLINT":
        return FieldType.SHORT;
      case "INTEGER":
      case "INT":
        return FieldType.INT;
      case "INT8":
      case "BIGINT":
        return FieldType.LONG;
      case "VARCHAR":
      case "CHARACTER":
      case "TEXT":
        return FieldType.STRING;
      case "BOOLEAN":
        return FieldType.BOOLEAN;
      case "DATE":
        return FieldType.DATE;
      case "TIME":
        return FieldType.TIME;
      case "TIMESTAMP":
        return FieldType.TIMESTAMP;
      default:
        return FieldType.STRING;
    }
  }
}
