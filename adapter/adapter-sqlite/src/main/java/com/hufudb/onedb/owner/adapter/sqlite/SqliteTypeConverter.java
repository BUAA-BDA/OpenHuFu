package com.hufudb.onedb.owner.adapter.sqlite;

import com.hufudb.onedb.owner.adapter.AdapterTypeConverter;
import com.hufudb.onedb.proto.OneDBData.ColumnType;

public class SqliteTypeConverter implements AdapterTypeConverter {
  public ColumnType convert(String typeName) {
    switch (typeName) {
      case "FLOAT":
      case "REAL":
        return ColumnType.FLOAT;
      case "DOUBLE":
        return ColumnType.DOUBLE;
      case "TINYINT":
        return ColumnType.BYTE;
      case "INT2":
      case "SMALLINT":
        return ColumnType.SHORT;
      case "INTEGER":
      case "INT":
        return ColumnType.INT;
      case "INT8":
      case "BIGINT":
        return ColumnType.LONG;
      case "VARCHAR":
      case "CHARACTER":
      case "TEXT":
        return ColumnType.STRING;
      case "BOOLEAN":
        return ColumnType.BOOLEAN;
      case "DATE":
        return ColumnType.DATE;
      case "TIME":
        return ColumnType.TIME;
      case "TIMESTAMP":
        return ColumnType.TIMESTAMP;
      default:
        return ColumnType.STRING;
    }
  }
}
