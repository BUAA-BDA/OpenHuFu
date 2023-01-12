package com.hufudb.onedb.owner.adapter.hive;

import com.hufudb.onedb.owner.adapter.AdapterTypeConverter;
import com.hufudb.onedb.proto.OneDBData.ColumnType;

public class HiveTypeConverter extends AdapterTypeConverter {

  @Override
  public ColumnType convert(int type, String typeName) {
    switch (typeName) {
      case "FLOAT":
        return ColumnType.FLOAT;
      case "DOUBLE":
      case "DECIMAL":
        return ColumnType.DOUBLE;
      case "TINYINT":
        return ColumnType.BYTE;
      case "SMALLINT":
        return ColumnType.SHORT;
      case "INTEGER":
      case "INT":
        return ColumnType.INT;
      case "BIGINT":
        return ColumnType.LONG;
      case "VARCHAR":
      case "CHAR":
      case "STRING":
        return ColumnType.STRING;
      case "BOOLEAN":
        return ColumnType.BOOLEAN;
      case "DATE":
        return ColumnType.DATE;
      case "TIMESTAMP":
        return ColumnType.TIMESTAMP;
      default:
        return ColumnType.STRING;
    }
  }
}
