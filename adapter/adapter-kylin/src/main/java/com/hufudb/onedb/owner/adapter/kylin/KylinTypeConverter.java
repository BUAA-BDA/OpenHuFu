package com.hufudb.onedb.owner.adapter.kylin;

import com.hufudb.onedb.owner.adapter.AdapterTypeConverter;
import com.hufudb.onedb.proto.OneDBData.ColumnType;

public class KylinTypeConverter extends AdapterTypeConverter {

  // reference https://kylin.apache.org/cn/docs31/tutorial/sql_reference.html#datatype
  @Override
  public ColumnType convert(int type, String typeName) {
    switch (typeName) {
      case "FLOAT":
      case "REAL":
        return ColumnType.FLOAT;
      case "DOUBLE":
      case "DECIMAL":
      case "NUMERIC":
        return ColumnType.DOUBLE;
      case "BYTE":
      case "TINYINT":
        return ColumnType.BYTE;
      case "SMALLINT":
      case "SHORT":
        return ColumnType.SHORT;
      case "INT":
      case "INTEGER":
        return ColumnType.INT;
      case "BIGINT":
      case "LONG":
        return ColumnType.LONG;
      case "BOOLEAN":
        return ColumnType.BOOLEAN;
      case "DATE":
      case "DATETIME":
        return ColumnType.DATE;
      case "TIME":
        return ColumnType.TIME;
      case "TIMESTAMP":
        return ColumnType.TIMESTAMP;
      case "BINARY":
        return ColumnType.BLOB;
      case "CHAR":
      case "VARCHAR":
      case "STRING":
      case "ANY":
      default:
        return ColumnType.STRING;
    }
  }
}
