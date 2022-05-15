package com.hufudb.onedb.data.schema.utils;

import java.sql.Types;
import com.hufudb.onedb.proto.OneDBData.ColumnType;

public class TypeConverter {
  private TypeConverter() {}

  public static ColumnType convert2OneDBType(int sqlType) {
    switch (sqlType) {
      case Types.VARCHAR:
        return ColumnType.STRING;
      case Types.BOOLEAN:
        return ColumnType.BOOLEAN;
      case Types.TINYINT:
        return ColumnType.BYTE;
      case Types.INTEGER:
        return ColumnType.INT;
      case Types.SMALLINT:
        return ColumnType.SHORT;
      case Types.BIGINT:
        return ColumnType.LONG;
      case Types.FLOAT:
        return ColumnType.FLOAT;
      case Types.DOUBLE:
      case Types.DECIMAL:
        return ColumnType.DOUBLE;
      case Types.DATE:
        return ColumnType.DATE;
      case Types.TIME:
        return ColumnType.TIME;
      case Types.TIMESTAMP:
        return ColumnType.TIMESTAMP;
      default:
        return ColumnType.STRING;
    }
  }
}
