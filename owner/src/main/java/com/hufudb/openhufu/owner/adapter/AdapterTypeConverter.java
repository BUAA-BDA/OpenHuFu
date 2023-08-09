package com.hufudb.openhufu.owner.adapter;

import com.hufudb.openhufu.proto.OpenHuFuData.ColumnType;

import java.sql.Types;

public abstract class AdapterTypeConverter {
  public ColumnType convert(int type, String typeName) {
    switch (type) {
      case Types.BOOLEAN:
        return ColumnType.BOOLEAN;
      case Types.INTEGER:
        return ColumnType.INT;
      case Types.BIGINT:
        return ColumnType.LONG;
      case Types.FLOAT:
        return ColumnType.FLOAT;
      case Types.DOUBLE:
      case Types.NUMERIC:
        return ColumnType.DOUBLE;
      case Types.BLOB:
        return ColumnType.BLOB;
      case Types.CHAR:
      case Types.VARCHAR:
      default:
        return ColumnType.STRING;
    }
  }
}
