package com.hufudb.openhufu.core.data;

import java.sql.Types;
import com.hufudb.openhufu.proto.OpenHuFuData.ColumnType;
import org.apache.calcite.sql.type.SqlTypeName;

public class TypeConverter {
  public static SqlTypeName convert2SqlType(ColumnType type) {
    switch (type) {
      case STRING:
        return SqlTypeName.VARCHAR;
      case BOOLEAN:
        return SqlTypeName.BOOLEAN;
      case BYTE:
        return SqlTypeName.TINYINT;
      case SHORT:
        return SqlTypeName.SMALLINT;
      case INT:
        return SqlTypeName.INTEGER;
      case LONG:
        return SqlTypeName.BIGINT;
      case FLOAT:
        return SqlTypeName.FLOAT;
      case DOUBLE:
        return SqlTypeName.DOUBLE;
      case DATE:
        return SqlTypeName.DATE;
      case TIME:
        return SqlTypeName.TIME;
      case TIMESTAMP:
        return SqlTypeName.TIMESTAMP;
      default:
        return SqlTypeName.ANY;
    }
  }

  public static ColumnType convert2OpenHuFuType(SqlTypeName type) {
    switch (type) {
      case VARCHAR:
      case CHAR:
        return ColumnType.STRING;
      case BOOLEAN:
        return ColumnType.BOOLEAN;
      case TINYINT:
        return ColumnType.BYTE;
      case INTEGER:
        return ColumnType.INT;
      case SMALLINT:
        return ColumnType.SHORT;
      case BIGINT:
        return ColumnType.LONG;
      case FLOAT:
        return ColumnType.FLOAT;
      case DOUBLE:
      case DECIMAL:
        return ColumnType.DOUBLE;
      case DATE:
        return ColumnType.DATE;
      case TIME:
        return ColumnType.TIME;
      case TIMESTAMP:
        return ColumnType.TIMESTAMP;
      case INTERVAL_YEAR:
      case INTERVAL_YEAR_MONTH:
      case INTERVAL_MONTH:
      case INTERVAL_DAY:
      case INTERVAL_DAY_MINUTE:
      case INTERVAL_DAY_SECOND:
      case INTERVAL_HOUR:
      case INTERVAL_HOUR_MINUTE:
      case INTERVAL_HOUR_SECOND:
      case INTERVAL_MINUTE:
      case INTERVAL_MINUTE_SECOND:
      case INTERVAL_SECOND:
        return ColumnType.INTERVAL;
      default:
        throw new UnsupportedOperationException("Unsupported type " + type.getName());
    }
  }

  public static ColumnType convert2OpenHuFuType(int sqlType) {
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
        throw new UnsupportedOperationException("Unsupported type");
    }
  }
}
