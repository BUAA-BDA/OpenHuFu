package com.hufudb.onedb.core.data;

import java.sql.Types;

import org.apache.calcite.sql.type.ExtraSqlTypes;
import org.apache.calcite.sql.type.SqlTypeName;

public class TypeConverter {
  public static SqlTypeName convert2SqlType(FieldType type) {
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
      case POINT:
        return SqlTypeName.GEOMETRY;
      default:
        return SqlTypeName.ANY;
    }
  }

  public static FieldType convert2OneDBType(SqlTypeName type) {
    switch (type) {
      case VARCHAR:
        return FieldType.STRING;
      case BOOLEAN:
        return FieldType.BOOLEAN;
      case TINYINT:
        return FieldType.BYTE;
      case INTEGER:
        return FieldType.INT;
      case SMALLINT:
        return FieldType.SHORT;
      case BIGINT:
        return FieldType.LONG;
      case FLOAT:
        return FieldType.FLOAT;
      case DOUBLE:
      case DECIMAL:
        return FieldType.DOUBLE;
      case DATE:
        return FieldType.DATE;
      case TIME:
        return FieldType.TIME;
      case TIMESTAMP:
        return FieldType.TIMESTAMP;
      case GEOMETRY:
        return FieldType.POINT;
      default:
        return FieldType.STRING;
    }
  }

  public static FieldType convert2OneDBType(int sqlType) {
    switch (sqlType) {
      case Types.VARCHAR:
        return FieldType.STRING;
      case Types.BOOLEAN:
        return FieldType.BOOLEAN;
      case Types.TINYINT:
        return FieldType.BYTE;
      case Types.INTEGER:
        return FieldType.INT;
      case Types.SMALLINT:
        return FieldType.SHORT;
      case Types.BIGINT:
        return FieldType.LONG;
      case Types.FLOAT:
        return FieldType.FLOAT;
      case Types.DOUBLE:
      case Types.DECIMAL:
        return FieldType.DOUBLE;
      case Types.DATE:
        return FieldType.DATE;
      case Types.TIME:
        return FieldType.TIME;
      case Types.TIMESTAMP:
        return FieldType.TIMESTAMP;
      case ExtraSqlTypes.GEOMETRY:
        return FieldType.POINT;
      default:
        return FieldType.STRING;
    }
  }
}
