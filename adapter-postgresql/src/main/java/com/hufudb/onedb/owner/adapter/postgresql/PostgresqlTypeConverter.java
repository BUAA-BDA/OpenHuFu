package com.hufudb.onedb.owner.adapter.postgresql;

import com.hufudb.onedb.core.data.FieldType;
import com.hufudb.onedb.owner.adapter.AdapterTypeConverter;

public class PostgresqlTypeConverter implements AdapterTypeConverter {
  public FieldType convert(String typeName) {
    switch (typeName) {
      case "real":
      case "float4":
      return FieldType.FLOAT;
      case "float8":
      case "double precision":
      case "numeric":
      return FieldType.DOUBLE;
      case "TINYINT":
      return FieldType.BYTE;
      case "SMALLINT":
      return FieldType.SHORT;
      case "int2":
      case "int4":
      return FieldType.INT;
      case "oid":
      case "int8":
      return FieldType.LONG;
      case "varchar":
      case "char":
      case "bpchar":
      case "text":
      case "name":
      return FieldType.STRING;
      case "bit":
      case "bool":
      return FieldType.BOOLEAN;
      case "date":
      return FieldType.DATE;
      case "time":
      return FieldType.TIME;
      case "timestamp":
      return FieldType.TIMESTAMP;
      default:
      return FieldType.STRING;
    }
  }
}
