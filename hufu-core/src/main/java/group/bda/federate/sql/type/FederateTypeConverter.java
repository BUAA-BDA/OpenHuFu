package group.bda.federate.sql.type;

import org.apache.calcite.sql.type.SqlTypeName;

public class FederateTypeConverter {
  public static SqlTypeName convert2SqlType(FederateFieldType type) {
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

  public static FederateFieldType convert2FederateType(SqlTypeName type) {
    switch (type) {
      case VARCHAR:
        return FederateFieldType.STRING;
      case BOOLEAN:
        return FederateFieldType.BOOLEAN;
      case TINYINT:
        return FederateFieldType.BYTE;
      case INTEGER:
        return FederateFieldType.INT;
      case SMALLINT:
        return FederateFieldType.SHORT;
      case BIGINT:
        return FederateFieldType.LONG;
      case FLOAT:
        return FederateFieldType.FLOAT;
      case DOUBLE:
      case DECIMAL:
        return FederateFieldType.DOUBLE;
      case DATE:
        return FederateFieldType.DATE;
      case TIME:
        return FederateFieldType.TIME;
      case TIMESTAMP:
        return FederateFieldType.TIMESTAMP;
      case GEOMETRY:
        return FederateFieldType.POINT;
      default:
        return FederateFieldType.STRING;
    }
  }
}
