package group.bda.federate.sql.type;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.google.common.collect.ImmutableList;

import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.linq4j.tree.Primitive;
import org.apache.calcite.rel.type.RelDataType;

public enum FederateFieldType {
  STRING(String.class, "string"), BOOLEAN(Primitive.BOOLEAN), BYTE(Primitive.BYTE), SHORT(Primitive.SHORT),
  INT(Primitive.INT), LONG(Primitive.LONG), FLOAT(Primitive.FLOAT), DOUBLE(Primitive.DOUBLE),
  DATE(java.sql.Date.class, "date"), TIME(java.sql.Time.class, "time"),
  TIMESTAMP(java.sql.Timestamp.class, "timestamp"), POINT(Point.class, "point");

  private final Class clazz;
  private final String simpleName;

  private static final Map<String, FederateFieldType> MAP = new HashMap<>();

  static {
    for (FederateFieldType value : values()) {
      MAP.put(value.simpleName, value);
    }
  }

  FederateFieldType(Primitive primitive) {
    this(primitive.boxClass, primitive.primitiveName);
  }

  FederateFieldType(Class clazz, String simpleName) {
    this.clazz = clazz;
    this.simpleName = simpleName;
  }

  public RelDataType toType(JavaTypeFactory typeFactory) {
    RelDataType javaType = typeFactory.createJavaType(clazz);
    RelDataType sqlType = typeFactory.createSqlType(javaType.getSqlTypeName());
    return typeFactory.createTypeWithNullability(sqlType, true);
  }

  public static FederateFieldType of(String typeString) {
    return MAP.get(typeString);
  }

  public static final List<FederateFieldType> INTEGER = ImmutableList.of(BYTE, SHORT, INT, LONG);

  public static final List<FederateFieldType> REAL = ImmutableList.of(FLOAT, DOUBLE);

}