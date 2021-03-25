package tk.onedb.core.data;

public enum FieldType {
  BOOLEAN,
  BYTE,
  SHORT,
  INT,
  LONG,
  FLOAT,
  DOUBLE,
  DATE,
  TIME,
  TIMESTAMP,
  POINT,
  STRING,
  INPUT_REF, // input reference in expression
  OP; // operator in expression

  public static FieldType of(int id) {
    return values()[id];
  }
}
