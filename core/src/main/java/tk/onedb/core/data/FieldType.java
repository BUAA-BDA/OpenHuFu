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
  STRING;

  public static FieldType of(int id) {
    return values()[id];
  }
}
