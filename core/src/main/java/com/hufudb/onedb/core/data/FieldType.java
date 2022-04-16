package com.hufudb.onedb.core.data;

import com.google.gson.annotations.SerializedName;

public enum FieldType {
  @SerializedName("UNKOWN")
  UNKOWN,
  @SerializedName("BOOLEAN")
  BOOLEAN,
  @SerializedName("BYTE")
  BYTE,
  @SerializedName("SHORT")
  SHORT,
  @SerializedName("INT")
  INT,
  @SerializedName("LONG")
  LONG,
  @SerializedName("FLOAT")
  FLOAT,
  @SerializedName("DOUBLE")
  DOUBLE,
  @SerializedName("DATE")
  DATE,
  @SerializedName("TIME")
  TIME,
  @SerializedName("TIMESTAMP")
  TIMESTAMP,
  @SerializedName("POINT")
  POINT,
  @SerializedName("STRING")
  STRING,
  @SerializedName("SARG")
  SARG;

  public static FieldType of(int id) {
    return values()[id];
  }
}
