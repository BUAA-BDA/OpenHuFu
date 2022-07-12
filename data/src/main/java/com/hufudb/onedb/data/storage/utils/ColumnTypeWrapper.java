package com.hufudb.onedb.data.storage.utils;

import com.google.common.collect.ImmutableMap;
import com.google.gson.annotations.SerializedName;
import com.hufudb.onedb.data.storage.Point;
import com.hufudb.onedb.proto.OneDBData.ColumnType;

/**
 * Wrapper for protocol buffer @OneDBData.ColumnType
 */
public enum ColumnTypeWrapper {
  @SerializedName("UNKOWN")
  UNKOWN(ColumnType.UNKNOWN),
  @SerializedName("BOOLEAN")
  BOOLEAN(ColumnType.BOOLEAN),
  @SerializedName("BYTE")
  BYTE(ColumnType.BYTE),
  @SerializedName("SHORT")
  SHORT(ColumnType.SHORT),
  @SerializedName("INT")
  INT(ColumnType.INT),
  @SerializedName("LONG")
  LONG(ColumnType.LONG),
  @SerializedName("FLOAT")
  FLOAT(ColumnType.FLOAT),
  @SerializedName("DOUBLE")
  DOUBLE(ColumnType.DOUBLE),
  @SerializedName("DATE")
  DATE(ColumnType.DATE),
  @SerializedName("TIME")
  TIME(ColumnType.TIME),
  @SerializedName("TIMESTAMP")
  TIMESTAMP(ColumnType.TIMESTAMP),
  @SerializedName("STRING")
  STRING(ColumnType.STRING),
  @SerializedName("BLOB")
  BLOB(ColumnType.BLOB),
  @SerializedName("POINT")
  POINT(ColumnType.POINT);

  private final static ImmutableMap<Integer, ColumnTypeWrapper> MAP;

  static {
    final ImmutableMap.Builder<Integer, ColumnTypeWrapper> builder = ImmutableMap.builder();
    for (ColumnTypeWrapper col : values()) {
      builder.put(col.getId(), col);
    }
    MAP = builder.build();
  }

  private final ColumnType type;

  ColumnTypeWrapper(ColumnType type) {
    this.type = type;
  }

  public int getId() {
    return type.getNumber();
  }

  public ColumnType get() {
    return type;
  }

  public static ColumnTypeWrapper of(ColumnType type) {
    return MAP.get(type.getNumber());
  }
}
