package com.hufudb.onedb.core.data;

import java.util.List;
import com.google.common.collect.ImmutableMap;
import com.google.gson.annotations.SerializedName;
import com.hufudb.onedb.core.sql.expression.OneDBExpression;

/*
 * Privacy/Security level of column
 * when multiple columns are involved in a query,
 * the level with higher id will dominate the lower (except hidden)
 */
public enum Level {
  /*
   * Basic levels
   * id < 10
   * independent from a specific protocol/technique
   *
   */
  // columns marked as hide are invisible to client and other dataServers
  @SerializedName("hidden")
  HIDDEN("HIDDEN", 0),
  // public columns can be scanned directly
  @SerializedName("public")
  PUBLIC("PUBLIC", 1),
  // protected columns can be scanned but data source information will be hidden from client and other dataServers
  @SerializedName("protected")
  PROTECTED("PROTECTED", 2),
  // can't scan private columns directly, only statistics including COUNT, SUM, AVG can use on these columns
  @SerializedName("private")
  PRIVATE("PRIVATE", 3),

  /*
   * Advanced levels
   * id >= 10
   * use a specific protocol/technique
   */
  @SerializedName("gmw")
  GMW("GMW", 10);

  private final static ImmutableMap<Integer, Level> MAP;

  static {
    final ImmutableMap.Builder<Integer, Level> builder = ImmutableMap.builder();
    for (Level level : values()) {
      builder.put(level.id, level);
    }
    MAP = builder.build();
  }

  Level(String name, int id) {
    this.name = name;
    this.id = id;
  }

  private final String name;
  private final int id;

  public int getId() {
    return id;
  }

  public String getName() {
    return name;
  }

  public static Level of(int id) {
    return MAP.get(id);
  }

  public static Level dominate(Level a, Level b) {
    if (a.id > b.id) {
      return a;
    } else {
      return b;
    }
  }

  public static Level findDominator(List<OneDBExpression> exps) {
    Level d = PUBLIC;
    for (OneDBExpression exp : exps) {
      d = dominate(exp.getLevel(), d);
    }
    return d;
  }
}
