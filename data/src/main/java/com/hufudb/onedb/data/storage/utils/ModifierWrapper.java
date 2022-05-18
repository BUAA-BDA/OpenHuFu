package com.hufudb.onedb.data.storage.utils;

import java.util.List;
import com.google.common.collect.ImmutableMap;
import com.google.gson.annotations.SerializedName;
import com.hufudb.onedb.proto.OneDBData.Modifier;
import com.hufudb.onedb.proto.OneDBPlan.Expression;

/**
 * Wrapper for protocol buffer enum @OneDBData.Modifier
 */
public enum ModifierWrapper {
  // columns marked as hide are invisible to client and other dataServers
  @SerializedName("hidden")
  HIDDEN("HIDDEN", Modifier.HIDDEN, 0, 0),
  // public columns can be scanned directly
  @SerializedName("public")
  PUBLIC("PUBLIC", Modifier.PUBLIC, 0, 0),
  // protected columns can be scanned but data source information will be hidden from client and other dataServers
  @SerializedName("protected")
  PROTECTED("PROTECTED", Modifier.PROTECTED, 0, 200),
  // can't scan private columns directly, only statistics including COUNT, SUM, AVG can use on these 
  @SerializedName("private")
  PRIVATE("PRIVATE", Modifier.PRIVATE, 100, 200),

  /*
   * Advanced ModifierWrappers id >= 10
   * use a specific protocol/technique
   */
  @SerializedName("gmw")
  GMW("GMW", Modifier.UNSUPPORT, 30, 30);

  private final static ImmutableMap<Integer, ModifierWrapper> MAP;

  static {
    final ImmutableMap.Builder<Integer, ModifierWrapper> builder = ImmutableMap.builder();
    for (ModifierWrapper ModifierWrapper : values()) {
      builder.put(ModifierWrapper.getId(), ModifierWrapper);
    }
    MAP = builder.build();
  }

  ModifierWrapper(String name, Modifier modifier, int commonPtoId, int eqJoinPtoId) {
    this.name = name;
    this.modifier = modifier;
    this.commonPtoId = commonPtoId;
    this.eqJoinPtoId = eqJoinPtoId;
  }

  private final String name;
  private final Modifier modifier;
  private final int eqJoinPtoId;
  private final int commonPtoId;

  public int getId() {
    return modifier.getNumber();
  }

  public Modifier get() {
    return modifier;
  }

  public String getName() {
    return name;
  }

  public int getCommonPtoId() {
    return commonPtoId;
  }

  public int getEqJoinPtoId() {
    return eqJoinPtoId;
  }

  public static ModifierWrapper of(Modifier modifier) {
    return MAP.get(modifier.getNumber());
  }

  public static Modifier dominate(Modifier a, Modifier b) {
    if (a.getNumber() > b.getNumber()) {
      return a;
    } else {
      return b;
    }
  }

  public static Modifier dominate(List<Modifier> mods) {
    return mods.stream().reduce(Modifier.PUBLIC, (d, m) -> dominate(d, m));
  }

  public static Modifier deduceModifier(List<Expression> exps) {
    return exps.stream().reduce(Modifier.PUBLIC, (d, e) -> dominate(d, e.getModifier()),
        (m1, m2) -> dominate(m1, m2));
  }
}
