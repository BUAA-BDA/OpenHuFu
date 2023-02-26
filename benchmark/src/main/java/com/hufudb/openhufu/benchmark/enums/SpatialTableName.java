package com.hufudb.openhufu.benchmark.enums;

import java.util.HashSet;
import java.util.Set;

/**
 * @author yang.song
 * @date 2/15/23 1:45 AM
 */
public enum SpatialTableName {
  SPATIAL("spatial");
  SpatialTableName(String name) {
    this.name = name;
  }
  private final String name;
  private static final Set<SpatialTableName> tableNameSet;

  static {
    tableNameSet = new HashSet<>();
    tableNameSet.add(SPATIAL);
  }

  public String getName() {
    return name;
  }

  public Set<SpatialTableName> getSpatialTableNames() {
    return tableNameSet;
  }
}
