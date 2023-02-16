package com.hufudb.openhufu.benchmark.enums;

import java.util.HashSet;
import java.util.Set;

/**
 * @author yang.song
 * @date 2/15/23 1:45 AM
 */
public enum TPCHTableName {
  CUSTOMER("customer"),
  LINEITEM("lineitem"),
  NATION("nation"),
  ORDERS("orders"),
  PART("part"),
  PARTSUPP("partsupp"),
  REGION("region"),
  SUPPLIER("supplier");
  TPCHTableName(String name) {
    this.name = name;
  }
  private final String name;
  private static final Set<TPCHTableName> tableNameSet;

  static {
    tableNameSet = new HashSet<>();
    tableNameSet.add(CUSTOMER);
    tableNameSet.add(LINEITEM);
    tableNameSet.add(NATION);
    tableNameSet.add(ORDERS);
    tableNameSet.add(PART);
    tableNameSet.add(PARTSUPP);
    tableNameSet.add(REGION);
    tableNameSet.add(SUPPLIER);
  }

  public String getName() {
    return name;
  }

  public Set<TPCHTableName> getTPCHTableNames() {
    return tableNameSet;
  }
}
