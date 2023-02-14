package com.hufudb.openhufu.common.enums;

/**
 * @author yang.song
 * @date 2/14/23 2:57 PM
 */
public enum DataSourceType {

  CSV("csv");

  DataSourceType(String type) {
    this.type = type;
  }

  String type;
}
