package com.hufudb.onedb.owner.mysql;

import com.hufudb.onedb.core.data.utils.POJOPublishedTableInfo;
import java.util.List;

public class MysqlConfig {
  public int port;
  public String hostname;
  public String credspath;
  public String url;
  public String catalog;
  public String user;
  public String passwd;
  public String zkservers;
  public String zkroot;
  public String digest;
  public List<POJOPublishedTableInfo> tables;
}
