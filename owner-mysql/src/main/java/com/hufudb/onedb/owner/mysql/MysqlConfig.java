package com.hufudb.onedb.owner.mysql;

import com.hufudb.onedb.core.data.utils.POJOPublishedTableInfo;
import java.util.List;

public class MysqlConfig {
  public int port;
  public int threadnum;
  public String hostname;
  public String privatekeypath;
  public String certchainpath;
  public String url;
  public String catalog;
  public String user;
  public String passwd;
  public String zkservers;
  public String zkroot;
  public String digest;
  public List<POJOPublishedTableInfo> tables;
}
