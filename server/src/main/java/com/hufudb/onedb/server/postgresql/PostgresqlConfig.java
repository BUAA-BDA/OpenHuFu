package com.hufudb.onedb.server.postgresql;

import java.util.List;

import com.hufudb.onedb.core.data.utils.POJOPublishedTableInfo;

public class PostgresqlConfig {
  public int port;
  public String hostname;
  public String url;
  public String catalog;
  public String user;
  public String passwd;
  public String zkservers;
  public String zkroot;
  public String digest;
  public List<POJOPublishedTableInfo> tables;
}
