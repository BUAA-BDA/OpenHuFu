package tk.onedb.server.postgresql;

import java.util.List;

import tk.onedb.server.data.ServerConfig;


public class PostgresqlConfig implements ServerConfig {
  public int port;
  public String hostname;
  public String url;
  public String catalog;
  public String user;
  public String passwd;
  public List<Table> tables;
  public String zkservers;
  public String zkroot;
  public String digest;

  @Override
  public Table getTable(String tableName) {
    for (Table table : tables) {
      if (tableName.equals(table.name)) {
        return table;
      }
    }
    return null;
  }
}
