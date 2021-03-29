package tk.onedb.core.table;

import java.util.HashMap;
import java.util.Map;

import tk.onedb.core.client.DBClient;
import tk.onedb.core.data.Header;

public class OneDBTableInfo {
  private String name;
  private Header header;
  private Map<String, Integer> columnMap;
  private Map<DBClient, String> tableMap;

  public OneDBTableInfo(String name, Header header, Map<DBClient, String> tableMap) {
    this.name = name;
    this.header = header;
    this.tableMap = tableMap;
    this.columnMap = new HashMap<>();
    for (int i = 0; i < header.size(); ++i) {
      columnMap.put(header.getName(i), i);
    }
  }

  public OneDBTableInfo(String name, Header header) {
    this(name, header, new HashMap<>());
  }

  public OneDBTableInfo(String globalName, Header header, DBClient client, String localName) {
    this(globalName, header, new HashMap<>());
    this.tableMap.put(client, localName);
  }

  public void addDB(DBClient client, String localName) {
    tableMap.put(client, localName);
  }

  public Header getHeader() {
    return header;
  }

  public String getName() {
    return name;
  }

  public Map<DBClient, String> getTableMap() {
    return tableMap;
  }

  public String getLocalTableName(DBClient client) {
    return tableMap.get(client);
  }
}
