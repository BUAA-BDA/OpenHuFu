package group.bda.federate.sql.table;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import group.bda.federate.data.Header;
import group.bda.federate.client.FederateDBClient;

public class FederateTableInfo {
  private static final Logger LOG = LogManager.getLogger(FederateTableInfo.class);

  private String name;
  private Header header;
  private Map<String, Integer> columnMap;
  private Map<FederateDBClient, String> tableMap;
  private String geomAttribute;


  public FederateTableInfo(String name, Header header, Map<FederateDBClient, String> tableMap) {
    this.name = name;
    this.header = header;
    this.geomAttribute = header.getGeomFieldName();
    this.tableMap = tableMap;
    this.columnMap = new HashMap<>();
    for (int i = 0; i < header.size(); ++i) {
      columnMap.put(header.getName(i), i);
    }
  }

  public FederateTableInfo(String name, Header header) {
    this(name, header, new HashMap<>());
  }

  public FederateTableInfo(String globalName, Header header, FederateDBClient client, String localName) {
    this(globalName, header, new HashMap<>());
    this.tableMap.put(client, localName);
  }

  public void addFed(FederateDBClient client, String localName) {
    tableMap.put(client, localName);
  }

  public Header getHeader() {
    return header;
  }

  public Header generateHeader(List<String> columns) {
    Header.IteratorBuilder builder = Header.newBuilder();
    for (String column : columns) {
      Integer index = columnMap.get(column);
      if (index == null) {
        LOG.error("column {} not exist in table[{}]", column, name);
        break;
      }
      builder.add(column, header.getType(index));
    }
    return builder.build();
  }

  public String getName() {
    return name;
  }

  public Map<FederateDBClient, String> getTableMap() {
    return tableMap;
  }

  public String getLocalTableName(FederateDBClient client) {
    return tableMap.get(client);
  }

  public String getGeomAttribute() {
    return geomAttribute;
  }
}
