package tk.onedb.server.data;

import java.util.HashMap;
import java.util.Map;

import tk.onedb.core.data.FieldType;
import tk.onedb.core.data.Header;
import tk.onedb.core.data.Level;

public class TableInfo {
  private final String name;
  private final Header header;

  private final Map<String, Integer> columnIndex;

  private TableInfo(String name, Header header, Map<String, Integer> columnIndex) {
    this.name = name;
    this.header = header;
    this.columnIndex = columnIndex;
  }

  public static class Builder {
    private String tableName;
    private final Header.Builder builder;
    private final Map<String, Integer> columnIndex;

    private Builder() {
      this.builder = Header.newBuilder();
      columnIndex = new HashMap<>();
    }

    public void setTableName(String tableName) {
      this.tableName = tableName;
    }

    public void add(String name, FieldType type) {
      columnIndex.put(name, builder.size());
      builder.add(name, type);
    }

    public void add(String name, FieldType type, Level level) {
      columnIndex.put(name, builder.size());
      builder.add(name, type, level);
    }

    public TableInfo build() throws Exception {
      return new TableInfo(tableName, builder.build(), columnIndex);
    }
  }

  public static Builder newBuilder() {
    return new Builder();
  }


  public String getName() {
    return name;
  }


  public Header getHeader() {
    return header;
  }

  public Integer getColumnIndex(String name) {
    return columnIndex.get(name);
  }

  @Override
  public String toString() {
    return String.format("table [%s] : %s", name, header.toString());
  }
}
