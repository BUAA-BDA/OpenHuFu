package group.bda.federate.driver.table;

import java.util.HashMap;
import java.util.Map;

import group.bda.federate.data.Header;
import group.bda.federate.data.Level;
import group.bda.federate.sql.type.FederateFieldType;

public class ServerTableInfo {
  private final String name;
  private final Header header;

  private final Map<String, Integer> columnIndex;
  private final Header pubHeader;

  private ServerTableInfo(String name, Header header, Map<String, Integer> columnIndex) {
    this.name = name;
    this.header = header;
    this.columnIndex = columnIndex;
    Header.IteratorBuilder builder = Header.newBuilder();
    final int size = header.size();
    for (int i = 0; i < size; ++i) {
      if (header.getLevel(i).equals(Level.PUBLIC)) {
        builder.add(header.getName(i), header.getType(i));
      }
    }
    this.pubHeader = builder.build();
  }

  public static Builder newBuilder(int size) {
    return new Builder(size);
  }

  public static IteratorBuilder newBuilder() {
    return new IteratorBuilder();
  }

  public String getTableName() {
    return name;
  }

  public Header getHeader() {
    return header;
  }

  public String[] getPubAttributes() {
    return pubHeader.getNames();
  }

  public Integer getColumnIndex(String name) {
    return columnIndex.get(name);
  }

  private Header generateHeader(int[] ids) {
    Header.Builder builder = Header.newBuilder(ids.length);
    for (int i = 0; i < ids.length; ++i) {
      builder.set(i, this.header.getName(ids[i]), this.header.getType(ids[i]));
    }
    return builder.build();
  }

  public Header generateHeader(String[] columns) {
    if (columns == null) {
      return pubHeader;
    }
    int[] ids = new int[columns.length];
    if (checkAndConvert(columns, ids)) {
      return generateHeader(ids);
    }
    return null;
  }

  public boolean isPublic(int index) {
    return header.isPublic(index);
  }

  public boolean check(String[] names) {
    for (String s : names) {
      Integer idx = getColumnIndex(s);
      if (idx == null) {
        return false;
      }
      if (!isPublic(idx)) {
        return false;
      }
    }
    return true;
  }

  private boolean checkAndConvert(String[] names, int[] ids) {
    for (int i = 0; i < names.length; ++i) {
      Integer idx = getColumnIndex(names[i]);
      if (idx == null) {
        return false;
      }
      if (!isPublic(idx)) {
        return false;
      }
      ids[i] = idx;
    }
    return true;
  }

  @Override
  public String toString() {
    return String.format("table [%s] : %s", name, header.toString());
  }

  public static class Builder {
    private String tableName;
    private final Header.Builder builder;
    private String geomAttribute;

    private final Map<String, Integer> columnIndex;

    private Builder(int size) {
      this.builder = Header.newBuilder(size);
      columnIndex = new HashMap<>();
    }

    public void setTableName(String tableName) {
      this.tableName = tableName;
    }

    public void set(int index, String name, FederateFieldType type, Level level) {
      if (type.equals(FederateFieldType.POINT)) {
        geomAttribute = name;
      }
      builder.set(index, name, type, level);
      columnIndex.put(name, index);
    }

    public void set(int index, String name, FederateFieldType type) {
      if (type.equals(FederateFieldType.POINT)) {
        geomAttribute = name;
      }
      set(index, name, type, Level.PUBLIC);
    }

    public ServerTableInfo build() throws Exception {
      if (geomAttribute == null) {
        throw new Exception("table has no geom column");
      }
      return new ServerTableInfo(tableName, builder.build(), columnIndex);
    }
  }

  public static class IteratorBuilder {
    private String tableName;
    private final Header.IteratorBuilder builder;
    private final Map<String, Integer> columnIndex;

    private IteratorBuilder() {
      this.builder = Header.newBuilder();
      columnIndex = new HashMap<>();
    }

    public void setTableName(String tableName) {
      this.tableName = tableName;
    }

    public void add(String name, FederateFieldType type, Level level) {
      builder.add(name, type, level);
    }

    public void add(String name, FederateFieldType type) {
      add(name, type, Level.PUBLIC);
    }

    public ServerTableInfo build() throws Exception {
      return new ServerTableInfo(tableName, builder.build(), columnIndex);
    }
  }
}
