package group.bda.federate.driver.config;

import java.util.List;

import group.bda.federate.data.Level;

public interface ServerConfig {

  abstract public Table getTable(String tableName);

  public static class Table {
    public String name;
    public List<Column> columns;

    public Level getLevel(String columnName) {
      for (Column column : columns) {
        if (columnName.equals(column.name)) {
          return column.level;
        }
      }
      return Level.PUBLIC;
    }
  }

  public static class Column {
    public String name;
    public Level level;
  }
}
