package com.hufudb.onedb.owner.data;

import com.hufudb.onedb.core.data.Level;
import java.util.List;

public interface ServerConfig {

  Table getTable(String tableName);

  class Table {
    public String name;
    public List<Column> columns;
    public List<Mapping> mappings;

    public Level getLevel(String columnName) {
      for (Column column : columns) {
        if (columnName.equals(column.name)) {
          return column.level;
        }
      }
      return Level.PUBLIC;
    }
  }

  class Column {
    public String name;
    public Level level;
  }

  class Mapping {
    public String schema;
    public String name;
  }
}
