package group.bda.federate.driver.utils;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class PostgresqlTableIterator implements TableIterator {

  private ResultSet rs;

  public PostgresqlTableIterator(String tableName, String indexAttribute, String geoAttribute, Connection connection) {
    String sql = String.format("SELECT %s, ST_X(%s), ST_Y(%s) FROM %s", indexAttribute, geoAttribute, geoAttribute,
        tableName);
    try {
      Statement st = connection.createStatement();
      this.rs = st.executeQuery(sql);
    } catch (SQLException e) {
      e.printStackTrace();
    }
  }

  @Override
  public IndexPoint next() {
    try {
      long id = rs.getLong(1);
      double lon = rs.getFloat(2);
      double lat = rs.getFloat(3);
      return new IndexPoint(lon, lat, id);
    } catch (SQLException e) {
      e.printStackTrace();
      return null;
    }
  }

  @Override
  public boolean hasNext() {
    try {
      return rs.next();
    } catch (SQLException e) {
      e.printStackTrace();
      return false;
    }
  }
}
