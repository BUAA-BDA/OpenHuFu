package group.bda.federate.driver.utils;

import group.bda.federate.config.FedSpatialConfig;
import group.bda.federate.security.dp.Laplace;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

public class RangeQueryLaplaceFilterTests {

  public static void main(String[] args) {
//    double[] longitudes = {110.19075, 114.2250746, 120.8391325, 88.7105832, 110.0375095};
//    double[] latitudes = {18.95975, 22.4224543, 29.2714912, 27.6365303, 19.9629952};
//    double[] radius = {0.0935247, 0.0935247, 0.0935247, 0.0935247, 0.0935247};
    double[] longitudes = {120.90809, 120.7471905, 120.74455, 120.145087, 120.0938017};
    double[] latitudes = {30.36696, 31.1709233, 31.1784, 30.1439031, 30.2162853};
    double[] radius = {0.0935247, 0.0935247, 0.0935247, 0.0935247, 0.0935247};
    List<Double> radio = new ArrayList<>();
    String table = "osm_4";
    int totalCount = 1;

    String totalSql = String.format("SELECT count(*) from %s", table);
    try (Connection conn = DriverManager.getConnection("jdbc:postgresql://localhost:54321/osm_db",
        "hufu", "hufu")) {
      Statement stmt = conn.createStatement();
      ResultSet rs = stmt.executeQuery(totalSql);
      if (rs.next()) {
        totalCount = rs.getInt(1);
      }
    } catch (SQLException e) {
      System.out.println(e.getMessage());
    }

    for (int i = 0; i < longitudes.length; i++) {
      double longitude = longitudes[i];
      double latitude = latitudes[i];
      double r = radius[i];

      double[] result =
          Laplace.boundedPlanarLaplaceMechanism(longitude, latitude, FedSpatialConfig.EPS_DP,
              FedSpatialConfig.DELTA_DP);
      double newLongitude = result[0];
      double newLatitude = result[1];
      double newRadius = r + Math.sqrt(
          Math.pow(result[0] - longitude, 2) + Math.pow(result[1] - latitude, 2));
      System.out.println("(" + longitude + "," + latitude + "), " + r);
      System.out.println("(" + newLongitude + "," + newLatitude + "), " + newRadius);
      String sql =
          String.format("SELECT count(*) from %s where location <-> 'SRID=4326;POINT(%f %f)' <= %f",
              table, newLongitude, newLatitude, newRadius);

      try (Connection conn = DriverManager.getConnection("jdbc:postgresql://localhost:54321/osm_db",
          "hufu", "hufu")) {
        Statement stmt = conn.createStatement();
        System.out.println(sql);
        ResultSet rs = stmt.executeQuery(sql);

        rs.next();
        int count = rs.getInt(1);
        System.out.println("Count: " + count);
        System.out.println("Ratio: " + (double) (totalCount - count) / totalCount);
        radio.add((double) (totalCount - count) / totalCount);
        rs.close();
      } catch (SQLException e) {
        System.out.println(e.getMessage());
      }
    }
    System.out.println("Average radio: " + radio.stream().mapToDouble(Double::doubleValue).average().getAsDouble());
  }
}