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

public class KNNLaplaceFilterTests {

  public static void main(String[] args) {
//    double[] longitudes = {119.905257, 104.3389064, 102.6679303, 113.4104199, 121.5697965};
//    double[] latitudes = {30.0162251, 30.7070391, 23.1316743, 22.71103, 31.3551271};
    double[] longitudes = {120.90809, 120.7471905, 120.74455, 120.145087, 120.0938017};
    double[] latitudes = {30.36696, 31.1709233, 31.1784, 30.1439031, 30.2162853};
    List<Double> radio = new ArrayList<>();
    String table = "osm_4";
    int totalCount = 1;
    int k = 16;
    int silo = 6;
    String jdbcUrl = "jdbc:postgresql://localhost:54321/osm_db";
    String username= "hufu";
    String password = "hufu";
    String totalSql = String.format("SELECT count(*) from %s", table);
    try (Connection conn = DriverManager.getConnection(jdbcUrl, username, password)) {
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

      double[] result =
          Laplace.boundedPlanarLaplaceMechanism(longitude, latitude, FedSpatialConfig.EPS_DP,
              FedSpatialConfig.DELTA_DP);
      double newLongitude = result[0];
      double newLatitude = result[1];

      double kNND = Double.MAX_VALUE;
      for (int j = 0;j < silo;j++) {
        String distanceSql =
            String.format(
                "SELECT location <-> 'SRID=4326;POINT(%f %f)' distance from %s order by distance asc limit %d",
                newLongitude, newLatitude,table + "_" + j, k);
        try (Connection conn = DriverManager.getConnection(jdbcUrl, username, password)) {
          Statement stmt = conn.createStatement();
          ResultSet rs = stmt.executeQuery(distanceSql);
          for(int index = 0;index < k && rs.next();index++) {
          }
          if (kNND > rs.getDouble(1)) {
            kNND = rs.getDouble(1);
          }
          System.out.println(distanceSql + ", k=" + k + " distance: " + kNND);
        } catch (SQLException e) {
          throw new RuntimeException(e);
        }
      }
      System.out.println("kNND: " + kNND);
      double newRadius = kNND + Math.sqrt(
          Math.pow(result[0] - longitude, 2) + Math.pow(result[1] - latitude, 2));;

      System.out.println("(" + longitude + "," + latitude + ")");
      System.out.println("(" + newLongitude + "," + newLatitude + ")");

      String sql =
          String.format("SELECT count(*) from %s where location <-> 'SRID=4326;POINT(%f %f)' <= %f",
              table, longitude, latitude, newRadius);

      try (Connection conn = DriverManager.getConnection(jdbcUrl, username, password)) {
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
    System.out.println("Average radio: " + radio.stream().mapToDouble(Double::doubleValue).average()
        .getAsDouble());
  }
}