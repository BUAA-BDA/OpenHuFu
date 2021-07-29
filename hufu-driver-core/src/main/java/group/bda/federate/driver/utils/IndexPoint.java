package group.bda.federate.driver.utils;

import group.bda.federate.rpc.FederateCommon.Point;

public class IndexPoint {
  public Point point;
  public long id; // todo: change id type from long to <T>

  IndexPoint(Point p, long id) {
    this.point = p;
    this.id = id;
  }

  IndexPoint(double lon, double lat, long id) {
    this.point = Point.newBuilder().setLongitude(lon).setLatitude(lat).build();
    this.id = id;
  }
}
