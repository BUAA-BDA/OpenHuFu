package com.hufudb.onedb.owner.adapter.postgis;

import org.postgresql.util.PGobject;
import org.postgis.GeometryBuilder;
import com.hufudb.onedb.data.storage.Point;

public class PostgisUtils {
    public PostgisUtils() {}

    /**
   * convert PostGIS Point to Pair in java
   */
  public Point fromPGPoint(PGobject pgpoint) {
    try {
      org.postgis.Point p = GeometryBuilder.geomFromString(o.getValue()).getPoint(0);
      return new Point(p.x, p.y)
    } catch (SQLException e) {
      e.printStackTrace();
      return null;
    }
  }
}
