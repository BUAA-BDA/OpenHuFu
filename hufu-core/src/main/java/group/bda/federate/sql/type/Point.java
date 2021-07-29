package group.bda.federate.sql.type;

import java.io.Serializable;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.esri.core.geometry.Geometry;
import com.esri.core.geometry.SpatialReference;

import org.apache.calcite.runtime.Geometries;
import org.apache.calcite.runtime.Geometries.Geom;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class Point implements Geom, Serializable {
  private static final long serialVersionUID = 10L;
  private static final SpatialReference SPATIAL_REFERENCE =
          SpatialReference.create(4326);

  private double x;
  private double y;

  public Point(double x, double y) {
    this.x = x;
    this.y = y;
  }

  public Point(com.esri.core.geometry.Point p) {
    this.x = p.getX();
    this.y = p.getY();
  }

  public Point() {
    super();
  }

  public double getX() {
    return x;
  }

  public double getY() {
    return y;
  }

  @Override
  public boolean equals(Object obj) {
    return obj == this || (obj instanceof Point && ((Point) obj).getX() == x && ((Point) obj).getY() == y);
  }

  private static class PointParser {
    private static final Logger LOG = LogManager.getLogger(PointParser.class);

    private static String pointRex = "\\(\\s*([\\-]?[0-9]+[.]?[0-9]*)\\s+([\\-]?[0-9]+[.]?[0-9]*)\\s*\\)";
    private Pattern pointPattern;

    private static PointParser parser;

    private PointParser() {
      pointPattern = Pattern.compile(pointRex);
      parser = this;
    }

    public static PointParser getParser() {
      if (parser == null) {
        return new PointParser();
      } else {
        return parser;
      }
    }

    public static Point parse(String pointStr) {
      PointParser parser = PointParser.getParser();
      Matcher pointMatcher = parser.pointPattern.matcher(pointStr);
      if (pointMatcher.find()) {
        String xStr = pointMatcher.group(1);
        String yStr = pointMatcher.group(2);
        return new Point(Double.parseDouble(xStr), Double.parseDouble(yStr));
      } else {
        LOG.error("can't parse {} to Point", pointStr);
        return new Point(0, 0);
      }
    }
  }

  @Override
  public String toString() {
    return String.format("POINT(%f %f)", getX(), getY());
  }

  public static Point parsePoint(String pointStr) {
    return PointParser.parse(pointStr);
  }

  public double distance(Point p) {
    return Math.sqrt(Math.pow(this.getX() - p.getX(), 2) + Math.pow(this.getY() - p.getY(), 2));
  }

  @Override
  public int compareTo(Geom o) {
    return toString().compareTo(o.toString());
  }

  @Override
  public Geometry g() {
    return new com.esri.core.geometry.Point(x, y);
  }

  @Override
  public Geometries.Type type() {
    return Geometries.Type.POINT;
  }

  @Override
  public SpatialReference sr() {
    return SPATIAL_REFERENCE;
  }

  @Override
  public Geom transform(int srid) {
    return this;
  }

  @Override
  public Geom wrap(Geometry g) {
    assert g instanceof com.esri.core.geometry.Point;
    return new Point((com.esri.core.geometry.Point) g);
  }
}
