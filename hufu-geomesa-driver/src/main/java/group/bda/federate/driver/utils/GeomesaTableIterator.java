package group.bda.federate.driver.utils;

import org.geotools.data.simple.SimpleFeatureIterator;
import org.locationtech.jts.geom.Point;
import org.opengis.feature.simple.SimpleFeature;

public class GeomesaTableIterator implements TableIterator {

  SimpleFeatureIterator it;

  public GeomesaTableIterator(SimpleFeatureIterator it) {
    this.it = it;
  }

  @Override
  public IndexPoint next() {
    SimpleFeature feature = it.next();
    final Point p = (Point) feature.getDefaultGeometry();
    String id = feature.getAttribute("fid").toString();

    IndexPoint point = new IndexPoint(p.getX(), p.getY(), Long.valueOf(id));
    return point;
  }

  @Override
  public boolean hasNext() {
    return it.hasNext();
  }
}
