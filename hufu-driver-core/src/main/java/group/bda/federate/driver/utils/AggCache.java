package group.bda.federate.driver.utils;

import group.bda.federate.data.Header;
import group.bda.federate.data.Row;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class AggCache {
  private static final Logger LOG = LogManager.getLogger(AggCache.class);
  private final Row row;
  private final Header header;

  public AggCache(Row row, Header header) {
    this.row = row;
    this.header = header;
  }

  public double getColumn(int index) {
    switch (header.getType(index)) {
      case INT:
        return (double) ((int)row.getObject(index));
      case SHORT:
        return (double) ((short)row.getObject(index));
      case LONG:
        return (double) ((long)row.getObject(index));
      case FLOAT:
        return (double) ((float)row.getObject(index));
      case DOUBLE:
        return (double) row.getObject(index);
      default:
        LOG.error("not support type");
        throw new RuntimeException("not support type");
    }
  }
}
