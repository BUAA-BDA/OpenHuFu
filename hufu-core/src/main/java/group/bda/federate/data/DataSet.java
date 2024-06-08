package group.bda.federate.data;

import java.io.Serializable;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import com.google.common.collect.ImmutableList;
import com.google.protobuf.ByteString;

import org.apache.calcite.linq4j.Enumerator;
import org.apache.commons.lang3.SerializationUtils;

import group.bda.federate.data.DataSet.DataRow;
import group.bda.federate.rpc.FederateCommon.DataSetProto;
import group.bda.federate.rpc.FederateCommon.HeaderProto;
import group.bda.federate.rpc.FederateCommon.RowsProto;
import group.bda.federate.sql.type.FederateFieldType;
import group.bda.federate.sql.type.FederateOrder;
import group.bda.federate.sql.type.Point;

public class DataSet implements Iterable<DataRow>, Enumerator<Row>, Serializable {
  final public static DataSet EMPTY_DATA_SET = new DataSet(Header.newBuilder().build(), ImmutableList.of(), "");

  private static final long serialVersionUID = 10L;
  final Header header;
  final List<Row> rows;
  final String uuid;
  Row current = null;
  int count = 0;

  private DataSet(Header header,List<Row> rows, String uuid) {
    this.header = header;
    this.rows = rows;
    this.uuid = uuid;
    this.count = rows.size();
  }

  public static DataSet newDataSet(Header header) {
    return new DataSet(header, new ArrayList<>(), "");
  }

  public static DataSet newDataSet(Header header, String uuid) {
    return new DataSet(header, new ArrayList<>(), uuid);
  }

  public static DataSet newDataSetUnsafe(Header header, List<Row> rows) {
    return new DataSet(header, rows, "");
  }

  public static DataSet newDataSetUnsafe(Header header, List<Row> rows, String uuid) {
    return new DataSet(header, rows, uuid);
  }

  /**
   * for read and serialization only, change element in view may cause unexpected
   * error
   */
  public static DataSet rangeView(DataSet dataSet, int begin, int end) {
    return new DataSet(dataSet.header, dataSet.range(begin, end), "");
  }

  public static Builder newBuilder(int size) {
    return new Builder(size);
  }

  public int rowCount() {
    return rows.size();
  }

  public int columnSize() {
    return header.size();
  }

  public void mergeDataSetUnsafe(DataSet dataSet) {
    Iterator<Row> iterator = dataSet.rawIterator();
    while (iterator.hasNext()) {
      this.rows.add(iterator.next());
    }
  }

  public void sort(final List<String> orders) {
    if (orders.isEmpty()) {
      return;
    }
    rows.sort((o1, o2) -> {
      for (String str : orders) {
        FederateOrder order = FederateOrder.parse(str);
        int fieldIndex = header.index("EXP$" + order.idx);
        int compareResult;
        switch (header.getTypeUnsafe(fieldIndex)) {
          case INT:
          case LONG:
          case SHORT:
          case FLOAT:
          case DOUBLE:
          case STRING:
          case DATE:
          case TIME:
          case TIMESTAMP:
            //TODO:
            compareResult = ((Comparable) o1.getObject(fieldIndex)).compareTo(o2.getObject(fieldIndex));
            if (order.direction.equals(FederateOrder.Direction.DESC)) {
              compareResult = compareResult * -1;
            }
            break;
          default:
            throw new RuntimeException("the field can not be sorted");
        }
        if (compareResult != 0) {
          return compareResult;
        }
      }
      return 0;
    });
  }

  public DataSetProto toProto() {
    HeaderProto headerProto = header.toProto();
    RowsProto.Builder rowsProto = RowsProto.newBuilder();
    for (Row row : rows) {
      byte[] rowBytes = SerializationUtils.serialize(row);
      rowsProto.addRow(ByteString.copyFrom(rowBytes));
    }
    return DataSetProto.newBuilder().setHeader(headerProto).setRows(rowsProto).setUuid(uuid).build();
  }

  public static DataSet fromProto(DataSetProto proto) {
    Header header = Header.fromProto(proto.getHeader());
    String uuid = proto.getUuid();
    RowsProto rowsProto = proto.getRows();
    List<Row> rows = new ArrayList<>();
    final int size = rowsProto.getRowCount();
    for (int i = 0; i < size; ++i) {
      rows.add(SerializationUtils.deserialize(rowsProto.getRow(i).toByteArray()));
    }
    return DataSet.newDataSetUnsafe(header, rows, uuid);
  }

  public DataRowBuilder newRow() {
    return new DataRowBuilder(this);
  }

  private void /**/addRow(Row.RowBuilder rowBuilder) {
    this.rows.add(rowBuilder.build());
  }

  public FederateFieldType getType(int index) {
    return header.getType(index);
  }

  public DataRow getRow(int index) {
    return new DataRow(this, rows.get(index));
  }

  public String getUuid() {
    return uuid;
  }

  private List<Row> range(int begin, int end) {
    return new ArrayList<>(rows.subList(begin, end));
  }

  public String toString() {
    return header.toTableString();
  }

  public boolean equalHeader(DataSet dataSet) {
    return this.header.equals(dataSet.header);
  }

  @Override
  public java.util.Iterator<DataRow> iterator() {
    return DataSetIterator.newIterator(this);
  }

  public java.util.Iterator<Row> rawIterator() {
    return rows.iterator();
  }

  public static class Builder {
    Header.Builder builder;

    private Builder(int size) {
      this.builder = Header.newBuilder(size);
    }

    public void set(int index, String name, FederateFieldType type) {
      builder.set(index, name, type);
    }

    public void reset() {
      builder.reset();
    }

    public DataSet build() {
      Header header = this.builder.build();
      return new DataSet(header, new ArrayList<>(), "");
    }
  }

  public static class DataRow {
    private final Header header;
    private final Row row;

    private DataRow(DataSet dataSet, Row row) {
      this.header = dataSet.header;
      this.row = row;
    }

    public Object get(int index) {
      return row.getObject(index);
    }

    public Object get(int index, FederateFieldType type) {
      switch (type) {
        case STRING:
          return getString(index);
        case BOOLEAN:
          return getBoolean(index);
        case BYTE:
          return getByte(index);
        case SHORT:
          return getShort(index);
        case INT:
          return getInt(index);
        case LONG:
          return getLong(index);
        case FLOAT:
          return getFloat(index);
        case DOUBLE:
          return getDouble(index);
        case DATE:
          return getDate(index);
        case TIME:
          return getTime(index);
        case TIMESTAMP:
          return getTimestamp(index);
        case POINT:
          return getPoint(index);
      }
      return null;
    }

    public boolean getBoolean(int index) {
      if (header.getType(index) == FederateFieldType.BOOLEAN) {
        return (boolean) row.getObject(index);
      } else {
        return false;
      }
    }

    public float getFloat(int index) {
      if (header.getType(index) == FederateFieldType.FLOAT) {
        return ((Number) row.getObject(index)).floatValue();
      } else {
        return 0.0f;
      }
    }

    public double getDouble(int index) {
      FederateFieldType type = header.getType(index);
      if (type == FederateFieldType.DOUBLE || type == FederateFieldType.FLOAT) {
        return ((Number) row.getObject(index)).doubleValue();
      } else {
        return 0.0;
      }
    }

    public short getShort(int index) {
      FederateFieldType type = header.getType(index);
      if (type == FederateFieldType.SHORT || type == FederateFieldType.BYTE) {
        return ((Number) row.getObject(index)).shortValue();
      } else {
        return 0;
      }
    }

    public byte getByte(int index) {
      if (header.getType(index) == FederateFieldType.BYTE) {
        return ((Number) row.getObject(index)).byteValue();
      } else {
        return 0;
      }
    }

    public int getInt(int index) {
      FederateFieldType type = header.getType(index);
      if (header.getType(index) == FederateFieldType.INT || type == FederateFieldType.SHORT || type == FederateFieldType.BYTE) {
        return ((Number) row.getObject(index)).intValue();
      } else {
        return 0;
      }
    }

    public long getLong(int index) {
      FederateFieldType type = header.getType(index);
      if (type == FederateFieldType.LONG || type == FederateFieldType.INT || type == FederateFieldType.SHORT || type == FederateFieldType.BYTE) {
        return ((Number) row.getObject(index)).longValue();
      } else {
        return 0L;
      }
    }

    public String getString(int index) {
      if (header.getType(index) == FederateFieldType.STRING) {
        return (String) row.getObject(index);
      } else {
        return "";
      }
    }

    public Date getDate(int index) {
      if (header.getType(index) == FederateFieldType.DATE) {
        return new Date((Long) row.getObject(index) * 86400000L);
      } else {
        return new Date(0);
      }
    }

    public Time getTime(int index) {
      if (header.getType(index) == FederateFieldType.DATE) {
        return new Time((Long) row.getObject(index));
      } else {
        return new Time(0);
      }
    }

    public Timestamp getTimestamp(int index) {
      if (header.getType(index) == FederateFieldType.DATE) {
        return new Timestamp((Long) row.getObject(index));
      } else {
        return new Timestamp(0);
      }
    }

    public Point getPoint(int index) {
      if (header.getType(index) == FederateFieldType.POINT) {
        return (Point) row.getObject(index);
      } else {
        return new Point();
      }
    }

    @Override
    public String toString() {
      StringBuilder builder = new StringBuilder();
      for (int i = 0; i < header.size(); ++i) {
        Object o = row.getObject(i);
        switch (header.getTypeUnsafe(i)) {
          case BYTE:
            builder.append((byte) o);
            break;
          case SHORT:
            builder.append((short) o);
            break;
          case INT:
            builder.append((int) o);
            break;
          case LONG:
            builder.append((long) o);
            break;
          case FLOAT:
            builder.append((float) o);
            break;
          case DOUBLE:
            builder.append((double) o);
            break;
          case BOOLEAN:
            builder.append((boolean) o);
            break;
          case STRING:
            builder.append((String) o);
            break;
          case POINT:
            builder.append(o);
            break;
          case DATE:
            builder.append(new Date(((Long) o) * 86400000L));
            break;
          case TIME:
            builder.append(new Time((Long) o));
            break;
          case TIMESTAMP:
            builder.append(new Timestamp((Long) o));
            break;
          default:
            builder.append("UNKNOWN");
            break;
        }
        builder.append("\t");
      }
      return builder.toString();
    }
  }

  public static class DataSetIterator implements Iterator<DataRow> {
    private final DataSet dataSet;
    final Iterator<Row> rows;

    private DataSetIterator(DataSet dataSet) {
      this.dataSet = dataSet;
      rows = dataSet.rows.iterator();
    }

    public static DataSetIterator newIterator(DataSet dataSet) {
      return new DataSetIterator(dataSet);
    }

    @Override
    public boolean hasNext() {
      return rows.hasNext();
    }

    @Override
    public DataRow next() {
      return new DataRow(dataSet, rows.next());
    }

    @Override
    public void remove() {
      rows.remove();
    }
  }

  public static class DataRowBuilder {
    private final DataSet dataSet;
    private Row.RowBuilder rowBuilder;

    private DataRowBuilder(DataSet dataSet) {
      this.dataSet = dataSet;
      this.rowBuilder = Row.newBuilder(dataSet.columnSize());
    }

    public void reset() {
      this.rowBuilder = Row.newBuilder(dataSet.columnSize());
    }

    public void setUnsafe(int index, Object value) {
      rowBuilder.set(index, value);
    }

    public void setString(int index, String value) {
      FederateFieldType expect = dataSet.getType(index);
      switch (expect) {
        case BYTE:
          rowBuilder.set(index, Byte.parseByte(value));
          return;
        case SHORT:
          rowBuilder.set(index, Short.parseShort(value));
          return;
        case INT:
          rowBuilder.set(index, Integer.parseInt(value));
          return;
        case LONG:
        case TIMESTAMP:
          rowBuilder.set(index, Long.parseLong(value));
          return;
        case FLOAT:
          rowBuilder.set(index, Float.parseFloat(value));
          return;
        case DOUBLE:
          rowBuilder.set(index, Double.parseDouble(value));
          return;
        case BOOLEAN:
          rowBuilder.set(index, Boolean.parseBoolean(value));
          return;
        case STRING:
          rowBuilder.set(index, value);
          return;
        case POINT:
          rowBuilder.set(index, Point.parsePoint(value));
          return;
        default:
          // unsupported type(including data, time)
      }
    }

    public boolean set(int index, Object value) {
      FederateFieldType expect = dataSet.getType(index);
      switch (expect) {
        case STRING:
          if (value instanceof String) {
            rowBuilder.set(index, value);
            return true;
          }
          return false;
        case BOOLEAN:
          if (value instanceof Boolean) {
            rowBuilder.set(index, value);
            return true;
          }
          return false;
        case BYTE:
          if (value instanceof Byte) {
            rowBuilder.set(index, value);
            return true;
          }
          return false;
        case SHORT:
          if (value instanceof Short) {
            rowBuilder.set(index, value);
            return true;
          }
          return false;
        case INT:
          if (value instanceof Integer || value instanceof Long) {
            rowBuilder.set(index, value);
            return true;
          }
          return false;
        case LONG:
          if (value instanceof Long) {
            rowBuilder.set(index, value);
            return true;
          }
          return false;
        case FLOAT:
          if (value instanceof Float) {
            rowBuilder.set(index, value);
            return true;
          }
          return false;
        case DOUBLE:
          if (value instanceof Double) {
            rowBuilder.set(index, value);
            return true;
          }
          return false;
        case DATE:
          if (value instanceof Date) {
            Long v = ((Date) value).getTime() / 86400000L;
            rowBuilder.set(index, v);
            return true;
          } else if (value instanceof Integer || value instanceof Long) {
            rowBuilder.set(index, ((Number) value).longValue());
            return true;
          }
          return false;
        case TIME:
          if (value instanceof Time) {
            Long v = ((Time) value).getTime();
            rowBuilder.set(index, v);
            return true;
          } else if (value instanceof Integer || value instanceof Long) {
            rowBuilder.set(index, ((Number) value).longValue());
            return true;
          }
          return false;
        case TIMESTAMP:
          if (value instanceof Timestamp) {
            Long v = ((Timestamp) value).getTime();
            rowBuilder.set(index, v);
            return true;
          } else if (value instanceof Long) {
            rowBuilder.set(index, value);
            return true;
          }
          return false;
        case POINT:
          if (value instanceof Point) {
            rowBuilder.set(index, value);
            return true;
          }
          return false;
        default:
          // unsupported type
          return false;
      }
    }

    public void build() {
      dataSet.addRow(rowBuilder);
    }
  }

  @Override
  public Row current() {
    return current;
  }

  @Override
  public boolean moveNext() {
    if (count < rows.size()) {
      current = rows.get(count);
      return true;
    }
    return false;
  }

  @Override
  public void reset() {
    count = 0;
  }

  @Override
  public void close() {
    // do nothin
  }
}
