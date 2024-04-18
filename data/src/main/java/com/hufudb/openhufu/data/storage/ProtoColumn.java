package com.hufudb.openhufu.data.storage;

import com.hufudb.openhufu.data.storage.utils.GeometryUtils;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import com.google.protobuf.ByteString;
import com.hufudb.openhufu.data.storage.utils.DateUtils;
import com.hufudb.openhufu.proto.OpenHuFuData.BoolColumn;
import com.hufudb.openhufu.proto.OpenHuFuData.BytesColumn;
import com.hufudb.openhufu.proto.OpenHuFuData.ColumnProto;
import com.hufudb.openhufu.proto.OpenHuFuData.ColumnType;
import com.hufudb.openhufu.proto.OpenHuFuData.F32Column;
import com.hufudb.openhufu.proto.OpenHuFuData.F64Column;
import com.hufudb.openhufu.proto.OpenHuFuData.I32Column;
import com.hufudb.openhufu.proto.OpenHuFuData.I64Column;
import com.hufudb.openhufu.proto.OpenHuFuData.StringColumn;
import org.locationtech.jts.geom.Geometry;

/**
 * Wrapper of protocol buffer ColumnProto
 */
public class ProtoColumn implements Column {
  final ColumnType type;
  final ColumnProto column;
  final CellGetter getter;
  final BitArray isNulls;
  final int size;

  ProtoColumn(ColumnType type, ColumnProto column) {
    this.type = type;
    this.column = column;
    switch (column.getColCase()) {
      case I32COL:
        switch (type) {
          case TIME:
            this.getter = (rowNum) -> DateUtils.intToTime(column.getI32Col().getCell(rowNum));
            break;
          default:
            this.getter = (rowNum) -> column.getI32Col().getCell(rowNum);
        }
        this.size = column.getI32Col().getCellCount();
        break;
      case I64COL:
        switch (type) {
          case DATE:
            this.getter = (rowNum) -> DateUtils.longToDate(column.getI64Col().getCell(rowNum));
            break;
          case TIMESTAMP:
            this.getter = (rowNum) -> DateUtils.longToTimestamp(column.getI64Col().getCell(rowNum));
            break;
          default:
            this.getter = (rowNum) -> column.getI64Col().getCell(rowNum);
        }
        this.size = column.getI64Col().getCellCount();
        break;
      case F32COL:
        this.getter = (rowNum) -> column.getF32Col().getCell(rowNum);
        this.size = column.getF32Col().getCellCount();
        break;
      case F64COL:
        this.getter = (rowNum) -> column.getF64Col().getCell(rowNum);
        this.size = column.getF64Col().getCellCount();
        break;
      case STRCOL:
        this.getter = (rowNum) -> column.getStrcol().getCell(rowNum);
        this.size = column.getStrcol().getCellCount();
        break;
      case BOOLCOL:
        this.getter = (rowNum) -> column.getBoolcol().getCell(rowNum);
        this.size = column.getBoolcol().getCellCount();
        break;
      case BYTESCOL:
        if (type == ColumnType.GEOMETRY) {
          this.getter = (rowNum) -> Point.fromBytes(column.getBytescol().getCell(rowNum).toByteArray());
        } else {
          this.getter = (rowNum) -> column.getBytescol().getCell(rowNum).toByteArray();
        }
        this.size = column.getBytescol().getCellCount();
        break;
      default:
        throw new UnsupportedOperationException("Unsupported type for column");
    }
    this.isNulls = new BitArray(this.size, column.getIsnull().toByteArray());
  }

  @Override
  public Object getObject(int rowNum) {
    if (isNull(rowNum)) {
      return null;
    } else {
      return getter.get(rowNum);
    }
  }

  @Override
  public boolean isNull(int rowNum) {
    return isNulls.get(rowNum);
  }

  @Override
  public ColumnType getType() {
    return type;
  }

  @Override
  public int size() {
    return size;
  }

  public static Builder newBuilder(ColumnType type) {
    return new Builder(type);
  }

  public static class Builder {
    final ColumnType type;
    final ColumnProto.Builder columnBuilder;
    final BitArray.Builder nullBuilder;
    final CellAppender appender;
    I32Column.Builder i32Builder;
    I64Column.Builder i64Builder;
    F32Column.Builder f32Builder;
    F64Column.Builder f64Builder;
    BoolColumn.Builder boolBuilder;
    StringColumn.Builder strBuilder;
    BytesColumn.Builder bytesBuilder;

    Builder(ColumnType type) {
      this.type = type;
      this.columnBuilder = ColumnProto.newBuilder();
      this.nullBuilder = BitArray.builder();
      switch (type) {
        case BOOLEAN:
          boolBuilder = BoolColumn.newBuilder();
          appender = (val) -> boolBuilder.addCell((Boolean) val);
          break;
        case BLOB:
          bytesBuilder = BytesColumn.newBuilder();
          appender = (val) -> bytesBuilder.addCell(ByteString.copyFrom((byte[]) val));
          break;
        case GEOMETRY:
          bytesBuilder = BytesColumn.newBuilder();
          appender = (val) -> bytesBuilder.addCell(ByteString.copyFrom(((Point) val).toBytes()));
          break;
        case STRING:
          strBuilder = StringColumn.newBuilder();
          appender = (val) -> strBuilder.addCell((String) val);
          break;
        case FLOAT:
          f32Builder = F32Column.newBuilder();
          appender = (val) -> f32Builder.addCell(((Number) val).floatValue());
          break;
        case DOUBLE:
          f64Builder = F64Column.newBuilder();
          appender = (val) -> f64Builder.addCell(((Number) val).doubleValue());
          break;
        case BYTE:
        case SHORT:
        case INT:
          i32Builder = I32Column.newBuilder();
          appender = (val) -> i32Builder.addCell(((Number) val).intValue());
          break;
        case TIME:
          i32Builder = I32Column.newBuilder();
          appender = (val) -> i32Builder.addCell(DateUtils.timeToInt((Time) val));
          break;
        case LONG:
          i64Builder = I64Column.newBuilder();
          appender = (val) -> i64Builder.addCell(((Number) val).longValue());
          break;
        case DATE:
          i64Builder = I64Column.newBuilder();
          appender = (val) -> i64Builder.addCell(DateUtils.dateToLong((Date) val));
          break;
        case TIMESTAMP:
          i64Builder = I64Column.newBuilder();
          appender = (val) -> i64Builder.addCell(DateUtils.timestampToLong((Timestamp) val));
          break;
        default:
          throw new UnsupportedOperationException("Unsupported column type");
      }
    }

    void add(Object val) {
      if (val != null) {
        nullBuilder.add(false);
        appender.append(val);
      } else {
        nullBuilder.add(true);
        switch (type) {
          case BOOLEAN:
            appender.append(false);
            break;
          case STRING:
            appender.append("");
            break;
          case BLOB:
            appender.append(new byte[0]);
            break;
          default:
            appender.append(0);
        }
      }
    }

    void clear() {
      nullBuilder.clear();
      switch (type) {
        case BOOLEAN:
          boolBuilder.clear();
          break;
        case BLOB:
        case GEOMETRY:
          bytesBuilder.clear();
          break;
        case STRING:
          strBuilder.clear();
          break;
        case FLOAT:
          f32Builder.clear();
          break;
        case DOUBLE:
          f64Builder.clear();
          break;
        case BYTE:
        case SHORT:
        case INT:
        case TIME:
          i32Builder.clear();
          break;
        case LONG:
        case DATE:
        case TIMESTAMP:
          i64Builder.clear();
          break;
        default:
          throw new UnsupportedOperationException("Unsupported column type");
      }
    }

    ColumnProto buildProto() {
      switch (type) {
        case BOOLEAN:
          columnBuilder.setBoolcol(boolBuilder.build());
          break;
        case BLOB:
        case GEOMETRY:
          columnBuilder.setBytescol(bytesBuilder.build());
          break;
        case STRING:
          columnBuilder.setStrcol(strBuilder.build());
          break;
        case FLOAT:
          columnBuilder.setF32Col(f32Builder.build());
          break;
        case DOUBLE:
          columnBuilder.setF64Col(f64Builder.build());
          break;
        case BYTE:
        case SHORT:
        case TIME:
        case INT:
          columnBuilder.setI32Col(i32Builder.build());
          break;
        case LONG:
        case DATE:
        case TIMESTAMP:
          columnBuilder.setI64Col(i64Builder.build());
          break;
        default:
          throw new UnsupportedOperationException("Unsupported column type");
      }
      columnBuilder.setIsnull(ByteString.copyFrom(nullBuilder.buildByteArray()));
      return columnBuilder.build();
    }

    ProtoColumn build() {
      return new ProtoColumn(type, buildProto());
    }
  }
}
