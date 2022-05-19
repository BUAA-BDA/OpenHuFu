package com.hufudb.onedb.data.storage;

import com.google.protobuf.ByteString;
import com.hufudb.onedb.proto.OneDBData.BoolColumn;
import com.hufudb.onedb.proto.OneDBData.BytesColumn;
import com.hufudb.onedb.proto.OneDBData.ColumnProto;
import com.hufudb.onedb.proto.OneDBData.ColumnType;
import com.hufudb.onedb.proto.OneDBData.F32Column;
import com.hufudb.onedb.proto.OneDBData.F64Column;
import com.hufudb.onedb.proto.OneDBData.I32Column;
import com.hufudb.onedb.proto.OneDBData.I64Column;
import com.hufudb.onedb.proto.OneDBData.StringColumn;

/**
 * Wrapper of protocol buffer ColumnProto
 */
public class ProtoColumn implements Column {
  final ColumnType type;
  final ColumnProto column;
  final CellGetter getter;
  final int size;

  ProtoColumn(ColumnType type, ColumnProto column) {
    this.type = type;
    this.column = column;
    switch (column.getColCase()) {
      case I32COL:
        this.getter = (rowNum) -> column.getI32Col().getCell(rowNum);
        this.size = column.getI32Col().getCellCount();
        break;
      case I64COL:
        this.getter = (rowNum) -> column.getI64Col().getCell(rowNum);
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
        this.getter = (rowNum) -> column.getBytescol().getCell(rowNum).toByteArray();
        this.size = column.getBytescol().getCellCount();
        break;
      default:
        throw new UnsupportedOperationException("Unsupported type for column");
    }
  }

  @Override
  public Object getObject(int rowNum) {
    return getter.get(rowNum);
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
    final ColumnProto.Builder builder;
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
      this.builder = ColumnProto.newBuilder();
      switch (type) {
        case BOOLEAN:
          boolBuilder = BoolColumn.newBuilder();
          appender = (val) -> boolBuilder.addCell((Boolean) val);
          break;
        case BLOB:
          bytesBuilder = BytesColumn.newBuilder();
          appender = (val) -> bytesBuilder.addCell(ByteString.copyFrom((byte[]) val));
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
        case LONG:
        case DATE:
        case TIME:
        case TIMESTAMP:
          i64Builder = I64Column.newBuilder();
          appender = (val) -> i64Builder.addCell(((Number) val).longValue());
          break;
        default:
          throw new UnsupportedOperationException("Unsupported column type");
      }
    }

    void add(Object val) {
      appender.append(val);
    }

    void clear() {
      switch(type) {
        case BOOLEAN:
        boolBuilder.clear();
        break;
      case BLOB:
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
        i32Builder.clear();
        break;
      case LONG:
      case DATE:
      case TIME:
      case TIMESTAMP:
        i64Builder.clear();
        break;
      default:
        throw new UnsupportedOperationException("Unsupported column type");
      }
    }

    ColumnProto buildProto() {
      switch(type) {
        case BOOLEAN:
        builder.setBoolcol(boolBuilder.build());
        break;
      case BLOB:
        builder.setBytescol(bytesBuilder.build());
        break;
      case STRING:
        builder.setStrcol(strBuilder.build());
        break;
      case FLOAT:
        builder.setF32Col(f32Builder.build());
        break;
      case DOUBLE:
        builder.setF64Col(f64Builder.build());
        break;
      case BYTE:
      case SHORT:
      case INT:
        builder.setI32Col(i32Builder.build());
        break;
      case LONG:
      case DATE:
      case TIME:
      case TIMESTAMP:
        builder.setI64Col(i64Builder.build());
        break;
      default:
        throw new UnsupportedOperationException("Unsupported column type");
      }
      return builder.build();
    }

    ProtoColumn build() {
      return new ProtoColumn(type, buildProto());
    }
  }
}
