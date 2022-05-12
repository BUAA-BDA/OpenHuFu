package com.hufudb.onedb.data.storage;

import com.hufudb.onedb.data.OneDBData.ColumnProto;
import com.hufudb.onedb.data.OneDBData.ColumnType;

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
    return null;
  }

  @Override
  public int size() {
    return size;
  }
}
