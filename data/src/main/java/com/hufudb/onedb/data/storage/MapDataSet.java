package com.hufudb.onedb.data.storage;

import java.util.List;
import com.hufudb.onedb.data.schema.Schema;
import com.hufudb.onedb.data.function.Mapper;

/**
 * 类似于虚表，持有一组Mapper用来定义其与实际表的映射关系
 * 在访问该类对象时，可以透明化这层映射关系
 * 
 * Dataset which map each row from source dataset into a new row
 */
public class MapDataSet implements DataSet {
  private final Schema schema;
  private final List<Mapper> mappings;
  private final DataSet source;

  MapDataSet(Schema schema, List<Mapper> mappings, DataSet source) {
    this.schema = schema;
    this.mappings = mappings;
    this.source = source;
  }

  public static MapDataSet create(Schema schema, List<Mapper> mappings, DataSet source) {
    return new MapDataSet(schema, mappings, source);
  }

  @Override
  public Schema getSchema() {
    return schema;
  }

  @Override
  public DataSetIterator getIterator() {
    return new MapIterator(source.getIterator());
  }

  @Override
  public void close() {
    source.close();
  }

  class MapIterator implements DataSetIterator {
    DataSetIterator iterator;

    MapIterator(DataSetIterator iterator) {
      this.iterator = iterator;
    }

    @Override
    public boolean next() {
      return iterator.next();
    }

    @Override
    public Object get(int columnIndex) {
      return mappings.get(columnIndex).map(iterator);
    }

    @Override
    public int size() {
      return schema.size();
    }
  }
}
