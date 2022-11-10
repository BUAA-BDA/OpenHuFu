package com.hufudb.onedb.owner.adapter.json;

import com.hufudb.onedb.data.schema.Schema;
import com.hufudb.onedb.data.storage.DataSet;
import com.hufudb.onedb.data.storage.DataSetIterator;
import com.hufudb.onedb.owner.adapter.json.jsonsrc.JsonSrc;

import java.util.Iterator;

public class JsonDateSet implements DataSet {
  final Schema schema;
  final JsonSrc jsonSrc;

  public JsonDateSet(Schema schema, JsonSrc jsonSrc) {
    this.schema = schema;
    this.jsonSrc = jsonSrc;
  }

  @Override
  public String toString() {
    return schema.toString();
  }

  @Override
  public Schema getSchema() {
    return schema;
  }

  @Override
  public DataSetIterator getIterator() {
    return new JsonIterator(jsonSrc.getIterator());
  }

  @Override
  public void close() {
    // do nothing
  }

  class JsonIterator implements DataSetIterator {
    Iterator<String[]> iter;
    String[] current;

    public JsonIterator(Iterator<String[]> iter) {
      this.iter = iter;
      this.current = null;
    }

    @Override
    public Object get(int columnIndex) {
      return current[columnIndex];
    }

    @Override
    public int size() {
      return schema.size();
    }

    @Override
    public boolean next() {
      if (iter.hasNext()) {
        current = iter.next();
        return true;
      }
      return false;
    }
  }
}
