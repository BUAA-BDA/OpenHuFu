package com.hufudb.onedb.owner.adapter.json.jsonsrc;

import com.hufudb.onedb.data.storage.Row;

import java.util.Iterator;
import java.util.List;

public interface JsonSrc {
  public List<String> getColumnsNames();
  public Iterator<String[]> getIterator();
}
