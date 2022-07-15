package com.hufudb.onedb.owner.adapter.json.jsonsrc;

import com.google.gson.Gson;

import java.io.IOException;
import java.io.Reader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

public class JsonText implements JsonSrc {
  List<String> headerNames;
  List<String[]> rows;

  public JsonText(Path jsonPath) {
    Gson gson = new Gson();
    JsonData jsonData = null;
    try {
      Reader reader = Files.newBufferedReader(jsonPath);
      jsonData = gson.fromJson(reader, JsonData.class);
    } catch (IOException e) {
      e.printStackTrace();
    }
    assert jsonData != null;
    this.headerNames = jsonData.header;
    this.rows = new ArrayList<>();
    int n = this.headerNames.size();
    for (HashMap<String, String> rowMap : jsonData.data) {
      String[] row = new String[n];
      for (int i = 0; i < n; i++) {
        row[i] = rowMap.get(this.headerNames.get(i));
      }
      this.rows.add(row);
    }
  }

  @Override
  public List<String> getColumnsNames() {
    return this.headerNames;
  }

  @Override
  public Iterator<String[]> getIterator() {
    return this.rows.iterator();
  }

  static class JsonData {
    public List<String> header;
    public List<HashMap<String, String>> data;
  }
}
