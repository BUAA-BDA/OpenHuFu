package com.hufudb.onedb.core.table;

import com.google.gson.annotations.SerializedName;

public class LocalTableConfig {
  @SerializedName("endpoint")
  public String endpoint;
  @SerializedName(value="localName", alternate = {"localname, local_name"})
  public String localName;

  public LocalTableConfig() {}

  public LocalTableConfig(String endpoint, String localName) {
    this.endpoint = endpoint;
    this.localName = localName;
  }

  @Override
  public String toString() {
    return String.format("%s->%s", endpoint, localName);
  }
}