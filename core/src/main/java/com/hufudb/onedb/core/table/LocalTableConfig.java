package com.hufudb.onedb.core.table;

public class LocalTableConfig {
    public String endpoint;
    public String localName;

    public LocalTableConfig() {}

    public LocalTableConfig(String endpoint, String localName) {
      this.endpoint = endpoint;
      this.localName = localName;
    }

    public String getEndpoint() {
      return endpoint;
    }

    public void setEndpoint(String endpoint) {
      this.endpoint = endpoint;
    }

    public String getLocalName() {
      return localName;
    }

    public void setLocalName(String localName) {
      this.localName = localName;
    }

    @Override
    public String toString() {
      return String.format("%s->%s", endpoint, localName);
    }
  }