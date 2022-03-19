package com.hufudb.onedb.core.data;

import java.util.List;

public class AliasTableInfo {
    String localTableName;
    String publishedTableName;
    List<Field> fields;

    public String getLocalTableName() {
        return localTableName;
    }
    public void setLocalTableName(String localTableName) {
        this.localTableName = localTableName;
    }
    public String getPublishedTableName() {
        return publishedTableName;
    }
    public void setPublishedTableName(String publishedTableName) {
        this.publishedTableName = publishedTableName;
    }
    public List<Field> getFields() {
        return fields;
    }
    public void setFields(List<Field> fields) {
        this.fields = fields;
    }
}
