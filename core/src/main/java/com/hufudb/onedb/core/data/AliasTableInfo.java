package com.hufudb.onedb.core.data;

import java.util.List;

public class AliasTableInfo {
    String localTableName;
    String virtualTableName;
    List<Field> fields;

    public String getLocalTableName() {
        return localTableName;
    }
    public void setLocalTableName(String localTableName) {
        this.localTableName = localTableName;
    }
    public String getVirtualTableName() {
        return virtualTableName;
    }
    public void setVirtualTableName(String virtualTableName) {
        this.virtualTableName = virtualTableName;
    }
    public List<Field> getFields() {
        return fields;
    }
    public void setFields(List<Field> fields) {
        this.fields = fields;
    }
}
