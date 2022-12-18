package com.hufudb.onedb.data.schema.utils;

import java.util.List;

public class PojoActualTableSchema {

    public String actualName;
    public List<PojoColumnDesc> actualColumns;

    public PojoActualTableSchema(String actualName, List<PojoColumnDesc> actualColumns) {
        this.actualName = actualName;
        this.actualColumns = actualColumns;
    }

    public String getActualName() {
        return actualName;
    }

    public List<PojoColumnDesc> getActualColumns() {
        return actualColumns;
    }

    public void setActualName(String actualName) {
        this.actualName = actualName;
    }

    public void setActualColumns(List<PojoColumnDesc> actualColumns) {
        this.actualColumns = actualColumns;
    }
}
