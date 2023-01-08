package com.hufudb.onedb.backend.entity.request;

import com.hufudb.onedb.data.schema.utils.PojoColumnDesc;

public class DesensitizeRequest {
    public String tableName;
    public PojoColumnDesc columnDesc;
}
