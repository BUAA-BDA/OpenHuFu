package com.hufudb.onedb.data.method;

import com.hufudb.onedb.data.schema.utils.PojoMethod;
import com.hufudb.onedb.proto.OneDBData;

public class Replace extends PojoMethod {
    public String fromStr;
    public String toStr;

    public Replace(MethodTypeWrapper type, String fromStr, String toStr) {
        super(type);
        this.fromStr = fromStr;
        this.toStr = toStr;
        super.allowedTypes.add(OneDBData.ColumnType.STRING);
    }

    @Override
    public OneDBData.Method toMethod() {
        return OneDBData.Method.newBuilder().
                setReplace(OneDBData.Replace.newBuilder().setType(OneDBData.MethodType.REPLACE).setFromStr(fromStr).setToStr(toStr).build()).build();
    }

    public String getFromStr() {
        return fromStr;
    }

    public String getToStr() {
        return toStr;
    }

    public void setFromStr(String fromStr) {
        this.fromStr = fromStr;
    }

    public void setToStr(String toStr) {
        this.toStr = toStr;
    }

    @Override
    public String implement(Object val, OneDBData.ColumnDesc columnDesc, OneDBData.Method method) {
        OneDBData.Replace replace = method.getReplace();
        String colName = columnDesc.getName();
        if (columnDesc.getType() == OneDBData.ColumnType.STRING) {
            return ((String)val).replaceAll(replace.getFromStr(), replace.getToStr());
        }
        return null;
    }
}
