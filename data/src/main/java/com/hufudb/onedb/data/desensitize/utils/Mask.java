package com.hufudb.onedb.data.desensitize.utils;

import com.hufudb.onedb.data.storage.utils.MethodTypeWrapper;
import com.hufudb.onedb.data.schema.utils.PojoMethod;
import com.hufudb.onedb.proto.OneDBData;

public class Mask extends PojoMethod {
    public long begin;
    public long end;
    public String str;

    public Mask() {
        super.type = MethodTypeWrapper.MASK;
    }

    public Mask(MethodTypeWrapper type, long begin, long end, String str) {
        super(type);
        this.begin = begin;
        this.end = end;
        this.str = str;
        super.allowedTypes.add(OneDBData.ColumnType.STRING);
    }

    @Override
    public OneDBData.Method toMethod() {
        return OneDBData.Method.newBuilder().
                setMask(OneDBData.Mask.newBuilder().setType(OneDBData.MethodType.MASK).setBegin(begin).setEnd(end).setStr(str).build()).build();
    }

    public long getBegin() {
        return begin;
    }

    public long getEnd() {
        return end;
    }

    public String getStr() {
        return str;
    }

    public void setBegin(long begin) {
        this.begin = begin;
    }

    public void setEnd(long end) {
        this.end = end;
    }

    public void setStr(String str) {
        this.str = str;
    }

    @Override
    public String implement(Object val, OneDBData.ColumnDesc columnDesc, OneDBData.Method method) {
        OneDBData.Mask mask = method.getMask();
        if (columnDesc.getType() == OneDBData.ColumnType.STRING) {
            return mask((String)val, mask.getBegin(), mask.getEnd(), mask.getStr());
        }
        return null;
    }

    public static String mask(String val, long begin, long end,  String str) {
        assert begin < end : "begin should < end";
        char chr = str.charAt(0);
        StringBuilder stringBuilder = new StringBuilder();
        int i;
        for (i = 0; i < begin; i++) {
            if (begin < val.length()) {
                stringBuilder.append(val.charAt(i));
            } else {
                return stringBuilder.toString();
            }
        }
        for (; i < end; i++) {
            if (i < val.length()) {
                stringBuilder.append(chr);
            } else {
                return stringBuilder.toString();
            }
        }
        for (;i < val.length(); i++) {
            stringBuilder.append(val.charAt(i));
        }
        return stringBuilder.toString();
    }
}
