package com.hufudb.onedb.data.desensitize.utils;

import com.hufudb.onedb.data.storage.utils.MethodTypeWrapper;
import com.hufudb.onedb.data.schema.utils.PojoMethod;
import com.hufudb.onedb.proto.OneDBData;

public class NumberFloor extends PojoMethod {

    public int place;

    public NumberFloor() {}

    public NumberFloor(MethodTypeWrapper type, int place) {
        super(type);
        this.place = place;
        super.allowedTypes.add(OneDBData.ColumnType.INT);
        super.allowedTypes.add(OneDBData.ColumnType.LONG);
    }
    @Override
    public OneDBData.Method toMethod() {
        return  OneDBData.Method.newBuilder().
                setNumberFloor(OneDBData.NumberFloor.newBuilder().setType(OneDBData.MethodType.NUMBER_FLOOR).setPlace(place)).build();
    }

    public int getPlace() {
        return place;
    }

    public void setPlace(int place) {
        this.place = place;
    }

    @Override
    public Object implement(Object val, OneDBData.ColumnDesc columnDesc, OneDBData.Method method) {
        OneDBData.NumberFloor numberFloor = method.getNumberFloor();
        int place = numberFloor.getPlace();
        if (columnDesc.getType() == OneDBData.ColumnType.INT) {
            return numberFloor32((Integer) val, place);
        }
        if (columnDesc.getType() == OneDBData.ColumnType.LONG) {
            return numberFloor64((Long) val, place);
        }
        return null;
    }

    public static Integer numberFloor32(Integer val, int place) {
        long p = 1L;
        for (int i = 0; i < place; i++) {
            p = p * 10;
        }
        long tmp = (val).longValue();
        int rt;
        if (tmp < p) {
            rt = 0;
        } else {
            rt = val - (int) (tmp % p);
        }
        return rt;
    }

    public static Long numberFloor64(Long val, int place) {
        long p = 1L;
        for (int i = 0; i < place; i++) {
            p = p * 10;
        }
        long tmp = val;
        long rt;
        if (tmp < p) {
            rt = 0L;
        } else {
            rt = val - (tmp % p);
        }
        return rt;
    }
}
