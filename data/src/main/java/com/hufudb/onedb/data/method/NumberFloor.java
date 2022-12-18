package com.hufudb.onedb.data.method;

import com.hufudb.onedb.data.schema.utils.PojoMethod;
import com.hufudb.onedb.proto.OneDBData;

public class NumberFloor extends PojoMethod {

    public int place;

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
            return numberFloor32(val, place);
        }
        if (columnDesc.getType() == OneDBData.ColumnType.LONG) {
            return numberFloor64(val, place);
        }
        return null;
    }

    public static Object numberFloor32(Object val, int place) {
        long p = 1L;
        for (int i = 0; i < place; i++) {
            p = p * 10;
        }
        long tmp = ((Integer) val).longValue();
        int rt;
        if (tmp < p) {
            rt = 0;
        } else {
            rt = (Integer) val - (int) (tmp % p);
        }
        return rt;
    }

    public static Object numberFloor64(Object val, int place) {
        long p = 1L;
        for (int i = 0; i < place; i++) {
            p = p * 10;
        }
        long tmp = (Long) val;
        long rt;
        if (tmp < p) {
            rt = 0L;
        } else {
            rt = (Long) val - (tmp % p);
        }
        return rt;
    }
}
