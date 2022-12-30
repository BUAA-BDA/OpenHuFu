package com.hufudb.onedb.data.desensitize.utils;

import com.hufudb.onedb.data.schema.utils.PojoMethod;
import com.hufudb.onedb.data.storage.utils.MethodTypeWrapper;
import com.hufudb.onedb.proto.OneDBData;

import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;

public class DateFloor extends PojoMethod {

    public String floor;

    public DateFloor(MethodTypeWrapper type, String floor) {
        super(type);
        this.floor = floor;
        super.allowedTypes.add(OneDBData.ColumnType.DATE);
        super.allowedTypes.add(OneDBData.ColumnType.TIMESTAMP);
        super.allowedTypes.add(OneDBData.ColumnType.TIME);
    }


    @Override
    public OneDBData.Method toMethod() {
        return OneDBData.Method.newBuilder()
                .setDateFloor(OneDBData.DateFloor.newBuilder().setType(OneDBData.MethodType.DATE_FLOOR).setFloor(floor)).build();
    }

    public String getFloor() {
        return floor;
    }

    public void setFloor(String floor) {
        this.floor = floor;
    }

    @Override
    public Object implement(Object val, OneDBData.ColumnDesc columnDesc, OneDBData.Method method) {
        OneDBData.DateFloor dateFloor = method.getDateFloor();
        String floor = dateFloor.getFloor();
        if (columnDesc.getType() == OneDBData.ColumnType.DATE) {
            return dateFloor((Date) val, floor);
        }
        if (columnDesc.getType() == OneDBData.ColumnType.TIMESTAMP) {
            return timestampFloor((Timestamp) val, floor);
        }
        if (columnDesc.getType() == OneDBData.ColumnType.TIME) {
            return timeFloor((Time) val, floor);
        }
        return null;
    }

    public static Date dateFloor(Date val, String floor) {
        if (null == floor) {
            return null;
        }
        Calendar calendar = Calendar.getInstance();
        switch (floor) {
            case "year":
                calendar.setTime(val);
                calendar.getTime();
                val = new Date(calendar.get(Calendar.YEAR) - 1900, 0, 1);
                break;
            case "month":
                calendar.setTime(val);
                val = new Date(calendar.get(Calendar.YEAR) - 1900, calendar.get(Calendar.MONTH), 1);
                break;
            default:
                val = null;
        }
        return val;
    }
    
    public static Timestamp timestampFloor(Timestamp val, String floor) {
        if (null == floor) {
            return null;
        }
        Calendar calendar = Calendar.getInstance();
        switch (floor) {
            case "year":
                calendar.setTime(val);
                val = new Timestamp(calendar.get(Calendar.YEAR) - 1900, 0, 1, 0, 0, 0, 0);
                break;
            case "month":
                calendar.setTime(val);
                val = new Timestamp(calendar.get(Calendar.YEAR) - 1900, calendar.get(Calendar.MONTH), 1,
                        0, 0, 0, 0);
                break;
            case "day":
                calendar.setTime(val);
                val = new Timestamp(calendar.get(Calendar.YEAR) - 1900, calendar.get(Calendar.MONTH),
                        calendar.get(Calendar.DATE), 0, 0, 0, 0);
                break;
            case "hour":
                calendar.setTime(val);
                val = new Timestamp(calendar.get(Calendar.YEAR) - 1900, calendar.get(Calendar.MONTH),
                        calendar.get(Calendar.DATE), calendar.get(Calendar.HOUR), 0, 0, 0);
                break;
            case "minute":
                calendar.setTime(val);
                val = new Timestamp(calendar.get(Calendar.YEAR) - 1900, calendar.get(Calendar.MONTH),
                        calendar.get(Calendar.DATE), calendar.get(Calendar.HOUR), calendar.get(Calendar.MINUTE),
                        0, 0);
                break;
            case "second":
                calendar.setTime(val);
                val = new Timestamp(calendar.get(Calendar.YEAR) - 1900, calendar.get(Calendar.MONTH),
                        calendar.get(Calendar.DATE), calendar.get(Calendar.HOUR), calendar.get(Calendar.MINUTE),
                        calendar.get(Calendar.SECOND), 0);
                break;
            default:
                val = null;
        }
        return val;
    }

    private static Time timeFloor(Time val, String floor) {
        if (null == floor) {
            return null;
        }
        Calendar calendar = Calendar.getInstance();
        switch (floor) {
            case "hour":
                calendar.setTime(val);
                val = new Time(calendar.get(Calendar.HOUR), 0, 0);
                break;
            case "minute":
                calendar.setTime(val);
                val = new Time(calendar.get(Calendar.HOUR), calendar.get(Calendar.MINUTE), 0);
                break;
            default:
                val = null;
        }
        return val;
    }
}
