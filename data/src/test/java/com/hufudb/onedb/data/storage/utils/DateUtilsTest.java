package com.hufudb.onedb.data.storage.utils;

import static org.junit.Assert.assertEquals;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import org.junit.Test;

public class DateUtilsTest {
  @Test
  public void testDateConverter() {
    Date date = Date.valueOf("2022-07-03");
    int dint = DateUtils.dateToInt(date);
    assertEquals(date, DateUtils.intToDate(dint));

    date = Date.valueOf("1970-01-01");
    dint = DateUtils.dateToInt(date);
    assertEquals(date, DateUtils.intToDate(dint));

    date = Date.valueOf("1969-02-28");
    dint = DateUtils.dateToInt(date);
    assertEquals(date, DateUtils.intToDate(dint));

    date = Date.valueOf("1900-12-28");
    dint = DateUtils.dateToInt(date);
    assertEquals(date, DateUtils.intToDate(dint));

    Time time = Time.valueOf("00:05:29");
    int t = DateUtils.timeToInt(time);
    assertEquals(time, DateUtils.intToTime(t));

    time = Time.valueOf("12:59:31");
    t = DateUtils.timeToInt(time);
    assertEquals(time, DateUtils.intToTime(t));

    Timestamp timestamp = Timestamp.valueOf("1999-08-11 00:05:29.999");
    long ts = DateUtils.timestampToLong(timestamp);
    assertEquals(timestamp, DateUtils.longToTimestamp(ts));
    
    timestamp = Timestamp.valueOf("1900-12-28 12:59:31.123");
    ts = DateUtils.timestampToLong(timestamp);
    assertEquals(timestamp, DateUtils.longToTimestamp(ts));
  }
}
