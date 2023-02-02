package com.hufudb.openhufu.data.storage.utils;

import static org.junit.Assert.assertEquals;

import com.hufudb.openhufu.data.storage.utils.DateUtils;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;
import org.junit.Test;

public class DateUtilsTest {
  @Test
  public void testDateConverter() {
    Date date = Date.valueOf("2022-07-03");
    long dlon = DateUtils.dateToLong(date);
    assertEquals(date, DateUtils.longToDate(dlon));

    date = Date.valueOf("1970-01-01");
    dlon = DateUtils.dateToLong(date);
    assertEquals(date, DateUtils.longToDate(dlon));

    date = Date.valueOf("1969-02-28");
    dlon = DateUtils.dateToLong(date);
    assertEquals(date, DateUtils.longToDate(dlon));

    date = Date.valueOf("1900-12-28");
    dlon = DateUtils.dateToLong(date);
    assertEquals(date, DateUtils.longToDate(dlon));

    Date d1 = Date.valueOf("2000-11-01");
    Calendar c1 = Calendar.getInstance();
    c1.set(Calendar.YEAR, 2000);
    c1.set(Calendar.MONTH, 10);
    c1.set(Calendar.DAY_OF_MONTH, 1);
    c1.set(Calendar.HOUR_OF_DAY, 0);
    c1.set(Calendar.MINUTE, 0);
    c1.set(Calendar.SECOND, 0);
    c1.set(Calendar.MILLISECOND, 0);
    long l1 = DateUtils.calendarToDateLong(c1);
    assertEquals(d1, DateUtils.longToDate(l1));
  }

  @Test
  public void testTimeConverter() {
    Time time = Time.valueOf("00:05:29");
    int t = DateUtils.timeToInt(time);
    assertEquals(time, DateUtils.intToTime(t));

    time = Time.valueOf("12:59:31");
    t = DateUtils.timeToInt(time);
    assertEquals(time, DateUtils.intToTime(t));
  }

  @Test
  public void testTimestampConverter() {
    Timestamp timestamp = Timestamp.valueOf("1999-08-11 00:05:29.999");
    long ts = DateUtils.timestampToLong(timestamp);
    assertEquals(timestamp, DateUtils.longToTimestamp(ts));

    timestamp = Timestamp.valueOf("1900-12-28 12:59:31.123");
    ts = DateUtils.timestampToLong(timestamp);
    assertEquals(timestamp, DateUtils.longToTimestamp(ts));
  }
}
