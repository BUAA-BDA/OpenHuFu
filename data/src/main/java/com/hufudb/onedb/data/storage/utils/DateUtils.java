package com.hufudb.onedb.data.storage.utils;

import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;

/**
 * Simple Date Utils without consideration of time zone
 * todo: deal with time zone
 */
public class DateUtils {
  final Calendar calendar = Calendar.getInstance();
  final static int DAY_MASK = 0x1F;
  final static int DAY_OFFSET = 5;
  final static int MONTH_MASK = 0xF;
  final static int MONTH_OFFSET = 4;
  final static long MSFORDAY = 86400000L;
  public DateUtils() {}

  /**
   * convert date into integer
   *
   * year (23 bit) | month (4 bit) | day (5 bit)
   */
  public int dateToInt(Date date) {
    calendar.setTime(date);
    int year = calendar.get(Calendar.YEAR);
    int month = calendar.get(Calendar.MONTH);
    int day = calendar.get(Calendar.DAY_OF_MONTH);
    return (year << 9) | (month << 5) | day;
  }

  /**
   * convert calendar into date integer
   *
   * year (23 bit) | month (4 bit) | day (5 bit)
   */
  public static int calendarToDateInt(Calendar calendar) {
    int year = calendar.get(Calendar.YEAR);
    int month = calendar.get(Calendar.MONTH);
    int day = calendar.get(Calendar.DAY_OF_MONTH);
    return (year << 9) | (month << 5) | day;
  }

  /**
   * convert int to date
   */
  public Date intToDate(int dint) {
    int day = dint & DAY_MASK;
    dint = dint >> DAY_OFFSET;
    int month = dint & MONTH_MASK;
    dint = dint >> MONTH_OFFSET;
    calendar.set(dint, month, day);
    return new Date(calendar.getTimeInMillis());
  }

  /**
   * convert time to int
   */
  public int timeToInt(Time time) {
    return (int) (time.getTime() % MSFORDAY);
  }

  /**
   * convert calendar into time integer
   */
  public static int calendarToTimeInt(Calendar calendar) {
    return (int) (calendar.getTimeInMillis() % MSFORDAY);
  }

  /**
   * convert encoded int to time
   */
  public Time intToTime(int t) {
    return new Time((long) t);
  }

  /**
   * convert timestamp to long
   */
  public long timestampToLong(Timestamp ts) {
    return ts.getTime();
  }

  /**
   * convert calendar into time integer
   */
  public static long calendarToTimestampLong(Calendar calendar) {
    return calendar.getTimeInMillis();
  }

  /**
   * convert long to timestamp
   */
  public Timestamp longToTimestamp(long ts) {
    return new Timestamp(ts);
  }
}
