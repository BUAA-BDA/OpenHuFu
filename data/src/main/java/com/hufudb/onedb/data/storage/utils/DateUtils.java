package com.hufudb.onedb.data.storage.utils;

import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.TimeZone;

/**
 * Simple Date Utils without consideration of time zone
 */
public class DateUtils {
  final static int DAY_MASK = 0x1F;
  final static int DAY_OFFSET = 5;
  final static int MONTH_MASK = 0xF;
  final static int MONTH_OFFSET = 4;
  final static long MSFORDAY = 86400000L;
  final static TimeZone timeZone = TimeZone.getDefault();

  private DateUtils() {}

  /**
   * convert date into integer
   *
   * year (23 bit) | month (4 bit) | day (5 bit)
   */
  public static int dateToInt(Date date) {
    Calendar calendar = Calendar.getInstance();
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
  public static Date intToDate(int dint) {
    int day = dint & DAY_MASK;
    dint = dint >> DAY_OFFSET;
    int month = dint & MONTH_MASK;
    dint = dint >> MONTH_OFFSET;
    Calendar calendar = Calendar.getInstance();
    calendar.clear();
    calendar.set(dint, month, day);
    return new Date(calendar.getTimeInMillis());
  }

  /**
   * convert time to int
   */
  public static int timeToInt(Time time) {
    return (int) (time.getTime() % MSFORDAY);
  }

  /**
   * precondition: calendar in UTC timeZone
   * convert calendar into time integer
   */
  public static int calendarToTimeInt(Calendar calendar) {
    return (int) ((calendar.getTimeInMillis() - (long) timeZone.getRawOffset()) % MSFORDAY);
  }

  /**
   * convert encoded int to time
   */
  public static Time intToTime(int t) {
    return new Time((long) t);
  }

  /**
   * convert timestamp to long
   */
  public static long timestampToLong(Timestamp ts) {
    return ts.getTime();
  }

  /**
   * convert calendar into time integer
   */
  public static long calendarToTimestampLong(Calendar calendar) {
    return calendar.getTimeInMillis() - (long) timeZone.getRawOffset();
  }

  /**
   * convert long to timestamp
   */
  public static Timestamp longToTimestamp(long ts) {
    return new Timestamp(ts);
  }
}
