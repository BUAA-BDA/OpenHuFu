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
  final static long MSFORDAY = 86400000L;
  final static long MSFORHOUR = 3600000L;
  public DateUtils() {}

  /**
   * convert date into integer
   */
  public int dateToInt(Date date) {
    calendar.setTime(date);
    return (int) (date.getTime() / MSFORHOUR);
  }

  /**
   * convert int to date
   */
  public Date intToDate(int dint) {
    return new Date(dint * MSFORHOUR);
  }

  /**
   * convert time to int
   */
  public int timeToInt(Time time) {
    return (int) (time.getTime() % MSFORDAY);
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
   * convert long to timestamp
   */
  public Timestamp longToTimestamp(long ts) {
    return new Timestamp(ts);
  }
}
