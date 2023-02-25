package com.hufudb.openhufu.common.exception;

import java.text.MessageFormat;

public class ErrorFormatter {

  private ErrorFormatter() {}

  static String format(ErrorCode error, Object... args) {
    if (error == null) {
      throw new IllegalArgumentException("BaymaxError cannot be null");
    }

    if (error.getDesc() == null) {
      return String.valueOf(error.getErrorCode());
    } else {
      if (args == null || args.length == 0) {
        return error.getDesc();
      } else {
        MessageFormat format = new MessageFormat(error.getDesc());
        return format.format(args);
      }
    }
  }
}
