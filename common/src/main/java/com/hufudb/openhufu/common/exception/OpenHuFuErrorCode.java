package com.hufudb.openhufu.common.exception;

import java.util.HashMap;
import java.util.Map;

/**
 * @author yang.song
 * @date 2/7/23 8:28 PM
 */
public enum OpenHuFuErrorCode implements BaseErrorCode {

  INTERNAL_SERVER_ERROR(10001),
  ;


  private final int errorCode;
  private static final Map<Integer, OpenHuFuErrorCode> errorCodeMap;

  static {
    errorCodeMap = new HashMap<>();
    for (OpenHuFuErrorCode error : OpenHuFuErrorCode.values()) {
      errorCodeMap.put(error.getErrorCode(), error);
    }
  }

  OpenHuFuErrorCode(int errorCode) {
    this.errorCode = errorCode;
  }

  @Override
  public int getErrorCode() {
    return errorCode;
  }

  public static OpenHuFuErrorCode getVal(int errorCode) {
    return errorCodeMap.get(errorCode);
  }

  @Override
  public String getMsgCode() {
    return "exception." + name();
  }
}
