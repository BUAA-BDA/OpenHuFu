package com.hufudb.openhufu.common.exception;

import java.util.HashMap;
import java.util.Map;

/**
 * @author yang.song
 * @date 2/7/23 8:28 PM
 */
public enum ErrorCode implements BaseErrorCode {

  INTERNAL_SERVER_ERROR(10001),

  // config error
  IMPLEMENTOR_CONFIG_MISSING(20001),
  IMPLEMENTOR_CLASS_NOT_FOUND(20002),
  IMPLEMENTOR_CONSTRUCTOR_NOT_FOUND(20003),
  IMPLEMENTOR_CREATE_FAILED(20004),

  CSV_URL_NOT_EXISTS(20005),
  CSV_URL_IS_NOT_FOLDER(20006),

  // udf error
  UDF_LOAD_FAILED(30001),

  SETUP_FAILED(90001),
  ;


  private final int errorCode;
  private static final Map<Integer, ErrorCode> errorCodeMap;

  static {
    errorCodeMap = new HashMap<>();
    for (ErrorCode error : ErrorCode.values()) {
      errorCodeMap.put(error.getErrorCode(), error);
    }
  }

  ErrorCode(int errorCode) {
    this.errorCode = errorCode;
  }

  @Override
  public int getErrorCode() {
    return errorCode;
  }

  public static ErrorCode getVal(int errorCode) {
    return errorCodeMap.get(errorCode);
  }

  @Override
  public String getMsgCode() {
    return "exception." + name();
  }
}
