package com.hufudb.openhufu.common.exception;

import java.util.HashMap;
import java.util.Map;

/**
 * @author yang.song
 * @date 2/7/23 8:28 PM
 */
public enum ErrorCode {

  INTERNAL_SERVER_ERROR(10001, "internal server error"),

  // config error
  OPENHUFU_ROOT_ENV_NOT_SET(20001, "environment variable OPENHUFU_ROOT not set"),
  ADAPTER_FOLDER_NOT_FOUND(20002, "adapter folder:{} not found"),

  IMPLEMENTOR_CONFIG_MISSING(20031, "implementor config: {} missing"),
  IMPLEMENTOR_CLASS_NOT_FOUND(20032, "implementor class: {} not found"),
  IMPLEMENTOR_CONSTRUCTOR_NOT_FOUND(20033, "implementor: {} constrictor not found"),
  IMPLEMENTOR_CREATE_FAILED(20034, "implementor: {} create failed"),
  CSV_URL_NOT_FOUND(20035, "csv url: {} not found"),
  CSV_URL_IS_NOT_FOLDER(20036, "csv url: {} is not folder"),
  IMPLEMENTOR_CONFIG_FILE_PATH_NOT_SET(20037, "implementor config file path not set"),
  IMPLEMENTOR_CONFIG_FILE_NOT_FOUND(20038, "implementor config file: {} not found"),


  // udf error
  UDF_LOAD_FAILED(30001, "udf: {} load failed"),
  UDF_CLASS_LOAD_FAILED(30001, "udf class: {} load failed"),

  FUNCTION_PARAMS_SIZE_ERROR(30002, "function {} parameters size error, expects {} , given {}"),
  FUNCTION_PARAMS_TYPE_ERROR(30002, "function {} type error, expects {}"),
  // data type error
  DATA_TYPE_NOT_SUPPORT(40001, "data type: {} not support"),

  SETUP_FAILED(90001, "setup failed"),
  ;


  private final int errorCode;
  private final String desc;

  private static final Map<Integer, ErrorCode> errorCodeMap;

  static {
    errorCodeMap = new HashMap<>();
    for (ErrorCode error : ErrorCode.values()) {
      errorCodeMap.put(error.getErrorCode(), error);
    }
  }

  ErrorCode(int errorCode, String desc) {
    this.errorCode = errorCode;
    this.desc = desc;
  }

  public int getErrorCode() {
    return errorCode;
  }

  public String getDesc() {
    return desc;
  }

  public static ErrorCode getVal(int errorCode) {
    return errorCodeMap.get(errorCode);
  }

}
