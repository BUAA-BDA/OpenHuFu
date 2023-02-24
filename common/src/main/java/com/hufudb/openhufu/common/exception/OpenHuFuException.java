package com.hufudb.openhufu.common.exception;

import java.util.Arrays;

/**
 * @author yang.song
 * @date 2/7/23 8:28 PM
 */
public class OpenHuFuException extends RuntimeException implements BaseException {

  private static final long serialVersionUID = 6339122850757175543L;
  private final ErrorCode errorCode;

  private final Object[] arguments;

  public OpenHuFuException(Throwable cause, ErrorCode errorCode, Object... arguments) {
    super(cause);
    this.errorCode = errorCode;
    this.arguments = arguments;
  }

  public OpenHuFuException(ErrorCode errorCode, Object... arguments) {
    super(new Throwable());
    this.errorCode = errorCode;
    this.arguments = arguments;
  }

  public ErrorCode getErrorCode() {
    return errorCode;
  }

  public Object[] getArguments() {
    return arguments;
  }

  @Override
  public String toString() {
    return "OpenHuFuException{" +
        "errorCode=" + errorCode +
        ", arguments=" + Arrays.toString(arguments) +
        '}';
  }

  @Override
  public synchronized Throwable fillInStackTrace() {
    return this;
  }

}
