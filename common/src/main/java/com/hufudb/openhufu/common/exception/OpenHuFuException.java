package com.hufudb.openhufu.common.exception;

import java.util.Arrays;
import java.util.Optional;

/**
 * @author yang.song
 * @date 2/7/23 8:28 PM
 */
public class OpenHuFuException extends RuntimeException implements BaseException {

  private static final long serialVersionUID = 6339122850757175543L;
  private final ErrorCode errorCode;

  private final Object[] arguments;

  public OpenHuFuException(Throwable cause, ErrorCode errorCode, Object... arguments) {
    super(ErrorFormatter.format(errorCode, arguments), cause);
    this.errorCode = errorCode;
    this.arguments = arguments;
  }

  public OpenHuFuException(ErrorCode errorCode, Object... arguments) {
    super(ErrorFormatter.format(errorCode, arguments));
    this.errorCode = errorCode;
    this.arguments = arguments;
  }

  public ErrorCode getErrorCode() {
    return errorCode;
  }

  public Object[] getArguments() {
    return arguments;
  }

}
