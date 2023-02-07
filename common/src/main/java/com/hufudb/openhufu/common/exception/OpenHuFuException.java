package com.hufudb.openhufu.common.exception;

import java.io.Serializable;
import java.util.Arrays;

/**
 * @author yang.song
 * @date 2/7/23 8:28 PM
 */
public class OpenHuFuException extends RuntimeException implements BaseException {
  private static final long serialVersionUID = 6339122850757175543L;
  private final OpenHuFuErrorCode errorCode;

    private final Object[] arguments;

    public OpenHuFuException(Throwable cause, OpenHuFuErrorCode errorCode, Object... arguments) {
        super(cause);
        this.errorCode = errorCode;
        this.arguments = arguments;
    }

    public OpenHuFuException(OpenHuFuErrorCode errorCode, Object... arguments) {
        this.errorCode = errorCode;
        this.arguments = arguments;
    }

    public OpenHuFuErrorCode getErrorCode() {
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
    public Throwable fillInStackTrace() {
        return this;
    }

}
