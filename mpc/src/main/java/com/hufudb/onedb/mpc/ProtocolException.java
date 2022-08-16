package com.hufudb.onedb.mpc;

public class ProtocolException extends Exception {
  public ProtocolException(String message, Throwable cause) {
    super(message, cause);
  }

  public ProtocolException(String message) {
    super(message);
  }
}
