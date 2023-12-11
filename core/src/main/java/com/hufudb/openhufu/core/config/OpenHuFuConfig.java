package com.hufudb.openhufu.core.config;

public class OpenHuFuConfig {
  public static final int CLIENT_THREAD_NUM = 20; // client side thread pool size
  public static final int SERVER_THREAD_NUM = 4; // server side thread pool size
  public static final long RPC_TIME_OUT = 60000; // time out when waiting for response in ms
  public static final int ZK_TIME_OUT = 6000; // time out of zk
}
