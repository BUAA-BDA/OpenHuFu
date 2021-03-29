package tk.onedb.core.config;

public class OneDBConfig {
  public static final int CLIENT_THREAD_NUM = 4; // client side thread pool size
  public static final int SERVER_THREAD_NUM = 4; // server side thread pool size
  public static final long TIME_OUT = 60000; // time out when waiting for response in ms
}
