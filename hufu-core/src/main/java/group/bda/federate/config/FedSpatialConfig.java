package group.bda.federate.config;

public final class FedSpatialConfig {
  public static final int CLIENT_THREAD_NUM = 50; // client side thread pool size
  public static final int SERVER_THREAD_NUM = 50; // server side thread pool size
  public static final double RANDOM_SET_SCALE = 0.5; // random data set size / origin data set size
  public static final int RANDOM_SET_OFFSET = 10; // random data set size / origin data set size
  public static final long TIME_OUT = 60000; // time out when waiting for response in ms
  public static final int RETRY = 100; // retry times to concurrentBuffer
  public static final int LOCK_NUMBER = 4; // lock number in concurrentBuffer
  public static final long DATA_TIME_OUT = 100000; // time out when waiting for response in ms
  public static final int SET_UNION_DIVIDE = 2;
  public static final boolean USE_DP = true; // dp optimization for knn
  public static final double EPS_DP = 1.0; // dp epsilon
  public static final double SD_DP = 0.5; // dp sd
  public static final double KNN_RADIUS = 1.0; // predefine radius of kNN
}
