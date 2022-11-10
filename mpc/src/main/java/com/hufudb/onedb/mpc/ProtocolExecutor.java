package com.hufudb.onedb.mpc;

import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public interface ProtocolExecutor {
  static final Logger LOG = LoggerFactory.getLogger(ProtocolExecutor.class);

  ProtocolType getProtocolType();

  int getProtocolTypeId();

  int getOwnId();

  /**
   * @param taskId
   * @param parties: all parties involved in this protocol
   * @param args
   * @return
   */
  Object run(long taskId, List<Integer> parties, Object... args) throws ProtocolException;
}
