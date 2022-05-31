package com.hufudb.onedb.mpc;

import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public interface ProtocolExecutor {
  static final Logger LOG = LoggerFactory.getLogger(ProtocolExecutor.class);

  ProtocolType getProtocolType();

  int getProtocolTypeId();

  int getOwnId();

  abstract List<byte[]> run(long taskId, List<Integer> parties, List<byte[]> inputData, Object... args);
}
