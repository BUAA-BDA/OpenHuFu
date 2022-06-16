package com.hufudb.onedb.mpc.secretsharing;

import java.security.PublicKey;
import java.util.List;
import com.hufudb.onedb.mpc.ProtocolType;
import com.hufudb.onedb.mpc.RpcProtocolExecutor;
import com.hufudb.onedb.rpc.Rpc;

/**
 * (n, n) secret sharing implementation
 */
public class SecretSharing extends RpcProtocolExecutor {

  public SecretSharing(Rpc rpc) {
    super(rpc, ProtocolType.SS);
  }

  @Override
  public Object run(long taskId, List<Integer> parties, Object... args) {
    return null;
  }
}
