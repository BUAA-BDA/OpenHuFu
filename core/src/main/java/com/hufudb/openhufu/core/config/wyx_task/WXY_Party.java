package com.hufudb.openhufu.core.config.wyx_task;

public class WXY_Party {
  private String partyID;
  private ServiceInfo serviceInfo;

  public String getPartyID() {
    return partyID;
  }

  public String getIp() {
    return serviceInfo.ip;
  }

  public String getPort() {
    return serviceInfo.port;
  }

  public String getEndpoint() {
    return getIp() + ":" + getPort();
  }
  private class ServiceInfo {
    String ip;
    String port;
  }
}
