package com.hufudb.onedb.core.table.utils;

import com.google.gson.annotations.SerializedName;

public class PojoOwnerInfo {
  @SerializedName("endpoint")
  public String endpoint;
  @SerializedName(value = "trustCertPath", alternate = {"trustcertpath", "trust_cert_path"})
  public String trustCertPath;

  public PojoOwnerInfo() {}

  public PojoOwnerInfo(String endpoint, String trustCertPath) {
    this.endpoint = endpoint;
    this.trustCertPath = trustCertPath;
  }
}
