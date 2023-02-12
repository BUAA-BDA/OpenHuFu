package com.hufudb.openhufu.common.enums;

public enum TimeTerm {
  TOTAL_QUERY_TIME("Total Query Time"),
  LOCAL_QUERY_TIME("Local Query Time"),
  ENCRYPTION_TIME("Encryption Time"),
  DECRYPTION_TIME("Decryption Time"),

  ;

  TimeTerm(String term) {
    this.term = term;
  }
  public String term;
}
