package tk.onedb.core.data;

import com.google.gson.annotations.SerializedName;

public enum Level {
  @SerializedName("hide") // columns marked as hide are invisible to client and other dataServers
  HIDE,
  @SerializedName("public") // public columns can be scanned directly
  PUBLIC,
  @SerializedName("protected") // protected columns can be scanned but data source information will be hidden from client and other dataServers
  PROTECTED,
  @SerializedName("private") // can't scan private columns directly, only statistics including COUNT, SUM, AVG can use on these columns
  PRIVATE;

  public static Level of(int ordinal) {
    return values()[ordinal];
  }
}