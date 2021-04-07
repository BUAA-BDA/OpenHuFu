package tk.onedb.core.zk;

public class ZkConfig {
  public String servers;
  public String zkRoot;
  public String schemaName;
  public String user;
  public String passwd;

  public boolean valid() {
    return servers != null && zkRoot != null && schemaName != null && user != null && passwd != null;
  }

  public byte[] getDigest() {
    return String.format("%s:%s", user, passwd).getBytes();
  }
}
