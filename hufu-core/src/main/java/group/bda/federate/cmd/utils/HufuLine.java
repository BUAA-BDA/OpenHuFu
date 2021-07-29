package group.bda.federate.cmd.utils;

import java.io.IOException;
import java.io.InputStream;

import sqlline.BuiltInProperty;
import sqlline.SqlLine;
import sqlline.SqlLineOpts;

public class HufuLine extends SqlLine {
  public static Status start(String[] args,
      InputStream inputStream,
      boolean saveHistory) throws IOException {
    SqlLine sqlline = new SqlLine();
    sqlline.getOpts().set(BuiltInProperty.PROMPT, "Hu-Fu>");
    sqlline.getOpts().set(BuiltInProperty.ISOLATION, "TRANSACTION_NONE");
    Status status = sqlline.begin(args, inputStream, saveHistory);
    if (!Boolean.getBoolean(SqlLineOpts.PROPERTY_NAME_EXIT)) {
      System.exit(status.ordinal());
    }
    return status;
  }
}
