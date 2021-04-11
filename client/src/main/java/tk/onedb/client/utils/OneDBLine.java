package tk.onedb.client.utils;

import java.io.IOException;
import java.io.InputStream;

import sqlline.BuiltInProperty;
import sqlline.SqlLine;
import sqlline.SqlLineOpts;
import tk.onedb.core.sql.schema.OneDBSchema;

public class OneDBLine extends SqlLine {
  public static Status start(String[] args, InputStream inputStream, boolean saveHistory) throws IOException {
    OneDBLine onedbline = new OneDBLine();
    onedbline.getOpts().set(BuiltInProperty.PROMPT, "onedb>");
    onedbline.getOpts().set(BuiltInProperty.ISOLATION, "TRANSACTION_NONE");
    Status status = onedbline.begin(args, inputStream, saveHistory);
    if (!Boolean.getBoolean(SqlLineOpts.PROPERTY_NAME_EXIT)) {
      System.exit(status.ordinal());
    }
    return status;
  }

}
