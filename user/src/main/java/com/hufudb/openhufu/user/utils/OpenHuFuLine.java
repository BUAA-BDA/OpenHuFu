package com.hufudb.openhufu.user.utils;

import java.io.IOException;
import java.io.InputStream;
import sqlline.BuiltInProperty;
import sqlline.SqlLine;
import sqlline.SqlLineOpts;

public class OpenHuFuLine extends SqlLine {
  private static String LOGO = String.join
  ("\n",
  "   ____                _____   ____  ",
  "  / __ \\              |  __ \\ |  _ \\ ",
  " | |  | | _ __    ___ | |  | || |_) |",
  " | |  | || '_ \\  / _ \\| |  | ||  _ < ",
  " | |__| || | | ||  __/| |__| || |_) |",
  "  \\____/ |_| |_| \\___||_____/ |____/ "
  );

  public static Status start(String[] args, InputStream inputStream, boolean saveHistory)
      throws IOException {
    System.out.println(LOGO);
    OpenHuFuLine openhufuline = new OpenHuFuLine();
    openhufuline.getOpts().set(BuiltInProperty.PROMPT, "openhufu>");
    openhufuline.getOpts().set(BuiltInProperty.ISOLATION, "TRANSACTION_NONE");
    Status status = openhufuline.begin(args, inputStream, saveHistory);
    if (!Boolean.getBoolean(SqlLineOpts.PROPERTY_NAME_EXIT)) {
      System.exit(status.ordinal());
    }
    return status;
  }
}
