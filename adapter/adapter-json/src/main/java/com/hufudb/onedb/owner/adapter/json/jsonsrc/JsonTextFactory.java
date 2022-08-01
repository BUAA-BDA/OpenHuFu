package com.hufudb.onedb.owner.adapter.json.jsonsrc;

import com.hufudb.onedb.owner.adapter.AdapterConfig;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Stream;

public class JsonTextFactory implements JsonSrcFactory {
  HashMap<String, JsonText> table2JsonSrc = null;

  public JsonTextFactory() {

  }

  @Override
  public List<String> getTableNames(AdapterConfig config) {
    if (table2JsonSrc == null) {
      table2JsonSrc = new HashMap<>();
      File base = new File(config.url);
      if (base.isDirectory()) {
        try (Stream<Path> stream = Files.list(base.toPath())) {
          stream.filter(file -> !Files.isDirectory(file))
                  .filter(file -> file.toString().endsWith(".json")).forEach(file -> {
            String fileName = file.getFileName().toString();
            String tableName = fileName.substring(0, fileName.length() - 5);
            JsonText jsonText = new JsonText(file);
            table2JsonSrc.put(tableName, jsonText);
          });
        } catch (IOException e) {
          e.printStackTrace();
        }
      } else if (base.getName().endsWith(".json")) {
        String fileName = base.getName();
        String tableName = fileName.substring(0, fileName.length() - 5);
        JsonText jsonText = new JsonText(base.toPath());
        table2JsonSrc.put(tableName, jsonText);
      }
    }
    return new ArrayList<>(table2JsonSrc.keySet());
  }

  @Override
  public JsonSrc createJsonSrc(String tableName) {
    return table2JsonSrc.get(tableName);
  }

  @Override
  public String getType() {
    return "localFile";
  }
}
