package com.hufudb.onedb.user.utils;

import java.io.IOException;
import java.io.Reader;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import com.google.gson.Gson;
import com.hufudb.onedb.core.table.GlobalTableConfig;
import com.hufudb.onedb.core.table.LocalTableConfig;
import com.hufudb.onedb.core.table.utils.PojoOwnerInfo;

public class ModelGenerator {
  private static String modelTemplate = "inline:"
  + "{\n"
  + "  'version': '1.0',\n"
  + "  'defaultSchema': 'onedb',\n"
  + "  'schemas': [\n"
  + "    {\n"
  + "      'type': 'custom',\n"
  + "      'name': 'onedb',\n"
  + "      'factory': 'com.hufudb.onedb.core.sql.schema.OneDBSchemaFactory',\n"
  + "      'operand': {\n"
  + "        'owners': [\n%s\n]\n"
  + "      },\n"
  + "      'tables': [\n%s\n]\n"
  + "    }\n"
  + "  ]\n"
  + "}";
  private static String tableTemplate = "{\n"
  + "  'name': '%s',\n"
  + "  'factory': 'com.hufudb.onedb.core.table.OneDBTableFactory',\n"
  + "  'operand': {\n"
  + "    'components': [\n%s\n]\n"
  + "  }\n"
  + "}";

  static String generateOwner(List<PojoOwnerInfo> owners) {
    List<String> ownerStrs = new ArrayList<>();
    for (PojoOwnerInfo owner : owners) {
      ownerStrs.add(String.format("\n{\n'endpoint': '%s',\n'trustcertpath': '%s'\n}", owner.endpoint, owner.trustCertPath));
    }
    return String.join(",", ownerStrs);
  }

  static String generateTable(List<GlobalTableConfig> schemas) {
    List<String> tableStrs = new ArrayList<>();
    for (GlobalTableConfig schema : schemas) {
      List<String> components = new ArrayList<>();
      for (LocalTableConfig local : schema.localTables) {
        components.add(String.format("\n{\n'endpoint': '%s',\n'name': '%s'\n}", local.endpoint, local.localName));
      }
      tableStrs.add(String.format(tableTemplate, schema.tableName, String.join(",", components)));
    }
    return String.join(",", tableStrs);
  }

  static String generateModel(List<PojoOwnerInfo> owners, List<GlobalTableConfig> globalSchemas) {
    String ownerStr = generateOwner(owners);
    String tableStr = generateTable(globalSchemas);
    return String.format(modelTemplate, ownerStr, tableStr);
  }

  public static class Model {
    public List<PojoOwnerInfo> owners;
    public List<GlobalTableConfig> tables;
  }

  public static Model parseModel(String path) throws IOException {
    Gson gson = new Gson();
    Reader reader = Files.newBufferedReader(Paths.get(path));
    return gson.fromJson(reader, Model.class);
  }

  public static String loadUserConfig(String path) throws IOException {
    Model model = parseModel(path);
    return generateModel(model.owners, model.tables);
  }
}
