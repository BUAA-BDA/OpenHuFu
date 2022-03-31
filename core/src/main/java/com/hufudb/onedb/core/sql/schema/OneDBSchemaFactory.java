package com.hufudb.onedb.core.sql.schema;

import com.google.common.collect.ImmutableList;
import com.hufudb.onedb.core.zk.ZkConfig;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaFactory;
import org.apache.calcite.schema.SchemaPlus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OneDBSchemaFactory implements SchemaFactory {
  private static final Logger LOG = LoggerFactory.getLogger(OneDBSchemaFactory.class);

  public static final OneDBSchemaFactory INSTANCE = new OneDBSchemaFactory();

  private OneDBSchemaFactory() {}

  @Override
  public Schema create(SchemaPlus parentSchema, String name, Map<String, Object> operand) {
    // List<String> endpoints = (List<String>) operand.get("endpoints");
    List<String> endpoints = new ArrayList<>();
    List<Map<String, Object>> tables = new ArrayList<>();
    if (operand.containsKey("endpoints")) {
      endpoints.addAll((List) operand.get("endpoints"));
    }
    if (operand.containsKey("tables")) {
      tables.addAll((List) operand.get("tables"));
    }
    ZkConfig zkConfig = new ZkConfig();
    zkConfig.servers = (String) operand.get("zookeeper");
    zkConfig.schemaName = (String) operand.get("schema");
    zkConfig.zkRoot = (String) operand.get("zkroot");
    zkConfig.user = (String) operand.get("user");
    zkConfig.passwd = (String) operand.get("passwd");
    if (zkConfig.valid()) {
      LOG.info("Use Zk");
      return new OneDBSchema(tables, parentSchema, zkConfig);
    } else {
      LOG.info("Use model");
      return new OneDBSchema(endpoints, tables, parentSchema);
    }
  }

  public OneDBSchema create(SchemaPlus parentSchema, List<String> endpoints) {
    return new OneDBSchema(endpoints, ImmutableList.of(), parentSchema);
  }
}
