package com.hufudb.onedb.core.sql.schema;

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
  public static final OneDBSchemaFactory INSTANCE = new OneDBSchemaFactory();
  private static final Logger LOG = LoggerFactory.getLogger(OneDBSchemaFactory.class);

  private OneDBSchemaFactory() {}

  @Override
  public Schema create(SchemaPlus parentSchema, String name, Map<String, Object> operand) {
    List<OwnerInfo> owners = new ArrayList<>();
    List<Map<String, Object>> tableObjs = new ArrayList<>();
    int userId = 0;
    if (operand.containsKey("endpoints")) {
      for (Object endpoint : (List<Object>) operand.get("endpoints")) {
        owners.add(new OwnerInfo((String) endpoint, null));
      }
    } else if (operand.containsKey("owners")) {
      for (Map<String, Object> owner : (List<Map<String, Object>>) operand.get("owners")) {
        String endpoint = (String) owner.get("endpoint");
        String trustCertPath = (String) owner.get("trustcertpath");
        owners.add(new OwnerInfo(endpoint, trustCertPath));
      }
    }
    if (operand.containsKey("userid")) {
      userId = (Integer) operand.get("userid");
    }
    if (operand.containsKey("tables")) {
      tableObjs.addAll((List) operand.get("tables"));
    }
    ZkConfig zkConfig = new ZkConfig();
    zkConfig.servers = (String) operand.get("zookeeper");
    zkConfig.schemaName = (String) operand.get("schema");
    zkConfig.zkRoot = (String) operand.get("zkroot");
    zkConfig.user = (String) operand.get("user");
    zkConfig.passwd = (String) operand.get("passwd");
    if (zkConfig.valid()) {
      LOG.info("Use Zk");
      return new OneDBSchema(tableObjs, parentSchema, zkConfig);
    } else {
      LOG.info("Use model");
      return new OneDBSchema(owners, tableObjs, parentSchema, userId);
    }
  }

  static class OwnerInfo {
    String endpoint;
    String trustCertPath;

    OwnerInfo() {}

    OwnerInfo(String endpoint, String trustCertPath) {
      this.endpoint = endpoint;
      this.trustCertPath = trustCertPath;
    }

    public String getEndpoint() {
      return endpoint;
    }

    public String getTrustCertPath() {
      return trustCertPath;
    }
  }
}
