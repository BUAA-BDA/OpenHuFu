package com.hufudb.onedb.backend;

import java.util.List;
import java.util.Set;
import com.hufudb.onedb.core.table.GlobalTableConfig;
import com.hufudb.onedb.core.table.OneDBTableSchema;
import com.hufudb.onedb.core.table.utils.PojoGlobalTableSchema;
import com.hufudb.onedb.data.schema.TableSchema;
import com.hufudb.onedb.data.schema.utils.PojoResultSet;
import com.hufudb.onedb.data.schema.utils.PojoTableSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class UserController {
  private static final Logger LOG = LoggerFactory.getLogger(UserController.class);
  private final UserService clientService;

  UserController(UserService service) {
    this.clientService = service;
  }

  // for endpoints
  @GetMapping("/user/endpoints")
  Set<String> getEndpoints() {
    return clientService.getEndpoints();
  }

  @PostMapping("/user/endpoints")
  boolean addEndpoint(@RequestBody String endpoint) {
    return clientService.addOwner(endpoint);
  }

  @DeleteMapping("/user/endpoints/{endpoint}")
  void delEndpoint(@PathVariable String endpoint) {
    clientService.removeOwner(endpoint);
  }

  @GetMapping("/user/endpoints/{endpoint}")
  List<PojoTableSchema> getAllLocalTabelInfo(@PathVariable String endpoint) {
    List<TableSchema> schemas = clientService.getOwnerTableSchema(endpoint);
    LOG.info("get local table {} from endpoint {}", schemas, endpoint);
    return PojoTableSchema.from(schemas);
  }

  // for global tables
  @GetMapping("/user/globaltables")
  List<PojoGlobalTableSchema> getAllGlobalTableInfo() {
    List<OneDBTableSchema> schemas = clientService.getAllOneDBTableSchema();
    LOG.info("get global table {}", schemas);
    return PojoGlobalTableSchema.from(schemas);
  }

  @GetMapping("/user/globaltables/{name}")
  PojoGlobalTableSchema getGlobalTableInfo(@PathVariable String name) {
    return PojoGlobalTableSchema.from(clientService.getOneDBTableSchema(name));
  }

  @PostMapping("/user/globaltables")
  boolean addGlobalTable(@RequestBody GlobalTableConfig config) {
    return clientService.createOneDBTable(config);
  }

  @DeleteMapping("/user/globaltables/{name}")
  void dropGlobalTable(@PathVariable String name) {
    clientService.dropOneDBTable(name);
  }

  @PostMapping("/user/query")
  PojoResultSet query(@RequestBody String sql) {
    return PojoResultSet.fromResultSet(clientService.executeQuery(sql));
  }
}
