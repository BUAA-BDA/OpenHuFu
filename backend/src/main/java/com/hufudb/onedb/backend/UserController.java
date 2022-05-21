package com.hufudb.onedb.backend;

import java.util.List;
import java.util.Set;
import com.hufudb.onedb.OneDB;
import com.hufudb.onedb.backend.utils.Request;
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
  private final OneDB onedb;

  UserController(OneDB service) {
    this.onedb = service;
  }

  // for endpoints
  @GetMapping("/user/endpoints")
  Set<String> getOwners() {
    return onedb.getEndpoints();
  }

  @PostMapping("/user/endpoints")
  boolean addOwner(@RequestBody Request request) {
    return onedb.addOwner(request.value);
  }

  @DeleteMapping("/user/endpoints/{endpoint}")
  void delOwner(@PathVariable String endpoint) {
    onedb.removeOwner(endpoint);
  }

  @GetMapping("/user/endpoints/{endpoint}")
  List<PojoTableSchema> getAllLocalTableSchema(@PathVariable Request request) {
    List<TableSchema> schemas = onedb.getOwnerTableSchema(request.value);
    LOG.info("get local table {} from owner {}", schemas, request.value);
    return PojoTableSchema.from(schemas);
  }

  // for global tables
  @GetMapping("/user/globaltables")
  List<PojoGlobalTableSchema> getAllGlobalTableSchema() {
    List<OneDBTableSchema> schemas = onedb.getAllOneDBTableSchema();
    LOG.info("get global table {}", schemas);
    return PojoGlobalTableSchema.from(schemas);
  }

  @GetMapping("/user/globaltables/{name}")
  PojoGlobalTableSchema getGlobalTableSchema(@PathVariable Request request) {
    return PojoGlobalTableSchema.from(onedb.getOneDBTableSchema(request.value));
  }

  @PostMapping("/user/globaltables")
  boolean addGlobalTable(@RequestBody GlobalTableConfig config) {
    return onedb.createOneDBTable(config);
  }

  @DeleteMapping("/user/globaltables/{name}")
  void dropGlobalTable(@PathVariable Request request) {
    onedb.dropOneDBTable(request.value);
  }

  @PostMapping("/user/query")
  PojoResultSet query(@RequestBody Request request) {
    return PojoResultSet.fromResultSet(onedb.executeQuery(request.value));
  }
}
