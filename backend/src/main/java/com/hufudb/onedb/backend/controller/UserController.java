package com.hufudb.onedb.backend.controller;

import java.util.List;
import java.util.Set;
import com.hufudb.onedb.backend.utils.PojoResultSet;
import com.hufudb.onedb.backend.utils.Request;
import com.hufudb.onedb.core.table.GlobalTableConfig;
import com.hufudb.onedb.core.table.OneDBTableSchema;
import com.hufudb.onedb.core.table.utils.PojoGlobalTableSchema;
import com.hufudb.onedb.data.schema.TableSchema;
import com.hufudb.onedb.data.schema.utils.PojoTableSchema;
import com.hufudb.onedb.user.OneDB;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
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

  @Value("${owner.enable:false}")
  private boolean hasOwner;

  UserController(OneDB service) {
    this.onedb = service;
  }

  // for alive test
  @GetMapping("/alive")
  boolean isAlive() {
    return hasOwner;
  }

  // for endpoints
  @GetMapping("/user/owners")
  Set<String> getOwners() {
    return onedb.getEndpoints();
  }

  @PostMapping("/user/owners")
  boolean addOwner(@RequestBody Request request) {
    LOG.info("Add owner: {}", request.value);
    return onedb.addOwner(request.value, "./cert/ca.pem");
  }

  @DeleteMapping("/user/owners/{endpoint}")
  void delOwner(@PathVariable String endpoint) {
    onedb.removeOwner(endpoint);
  }

  @GetMapping("/user/owners/{endpoint}")
  List<PojoTableSchema> getAllLocalTableSchema(@PathVariable String endpoint) {
    List<TableSchema> schemas = onedb.getOwnerTableSchema(endpoint);
    LOG.info("get local table {} from owner {}", schemas, endpoint);
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
  PojoGlobalTableSchema getGlobalTableSchema(@PathVariable String name) {
    return PojoGlobalTableSchema.from(onedb.getOneDBTableSchema(name));
  }

  @PostMapping("/user/globaltables")
  boolean addGlobalTable(@RequestBody GlobalTableConfig config) {
    return onedb.createOneDBTable(config);
  }

  @DeleteMapping("/user/globaltables/{name}")
  void dropGlobalTable(@PathVariable String name) {
    onedb.dropOneDBTable(name);
  }

  @PostMapping("/user/query")
  PojoResultSet query(@RequestBody Request request) {
    return PojoResultSet.fromResultSet(onedb.executeQuery(request.value));
  }
}
