package com.hufudb.onedb.backend;

import java.util.List;
import java.util.Set;

import com.hufudb.onedb.core.data.TableInfo;
import com.hufudb.onedb.core.data.utils.POJOGlobalTableInfo;
import com.hufudb.onedb.core.data.utils.POJOLocalTableInfo;
import com.hufudb.onedb.core.data.utils.POJOResultSet;
import com.hufudb.onedb.core.table.OneDBTableInfo;
import com.hufudb.onedb.core.table.TableMeta;

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
  List<POJOLocalTableInfo> getAllLocalTabelInfo(@PathVariable String endpoint) {
    List<TableInfo> infos = clientService.getDBTableInfo(endpoint);
    LOG.info("get local table {} from endpoint {}", infos, endpoint);
    return POJOLocalTableInfo.from(infos);
  }

  // for global tables
  @GetMapping("/user/globaltables")
  List<POJOGlobalTableInfo> getAllGlobalTableInfo() {
    List<OneDBTableInfo> infos = clientService.getAllOneDBTableInfo();
    LOG.info("get global table {}", infos);
    return POJOGlobalTableInfo.from(infos);
  }

  @GetMapping("/user/globaltables/{name}")
  POJOGlobalTableInfo getGlobalTableInfo(@PathVariable String name) {
    return POJOGlobalTableInfo.from(clientService.getOneDBTableInfo(name));
  }

  @PostMapping("/user/globaltables")
  boolean addGlobalTable(@RequestBody TableMeta meta) {
    return clientService.createOneDBTable(meta);
  }

  @DeleteMapping("/user/globaltables/{name}")
  void dropGlobalTable(@PathVariable String name) {
    clientService.dropOneDBTable(name);
  }

  @PostMapping("/user/query")
  POJOResultSet query(@RequestBody String sql) {
    return POJOResultSet.fromResultSet(clientService.executeQuery(sql));
  }
}
