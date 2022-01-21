package com.hufudb.onedb.backend;

import java.util.List;
import java.util.Set;

import com.hufudb.onedb.backend.utils.SimpleGlobalTableInfo;
import com.hufudb.onedb.backend.utils.SimpleLocalTableInfo;
import com.hufudb.onedb.backend.utils.SimpleResultSet;
import com.hufudb.onedb.core.data.TableInfo;
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
public class ClientController {
  private static final Logger LOG = LoggerFactory.getLogger(ClientController.class);
  private final ClientService clientService;

  ClientController(ClientService service) {
      this.clientService = service;
  }

  // for endpoints
  @GetMapping("/client/endpoints")
  Set<String> getEndpoints() {
    return clientService.getEndpoints();
  }

  @PostMapping("/client/endpoints")
  boolean addEndpoint(@RequestBody String endpoint) {
    return clientService.addDB(endpoint);
  }

  @DeleteMapping("/client/endpoints/{endpoint}")
  void delEndpoint(@PathVariable String endpoint) {
    clientService.dropDB(endpoint);
  }

  @GetMapping("/client/endpoints/{endpoint}")
  List<SimpleLocalTableInfo> getAllLocalTabelInfo(@PathVariable String endpoint) {
    List<TableInfo> infos = clientService.getDBTableInfo(endpoint);
    LOG.info("get local table {} from endpoint {}", infos, endpoint);
    return SimpleLocalTableInfo.from(infos);
  }

  // for global tables
  @GetMapping("/client/globaltables")
  List<SimpleGlobalTableInfo> getAllGlobalTableInfo() {
    List<OneDBTableInfo> infos = clientService.getAllOneDBTableInfo();
    LOG.info("get global table {}", infos);
    return SimpleGlobalTableInfo.from(infos);
  }

  @GetMapping("/client/globaltables/{name}")
  SimpleGlobalTableInfo getGlobalTableInfo(@PathVariable String name) {
    return SimpleGlobalTableInfo.from(clientService.getOneDBTableInfo(name));
  }

  @PostMapping("/client/globaltables")
  boolean addGlobalTable(@RequestBody TableMeta meta) {
    return clientService.createOneDBTable(meta);
  }

  @DeleteMapping("/client/globaltables/{name}")
  void dropGlobalTable(@PathVariable String name) {
    clientService.dropOneDBTable(name);
  }

  @PostMapping("/client/query")
  SimpleResultSet query(@RequestBody String sql) {
    return SimpleResultSet.fromResultSet(clientService.executeQuery(sql));
  }
}