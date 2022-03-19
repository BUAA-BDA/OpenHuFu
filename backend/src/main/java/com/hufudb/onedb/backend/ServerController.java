package com.hufudb.onedb.backend;

import java.util.List;

import com.hufudb.onedb.backend.utils.SimpleLocalTableInfo;
import com.hufudb.onedb.core.data.AliasTableInfo;
import com.hufudb.onedb.server.DBService;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

@RestController
@ConditionalOnProperty(name = { "owner.db.enable" }, havingValue = "true")
public class ServerController {
  private static final Logger LOG = LoggerFactory.getLogger(ServerController.class);
  private final DBService service;

  ServerController(DBService service) {
    this.service = service;
  }

  @GetMapping("/server/localtables")
  List<SimpleLocalTableInfo> getLocalTableInfos() {
    return SimpleLocalTableInfo.from(service.getAllLocalTable());
  }

  @GetMapping("/server/virtualtables")
  List<SimpleLocalTableInfo> getVirtualTableInfos() {
    return SimpleLocalTableInfo.from(service.getAllPublishedTable());
  }

  @PostMapping("/server/virtualtables")
  boolean addVirtualTable(@RequestBody AliasTableInfo alias) {
    return service.addPublishedTable(alias);
  }

  @DeleteMapping("/server/virtualtables/{name}")
  void dropVirtualTable(@PathVariable String name) {
    service.dropPublishedTable(name);
  }
}
