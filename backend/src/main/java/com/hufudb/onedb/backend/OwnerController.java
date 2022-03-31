package com.hufudb.onedb.backend;

import java.util.List;

import com.hufudb.onedb.core.data.utils.POJOLocalTableInfo;
import com.hufudb.onedb.core.data.utils.POJOPublishedTableInfo;
import com.hufudb.onedb.server.OwnerService;

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
@ConditionalOnProperty(
    name = {"owner.db.enable"},
    havingValue = "true")
public class OwnerController {
  private static final Logger LOG = LoggerFactory.getLogger(OwnerController.class);
  private final OwnerService service;

  OwnerController(OwnerService service) {
    this.service = service;
  }

  @GetMapping("/owner/localtables")
  List<POJOLocalTableInfo> getLocalTableInfos() {
    return POJOLocalTableInfo.from(service.getAllLocalTable());
  }

  @GetMapping("/owner/publishedtables")
  List<POJOPublishedTableInfo> getPublishedTableInfos() {
    return POJOPublishedTableInfo.from(service.getAllPublishedTable());
  }

  @PostMapping("/owner/publishedtables")
  boolean addVirtualTable(@RequestBody POJOPublishedTableInfo alias) {
    return service.addPublishedTable(alias);
  }

  @DeleteMapping("/owner/publishedtables/{name}")
  void dropVirtualTable(@PathVariable String name) {
    service.dropPublishedTable(name);
  }
}
