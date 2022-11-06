package com.hufudb.onedb.backend.controller;

import java.util.List;

import com.hufudb.onedb.backend.utils.Page;
import com.hufudb.onedb.backend.utils.PageUtils;
import com.hufudb.onedb.backend.utils.RecordRequest;
import com.hufudb.onedb.data.schema.utils.PojoPublishedTableSchema;
import com.hufudb.onedb.data.schema.utils.PojoTableSchema;
import com.hufudb.onedb.owner.OwnerServer;
import com.hufudb.onedb.owner.OwnerService;
import com.hufudb.onedb.backend.entity.OwnerInfo;
import com.hufudb.onedb.backend.service.OwnerInfoService;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

@RestController
@ConditionalOnProperty(
    name = {"owner.enable"},
    havingValue = "true")
public class OwnerController {
  private static final Logger LOG = LoggerFactory.getLogger(OwnerController.class);
  private final OwnerServer server;
  private final OwnerService service;
  
  @Autowired
  OwnerInfoService ownerInfoService;

  OwnerController(OwnerServer server) {
    this.server = server;
    this.service = server.getService();
  }

  @GetMapping("/owner/localtables")
  List<PojoTableSchema> getLocalTableInfos() {
    return PojoTableSchema.from(service.getAllLocalTable());
  }

  @GetMapping("/owner/publishedtables")
  List<PojoPublishedTableSchema> getPublishedTableInfos() {
    return PojoPublishedTableSchema.from(service.getAllPublishedTable());
  }

  @PostMapping("/owner/publishedtables")
  boolean addPublishedTable(@RequestBody PojoPublishedTableSchema schema) {
    return service.addPublishedTable(schema);
  }

  @DeleteMapping("/owner/publishedtables/{name}")
  void dropPublishedTable(@PathVariable String name) {
    service.dropPublishedTable(name);
  }

  @PostMapping("/owner/searchowner") 
  Page<OwnerInfo> query(@RequestBody RecordRequest request) {
    int pageId = request.pageId;
    int pageSize = request.pageSize;
    String context = request.context == null ? ".*" : request.context;
    String status = request.status == null ? ".*" : request.status;
    String order = "id ASC";
    return PageUtils.getPage(()->ownerInfoService.selectOwner(context, status), pageId, pageSize, order);
  }
  
}
