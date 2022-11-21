package com.hufudb.onedb.backend.controller;

import com.hufudb.onedb.backend.service.OwnerInfoService;
import com.hufudb.onedb.backend.utils.Page;
import com.hufudb.onedb.backend.utils.PageUtils;
import com.hufudb.onedb.backend.entity.request.RecordRequest;
import com.hufudb.onedb.backend.utils.TestPing;
import com.hufudb.onedb.persistence.entity.OwnerInfo;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import com.hufudb.onedb.backend.service.SqlRecordService;
import com.hufudb.onedb.backend.entity.response.PojoResultSet;
import com.hufudb.onedb.backend.entity.request.Request;
import com.hufudb.onedb.core.table.GlobalTableConfig;
import com.hufudb.onedb.core.table.OneDBTableSchema;
import com.hufudb.onedb.core.table.utils.PojoGlobalTableSchema;
import com.hufudb.onedb.data.schema.TableSchema;
import com.hufudb.onedb.data.schema.utils.PojoTableSchema;
import com.hufudb.onedb.user.OneDB;
import javax.annotation.Resource;
import org.apache.commons.lang3.StringUtils;
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

  @Resource
  private SqlRecordService sqlRecordService;

  @Resource
  OwnerInfoService ownerInfoService;
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

  @PostMapping("/owner/searchowner")
  Page<OwnerInfo> query(@RequestBody RecordRequest request) {
//    String context = request.context == null ? ".*" : request.context;
//    String status = request.status == null ? ".*" : request.status;
//    String order = "id ASC";
//    return PageUtils.getPage(()->ownerInfoService.selectOwner(context, status), pageId, pageSize, order);
    Set<String> endpoints = onedb.getEndpoints();
    List<OwnerInfo> owners = new ArrayList<>();
    Long id = 1L;
    for (String endpoint : endpoints) {
      OwnerInfo ownerInfo = new OwnerInfo();
      ownerInfo.setId(id++);
      ownerInfo.setAddress(endpoint);
      Long tableNum = Long.valueOf(onedb.getOwnerTableSchema(endpoint).size());
      ownerInfo.setTableNum(tableNum);
      ownerInfo.setStatus(TestPing.alive(endpoint) ? "connected" : "disconneted");
      if ((request.context != null && endpoint.indexOf(request.context) == -1) ||
          (StringUtils.isNotBlank(request.status) && !ownerInfo.getStatus().equals(request.status))) {
        continue;
      }
      owners.add(ownerInfo);
    }
    return PageUtils.getPage(owners, request.pageId, request.pageSize);
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
    Long id = sqlRecordService.insertRecord(request.value, "root");
    ResultSet rs = onedb.executeQuery(request.value);
    String status = rs != null ? "Succeed" : "Failed";
    sqlRecordService.updateStatus(id, status);
    return PojoResultSet.fromResultSet(rs);
  }
}
