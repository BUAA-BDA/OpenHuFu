package com.hufudb.onedb.backend.controller;


import com.hufudb.onedb.persistence.entity.SqlRecord;
import com.hufudb.onedb.backend.service.SqlRecordService;
import com.hufudb.onedb.backend.utils.Page;
import com.hufudb.onedb.backend.utils.PageUtils;
import com.hufudb.onedb.backend.entity.request.RecordRequest;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;

import org.springframework.web.bind.annotation.RestController;

/**
 * <p>
 *  Front Controller
 * </p>
 *
 * @author qlh
 * @since 2022-10-21
 */
@RestController
@RequestMapping("/sqlRecord")
public class SqlRecordController {

    @Autowired
    SqlRecordService sqlRecordService;

    @PostMapping("/query")
    Page<SqlRecord> query(@RequestBody RecordRequest request) {
        int pageId = request.pageId;
        int pageSize = request.pageSize;
        String context = request.context == null ? ".*" : request.context;
        String status = request.status == null ? ".*" : request.status;
        String order = "id DESC";
        return PageUtils.getPage(()->sqlRecordService.selectRecord(context, status), pageId, pageSize, order);
    }
}
