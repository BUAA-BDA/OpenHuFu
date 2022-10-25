package com.hufudb.onedb.backend.service;

import com.hufudb.onedb.backend.entity.SqlRecord;
import com.baomidou.mybatisplus.extension.service.IService;

import java.util.List;

/**
 * <p>
 *  Service
 * </p>
 *
 * @author qlh
 * @since 2022-10-21
 */
public interface SqlRecordService extends IService<SqlRecord> {
    List<SqlRecord> selectRecord(String context, String status);

    Long insertRecord(String context, String userName);
    void updateStatus(Long id, String status);

}
