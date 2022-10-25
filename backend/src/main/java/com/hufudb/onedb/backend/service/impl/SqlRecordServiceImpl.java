package com.hufudb.onedb.backend.service.impl;

import com.hufudb.onedb.backend.entity.SqlRecord;
import com.hufudb.onedb.backend.mapper.SqlRecordMapper;
import com.hufudb.onedb.backend.service.SqlRecordService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hufudb.onedb.backend.utils.Page;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;

/**
 * <p>
 *  Service Implement
 * </p>
 *
 * @author qlh
 * @since 2022-10-21
 */
@Service
public class SqlRecordServiceImpl extends ServiceImpl<SqlRecordMapper, SqlRecord> implements SqlRecordService {

    @Autowired
    SqlRecordMapper sqlRecordMapper;

    @Override
    public List<SqlRecord> selectRecord(String context, String status) {
        return sqlRecordMapper.selectRecord(context, status);
    }

    @Override
    public Long insertRecord(String context, String userName) {
        SqlRecord sqlRecord = new SqlRecord();
        sqlRecord.setContext(context);
        sqlRecord.setUserName(userName);
        sqlRecordMapper.insertRecord(sqlRecord);
        return sqlRecord.getId();
    }

    @Override
    public void updateStatus(Long id, String status) {
        sqlRecordMapper.updateStatus(id, status);
    }
}
