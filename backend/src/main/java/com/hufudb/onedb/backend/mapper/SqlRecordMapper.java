package com.hufudb.onedb.backend.mapper;

import com.hufudb.onedb.backend.entity.SqlRecord;
import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import org.apache.ibatis.annotations.Mapper;

import java.util.List;

/**
 * <p>
 *  Mapper
 * </p>
 *
 * @author qlh
 * @since 2022-10-21
 */
@Mapper
public interface SqlRecordMapper extends BaseMapper<SqlRecord> {
    List<SqlRecord> selectRecord(String context, String status);

    Long insertRecord(SqlRecord sqlRecord);
    void updateStatus(Long id, String status);
}
