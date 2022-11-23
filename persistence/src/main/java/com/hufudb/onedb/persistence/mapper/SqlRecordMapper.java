package com.hufudb.onedb.persistence.mapper;

import com.hufudb.onedb.persistence.entity.SqlRecord;
import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import org.apache.ibatis.annotations.Mapper;

import java.util.List;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;
import org.apache.ibatis.annotations.Update;

/**
 * <p>
 * Mapper
 * </p>
 *
 * @author qlh
 * @since 2022-10-21
 */
@Mapper
public interface SqlRecordMapper extends BaseMapper<SqlRecord> {

  @Select("SELECT * FROM sql_record WHERE context RLIKE #{context} AND status RLIKE #{status}")
  List<SqlRecord> selectRecord(@Param("context") String context, @Param("status") String status);

  @Update("UPDATE sql_record SET status = #{status} where id = #{id}")
  void updateStatus(@Param("id") Long id, @Param("status") String status);
}
