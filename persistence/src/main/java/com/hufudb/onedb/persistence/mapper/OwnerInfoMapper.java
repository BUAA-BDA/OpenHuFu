package com.hufudb.onedb.persistence.mapper;

import java.util.List;

import org.apache.ibatis.annotations.Mapper;

import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import com.hufudb.onedb.persistence.entity.OwnerInfo;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;
import org.apache.ibatis.annotations.Update;

/**
 * <p>
 *
 * </p>
 *
 * @author fzh
 * @since 2022-11-4
 */
@Mapper
public interface OwnerInfoMapper extends BaseMapper<OwnerInfo> {

  @Select("SELECT * FROM owner WHERE address RLIKE #{context} AND status RLIKE #{status}")
  List<OwnerInfo> selectOwner(@Param("context") String context, @Param("status") String status);

  @Update("UPDATE owner SET status = #{status} where id = #{id}")
  void updateStatus(@Param("id") Long id, @Param("status") String status);
}
