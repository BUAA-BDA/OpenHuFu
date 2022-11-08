package com.hufudb.onedb.backend.mapper;

import java.util.List;

import org.apache.ibatis.annotations.Mapper;

import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import com.hufudb.onedb.backend.entity.OwnerInfo;

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
    List<OwnerInfo> selectOwner(String context, String status);

    void updateStatus(Long id, String status);
}
