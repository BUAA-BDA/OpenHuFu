package com.hufudb.onedb.backend.service;

import java.util.List;

import com.baomidou.mybatisplus.extension.service.IService;
import com.hufudb.onedb.persistence.entity.OwnerInfo;

/**
 * <p>
 *  Service
 * </p>
 *
 * @author fzh
 * @since 2022-11-4
 */
public interface OwnerInfoService extends IService<OwnerInfo>{
    List<OwnerInfo> selectOwner(String context, String status);

    void updateStatus(Long id, String status);
}
