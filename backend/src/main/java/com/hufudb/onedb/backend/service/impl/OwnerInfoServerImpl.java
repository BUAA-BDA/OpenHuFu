package com.hufudb.onedb.backend.service.impl;

import java.util.List;

import javax.annotation.Resource;
import org.springframework.stereotype.Service;

import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hufudb.onedb.persistence.entity.OwnerInfo;
import com.hufudb.onedb.persistence.mapper.OwnerInfoMapper;
import com.hufudb.onedb.backend.service.OwnerInfoService;

/**
 * <p>
 *  Service
 * </p>
 *
 * @author fzh
 * @since 2022-11-4
 */
@Service
public class OwnerInfoServerImpl extends ServiceImpl<OwnerInfoMapper, OwnerInfo> implements OwnerInfoService{

    @Resource
    OwnerInfoMapper ownerMapper;

    @Override
    public List<OwnerInfo> selectOwner(String context, String status) {
        return ownerMapper.selectOwner(context, status);
    }

    @Override
    public void updateStatus(Long id, String status) {
        ownerMapper.updateStatus(id, status);
        return;
    }
    
}
