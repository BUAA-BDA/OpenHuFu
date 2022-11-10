package com.hufudb.onedb.backend.config;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.tuple.Pair;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;

import com.hufudb.onedb.backend.service.OwnerInfoService;
import com.hufudb.onedb.backend.utils.TestPing;
import com.hufudb.onedb.backend.entity.OwnerInfo;

@Configuration
@EnableScheduling
public class Scheduler {
    
    @Autowired
    OwnerInfoService ownerInfoService;

    @Scheduled(cron = "* */5 * * * *")
    public void testAllOwner(){
        List<OwnerInfo> owners = ownerInfoService.selectOwner(".*", ".*");
        for(OwnerInfo owner : owners) {
            String[] ad = owner.getAddress().split(":");
            Pair<String,String> hostPost = Pair.of(ad[0],ad[1]);
            TestPing ping = new TestPing();
            String nowStatus = ping.alive(hostPost) ? "connected" : "disconneted";
            ownerInfoService.updateStatus(owner.getId(),nowStatus);
        }
    }
}
