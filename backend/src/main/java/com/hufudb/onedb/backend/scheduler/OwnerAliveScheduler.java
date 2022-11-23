package com.hufudb.onedb.backend.scheduler;

import com.hufudb.onedb.backend.service.OwnerInfoService;
import com.hufudb.onedb.backend.utils.TestPing;
import com.hufudb.onedb.persistence.entity.OwnerInfo;
import java.util.List;
import org.apache.commons.lang3.tuple.Pair;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

/**
 * @author yang.song
 * @date 11/8/22 3:05 PM
 */

@Service
@EnableScheduling
public class OwnerAliveScheduler {

  @Autowired
  OwnerInfoService ownerInfoService;

  @Scheduled(cron = "* */5 * * * *")
  public void testAllOwner() {
    List<OwnerInfo> owners = ownerInfoService.selectOwner(".*", ".*");
    for (OwnerInfo owner : owners) {
      String[] ad = owner.getAddress().split(":");
      Pair<String, String> hostPost = Pair.of(ad[0], ad[1]);
      String nowStatus = TestPing.alive(hostPost) ? "connected" : "disconneted";
      ownerInfoService.updateStatus(owner.getId(), nowStatus);
    }
  }

}
