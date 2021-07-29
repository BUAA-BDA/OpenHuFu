package group.bda.federate.security.secretsharing;

import group.bda.federate.client.FederateDBClient;
import group.bda.federate.rpc.FederateService;
import group.bda.federate.security.secretsharing.utils.ShamirCache;
import org.apache.commons.math3.linear.Array2DRowRealMatrix;
import org.apache.commons.math3.linear.ArrayRealVector;
import org.apache.commons.math3.linear.DecompositionSolver;
import org.apache.commons.math3.linear.LUDecomposition;
import org.apache.commons.math3.linear.RealMatrix;
import org.apache.commons.math3.linear.RealVector;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.Vector;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

public class ShamirSharing {
  private static final Logger LOG = LogManager.getLogger(ShamirSharing.class);
  private static final List<Integer> coeList = new ArrayList<>();
  private static final Map<String, ShamirCache> cacheMap = new HashMap<>();
  private static final Object object = new Object();
  private static final Random random = new Random();

  public static void addCoeList() {
    coeList.add(random.nextInt(32) + 1);
  }

  public static List<Integer> generateXList(int size) {
    List<Integer> xList = new ArrayList<>();
    for (int i = 0; i < size; ++i) {
      int x = random.nextInt(32) + 1;
      while (xList.contains(x)) {
        x = random.nextInt(32) + 1;
      }
      xList.add(x);
    }
    return xList;
  }


  public static double calQValue(int xi, double value, int length) {
    double q = value;
    for (int i = 0; i < length; ++i) {
      q += Math.pow(xi, i + 1) * coeList.get(i);
    }
    return q;
  }

  public static FederateService.PrivacyCountResponse
  privacyCount(double value, final List<Integer> x,
               final List<String> endpoints, final String uuid,
               Map<String, FederateDBClient> federateDBClientMap) {
    synchronized (object) {
      if (!cacheMap.containsKey(uuid)) {
        ShamirCache shamirCache = new ShamirCache();
        cacheMap.put(uuid, shamirCache);
      }
    }
    ShamirCache shamirCache = cacheMap.get(uuid);
    for (int i = 0; i < x.size(); i++) {
      double qxi = calQValue(x.get(i), value, x.size() - 1);
      String endpoint = endpoints.get(i);
      if (federateDBClientMap.containsKey(endpoint)) {
        federateDBClientMap.get(endpoint).sendQValue(qxi, uuid);
      } else {
        shamirCache.setQValue(qxi);
      }
    }
    synchronized (object) {
      while (shamirCache.getSize() < endpoints.size()) {
        try {
          object.wait();
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }
    }
    return FederateService.PrivacyCountResponse.newBuilder().setStatus(FederateService.Status.newBuilder()
            .setCode(FederateService.Code.kOk).setMsg("ok").build()).build();
  }

  public static FederateService.GeneralResponse revQValue(String uuid, double q) {
    synchronized (object) {
      if (!cacheMap.containsKey(uuid)) {
        ShamirCache shamirCache = new ShamirCache();
        cacheMap.put(uuid, shamirCache);
      }
      cacheMap.get(uuid).setQValue(q);
      object.notifyAll();
    }
    return FederateService.GeneralResponse.newBuilder().setStatus(FederateService.Status.newBuilder()
            .setCode(FederateService.Code.kOk).setMsg("ok").build()).build();
  }

  public static FederateService.PrivacyCountResponse getSum(String uuid) {
    double sum = cacheMap.get(uuid).calSum();
    cacheMap.remove(uuid);
    return FederateService.PrivacyCountResponse.newBuilder().setSum(sum)
            .setStatus(FederateService.Status.newBuilder().setCode(FederateService.Code.kOk).setMsg("").build()).build();
  }

  public static double calCount(List<Integer> xList, List<Double> sList) {
    double[][] xMatrix = new double[xList.size()][xList.size()];
    for (int i = 0; i < xList.size(); ++i) {
      for (int j = 0; j < xList.size(); ++j) {
        xMatrix[i][j] = Math.pow(xList.get(i), j);
      }
    }
    double[] yVector = new double[sList.size()];
    for (int i = 0; i < sList.size(); ++i) {
      yVector[i] = sList.get(i);
    }
    RealMatrix coefficients = new Array2DRowRealMatrix(xMatrix, false);
    DecompositionSolver solver = new LUDecomposition(coefficients).getSolver();
    RealVector constants = new ArrayRealVector(yVector, false);
    RealVector solution = solver.solve(constants);
    return solution.getEntry(0);
  }

  public static double shamirCount(Map<FederateDBClient, String> tableClients, FederateService.PrivacyCountRequest.Builder privacyCountRequest, ExecutorService executorService) {
    final List<Double> sList = new Vector<>();
    final List<Integer> xList = generateXList(tableClients.size());
    String uuid = UUID.randomUUID().toString();
    List<String> endpoints = new ArrayList<>();
    for (Map.Entry<FederateDBClient, String> entry : tableClients.entrySet()) {
      endpoints.add(entry.getKey().getEndpoint());
      sList.add((double) 0);
    }
    List<Callable<Boolean>> calSumTasks = new ArrayList<>();
    for (Map.Entry<FederateDBClient, String> entry : tableClients.entrySet()) {
      calSumTasks.add(() -> {
        entry.getKey().privacyCount(privacyCountRequest.clone().addAllX(xList).addAllEndpoints(endpoints).setUuid(uuid).build());
        return true;
      });
    }
    try {
      List<Future<Boolean>> results = executorService.invokeAll(calSumTasks);
      for (Future<Boolean> res : results) {
        if (!res.get()) {
          LOG.error("error when calculating secret shares");
          return 0L;
        }
      }
    } catch (InterruptedException | ExecutionException e) {
      e.printStackTrace();
    }
    LOG.debug("xList is {}", xList);
    int i = 0;
    List<Callable<Boolean>> getSumTasks = new ArrayList<>();
    for (Map.Entry<FederateDBClient, String> entry : tableClients.entrySet()) {
      final int id = i;
      getSumTasks.add(() -> {
        double sum = entry.getKey().getSum(uuid);
        sList.set(id, sum);
        return true;
      });
      i++;
    }
    try {
      List<Future<Boolean>> results = executorService.invokeAll(getSumTasks);
      for (Future<Boolean> res : results) {
        if (!res.get()) {
          LOG.error("error when collecting secret shares");
          return 0L;
        }
      }
    } catch (InterruptedException | ExecutionException e) {
      e.printStackTrace();
    }
    LOG.debug("cal S value finish {} ", sList);
    double res = calCount(xList, sList);
    LOG.debug("the shamir result is {} ", res);
    return res;
  }

}
