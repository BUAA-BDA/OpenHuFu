package group.bda.federate.sql.functions;

import java.util.Map;
import java.util.concurrent.ExecutorService;

import group.bda.federate.client.FederateDBClient;
import group.bda.federate.data.DataSet;

public interface AggregateFunc {
  void addRow(DataSet.DataRow row);
  void ShamirCount(String aggUuid, Map<FederateDBClient, String> tableClients, ExecutorService executorService);
  Object result();
}
