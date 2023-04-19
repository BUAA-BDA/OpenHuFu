package com.hufudb.openhufu.mpc.union;

import com.google.common.collect.ImmutableList;
import com.hufudb.openhufu.data.schema.Schema;
import com.hufudb.openhufu.data.storage.*;
import com.hufudb.openhufu.mpc.ProtocolException;
import com.hufudb.openhufu.mpc.ProtocolType;
import com.hufudb.openhufu.mpc.RpcProtocolExecutor;
import com.hufudb.openhufu.rpc.Rpc;
import com.hufudb.openhufu.rpc.utils.DataPacket;
import com.hufudb.openhufu.rpc.utils.DataPacketHeader;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
//Pawel Jurczyk and Li Xiong. 2011. Information Sharing across Private Databases: Secure Union Revisited. In SocialCom/PASSAT. 996–1003.
public class SecretUnion extends RpcProtocolExecutor {

  private List<Integer> parties;
  private long taskId;

  public SecretUnion(Rpc rpc) {
    super(rpc, ProtocolType.SECRET_UNION);
  }

  private void printAll(DataSet dataSet) {
    DataSetIterator it = dataSet.getIterator();
    while (it.next()) {
      System.out.println(it.get(0));
    }
  }

  private DataSet leaderProcedure(DataSet dataSet) {
    //phase 1
    RandomDataSet randomDataSet = new RandomDataSet(dataSet);
    sendRowsToSuccessor(randomDataSet.getRandomSet());
    ArrayDataSet receives = receiveRowsFromPredecessor(dataSet.getSchema());

    //phase 2
    sendRowsToSuccessor(randomDataSet.removeRandom(receives));
    return receiveRowsFromPredecessor(dataSet.getSchema());
  }

  private void followerProcedure(DataSet dataSet) {
    //phase 1
    RandomDataSet randomDataSet = new RandomDataSet(dataSet);
    ArrayDataSet receives = receiveRowsFromPredecessor(dataSet.getSchema());
    sendRowsToSuccessor(mergeDataSet(randomDataSet.getRandomSet(), receives));

    //phase 2
    ArrayDataSet receives2 = receiveRowsFromPredecessor(dataSet.getSchema());
    sendRowsToSuccessor(randomDataSet.removeRandom(receives2));
  }

  private void sendRowsToSuccessor(ArrayDataSet dataSet) {
    LOG.info("sending rows from {} to {}", ownId, getSuccessorID());
    final DataPacketHeader header =
            new DataPacketHeader(taskId, getProtocolTypeId(), 0, ownId, getSuccessorID());
    rpc.send(DataPacket.fromByteArrayList(header,
            dataSet2BytesList(dataSet)));
  }

  private ArrayDataSet receiveRowsFromPredecessor(Schema schema) {
    final DataPacketHeader expect =
            new DataPacketHeader(taskId, getProtocolTypeId(), 0, getPredecessorID(), ownId);
    DataPacket packet = rpc.receive(expect);
    return bytesList2DataSet(packet.getPayload(), schema);
  }

  private ArrayDataSet mergeDataSet(ArrayDataSet left, ArrayDataSet right) {
    ArrayList<ArrayRow> arrayRows = new ArrayList<>();
    arrayRows.addAll(left.getRows());
    arrayRows.addAll(right.getRows());
    return new ArrayDataSet(left.getSchema(), arrayRows);
  }

  private List<byte[]> dataSet2BytesList(ArrayDataSet dataSet) {
    byte[] dd = objToByteArray(dataSet.getRows());
    return ImmutableList.of(dd);
  }

  private ArrayDataSet bytesList2DataSet(List<byte[]> bytesList, Schema schema) {
    return new ArrayDataSet(schema, (List<ArrayRow>) byteArrayToObj(bytesList.get(0)));
  }

  /**
   * 对象转Byte数组
   *
   * @param obj
   * @return
   */
  public byte[] objToByteArray(Object obj) {
    byte[] bytes = null;
    ByteArrayOutputStream byteArrayOutputStream = null;
    ObjectOutputStream objectOutputStream = null;
    try {
      byteArrayOutputStream = new ByteArrayOutputStream();
      objectOutputStream = new ObjectOutputStream(byteArrayOutputStream);
      objectOutputStream.writeObject(obj);
      objectOutputStream.flush();
      bytes = byteArrayOutputStream.toByteArray();
    } catch (IOException e) {
      System.err.println("objectToByteArray failed, " + e);
    } finally {
      if (objectOutputStream != null) {
        try {
          objectOutputStream.close();
        } catch (IOException e) {
          System.err.println("close objectOutputStream failed, " + e);
        }
      }
      if (byteArrayOutputStream != null) {
        try {
          byteArrayOutputStream.close();
        } catch (IOException e) {
          System.err.println("close byteArrayOutputStream failed, " + e);
        }
      }
    }
    return bytes;
  }

  /**
   * Byte数组转对象
   *
   * @param bytes
   * @return
   */
  public Object byteArrayToObj(byte[] bytes) {
    Object obj = null;
    ByteArrayInputStream byteArrayInputStream = null;
    ObjectInputStream objectInputStream = null;
    try {
      byteArrayInputStream = new ByteArrayInputStream(bytes);
      objectInputStream = new ObjectInputStream(byteArrayInputStream);
      obj = objectInputStream.readObject();
    } catch (Exception e) {
      System.err.println("byteArrayToObject failed, " + e);
    } finally {
      if (byteArrayInputStream != null) {
        try {
          byteArrayInputStream.close();
        } catch (IOException e) {
          System.err.println("close byteArrayInputStream failed, " + e);
        }
      }
      if (objectInputStream != null) {
        try {
          objectInputStream.close();
        } catch (IOException e) {
          System.err.println("close objectInputStream failed, " + e);
        }
      }
    }
    return obj;
  }

  private int getSuccessorID() {
    int index = parties.indexOf(ownId);
    int next = (index + 1) % parties.size();
    return parties.get(next);
  }

  private int getPredecessorID() {
    int index = parties.indexOf(ownId);
    int previous = (index + parties.size() - 1) % parties.size();
    return parties.get(previous);
  }

  /**
   * @param args[0] DataSet inputdata
   */
  @Override
  public Object run(long taskId, List<Integer> parties, Object... args) throws ProtocolException {
    // todo: check this
    DataSet localDataSet = (DataSet) args[0];
    this.taskId = taskId;
    this.parties = parties;
    boolean isLeader = ownId == parties.get(0);
    if (isLeader) {
      return leaderProcedure(localDataSet);
    } else {
      followerProcedure(localDataSet);
    }
    return null;
  }
}
