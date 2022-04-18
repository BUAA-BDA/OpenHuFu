package com.hufudb.onedb.rpc.utils;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

public class DataPacketHeader {
  private final long taskId;
  private final int ptoId;
  private final int stepId;
  private final int senderId;
  private final int receiverId;
  private final long extraInfo;

  public DataPacketHeader(long taskId, int ptoId, int stepId, int senderId, int receiverId) {
    this(taskId, ptoId, stepId, 0L, senderId, receiverId);
  }

  public DataPacketHeader(long taskId, int ptoId, int stepId, long extraInfo, int senderId,
      int receiverId) {
    assert taskId >= 0;
    assert ptoId >= 0;
    assert stepId >= 0;
    assert senderId >= 0;
    assert receiverId >= 0;
    assert taskId >= 0;
    assert taskId >= 0;
    this.taskId = taskId;
    this.ptoId = ptoId;
    this.stepId = stepId;
    this.senderId = senderId;
    this.receiverId = receiverId;
    this.extraInfo = extraInfo;
  }

  public long getTaskId() {
    return taskId;
  }

  public int getPtoId() {
    return ptoId;
  }

  public int getStepId() {
    return stepId;
  }

  public long getExtraInfo() {
    return extraInfo;
  }

  public int getSenderId() {
    return senderId;
  }

  public int getReceiverId() {
    return receiverId;
  }

  @Override
  public int hashCode() {
    return new HashCodeBuilder()
      .append(taskId)
      .append(ptoId)
      .append(stepId)
      .append(extraInfo)
      .append(senderId)
      .append(receiverId)
      .toHashCode();
  }

  @Override
  public boolean equals(Object obj) {
    if (!(obj instanceof DataPacketHeader)) {
      return false;
    }
    if (obj == this) {
      return true;
    }
    DataPacketHeader that = (DataPacketHeader) obj;
    return new EqualsBuilder()
      .append(this.taskId, that.taskId)
      .append(this.ptoId, that.ptoId)
      .append(this.stepId, that.stepId)
      .append(this.extraInfo, that.extraInfo)
      .append(this.senderId, that.senderId)
      .append(this.receiverId, that.receiverId)
      .isEquals();
  }
}
