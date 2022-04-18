package com.hufudb.onedb.rpc.utils;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

public final class DataPacket {
  private DataPacketHeader header;
  private List<byte[]> payload;

  public static DataPacket fromByteArrayList(DataPacketHeader header, List<byte[]> payload) {
    DataPacket dataPacket = new DataPacket();
    dataPacket.header = header;
    dataPacket.payload = payload;
    return dataPacket;
  }

  private DataPacket() {}

  public DataPacketHeader getHeader() {
    return header;
  }

  public List<byte[]> getPayload() {
    return payload;
  }

  @Override
  public int hashCode() {
    return new HashCodeBuilder().append(header).append(payload.stream().map(ByteBuffer::wrap).collect(Collectors.toList())).toHashCode();
  }

  @Override
  public boolean equals(Object obj) {
    if (!(obj instanceof DataPacket)) {
      return false;
    }
    if (obj == this) {
      return true;
    }
    DataPacket that = (DataPacket) obj;
    return new EqualsBuilder().append(this.header, that.header)
        .append(
          this.payload.stream().map(ByteBuffer::wrap).collect(Collectors.toList()),
          that.payload.stream().map(ByteBuffer::wrap).collect(Collectors.toList())
        ).isEquals();
  }
}
