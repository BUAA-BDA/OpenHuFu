package com.hufudb.onedb.rpc.utils;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.stream.Collectors;
import com.google.protobuf.ByteString;
import com.hufudb.onedb.rpc.OneDBPipe.DataPacketProto;
import com.hufudb.onedb.rpc.OneDBPipe.PayloadProto;
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

  public int getHeaderByteLength() {
    return 32;
  }

  public int getPayloadByteLength() {
    return payload.stream().map(p -> p.length).reduce(0,
        (totalLength, length) -> totalLength + length);
  }

  public DataPacketProto toProto() {
    return DataPacketProto.newBuilder().setHeaderProto(header.toProto())
        .setPayloadProto(PayloadProto.newBuilder()
            .addAllPayloadBytes(
                payload.stream().map(p -> ByteString.copyFrom(p)).collect(Collectors.toList()))
            .build())
        .build();
  }

  public static DataPacket fromProto(DataPacketProto proto) {
    return DataPacket.fromByteArrayList(DataPacketHeader.fromProto(proto.getHeaderProto()),
        proto.getPayloadProto().getPayloadBytesList().stream().map(p -> p.toByteArray())
            .collect(Collectors.toList()));
  }

  @Override
  public int hashCode() {
    return new HashCodeBuilder().append(header)
        .append(payload.stream().map(ByteBuffer::wrap).collect(Collectors.toList())).toHashCode();
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
        .append(this.payload.stream().map(ByteBuffer::wrap).collect(Collectors.toList()),
            that.payload.stream().map(ByteBuffer::wrap).collect(Collectors.toList()))
        .isEquals();
  }

  @Override
  public String toString() {
    return header.toString();
  }
}
