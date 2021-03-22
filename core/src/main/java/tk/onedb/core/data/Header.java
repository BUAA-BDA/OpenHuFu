package tk.onedb.core.data;

import java.util.List;
import java.util.stream.Collectors;

import tk.onedb.rpc.OneDBCommon.HeaderProto;

public class Header {
  List<Field> fields;

  Header(List<Field> fields) {
    this.fields = fields;
  }

  Header(HeaderProto proto) {
    this.fields = proto.getFieldList().stream().map(f -> Field.fromProto(f)).collect(Collectors.toList());
  }

  public HeaderProto toProto() {
    return HeaderProto.newBuilder().addAllField(fields.stream().map(f -> f.toProto()).collect(Collectors.toList())).build();
  }

  public static Header fromProto(HeaderProto proto) {
    return new Header(proto);
  }

  public FieldType getType(int index) {
    return fields.get(index).type;
  }

  public int getTypeId(int index) {
    return fields.get(index).type.ordinal();
  }

  public String getName(int index) {
    return fields.get(index).name;
  }
}
