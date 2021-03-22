package tk.onedb.core.data;

import tk.onedb.rpc.OneDBCommon.FieldProto;

public class Field {
  String name;
  FieldType type;

  public Field(String name, FieldType type) {
    this.name = name;
    this.type = type;
  }

  Field(String name, int type) {
    this.name = name;
    this.type = FieldType.values()[type];
  }

  public FieldProto toProto() {
    return FieldProto.newBuilder().setName(name).setType(type.ordinal()).build();
  }

  public static Field fromProto(FieldProto proto) {
    return new Field(proto.getName(), proto.getType());
  }
}
