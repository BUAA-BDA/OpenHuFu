package com.hufudb.onedb.core.data;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/*
 * 
*/
public class VirtualHeader extends Header {

  private List<Field> aliasFields;
  private List<Integer> mapping;

  VirtualHeader() {
    super();
    aliasFields = new ArrayList<>();
    mapping = new ArrayList<>();
  }

  VirtualHeader(Header header) {
      super(header.getFields());
      aliasFields = header.getFields();
      mapping = new ArrayList<>();
      for (int i = 0; i < header.size(); ++i) {
        mapping.add(i);
      }
  }

  VirtualHeader(Header header, List<Field> aliasFields) {
    super(header.getFields());
    setAlias(aliasFields);
  }

  public static VirtualHeader of(Header header) {
    return new VirtualHeader(header);
  }

  public static VirtualHeader of(Header header, List<Field> aliasFields) {
    return new VirtualHeader(header, aliasFields);
  }

  @Override
  public String getName(int index) {
    int idx = mapping.get(index);
    return super.getName(idx);
  }

  @Override
  public FieldType getType(int index) {
    int idx = mapping.get(index);
    return super.getType(idx);
  }

  @Override
  public int getTypeId(int index) {
    int idx = mapping.get(index);
    return super.getTypeId(idx);
  }

  public Header getAliasHeader() {
    return new Header(aliasFields);
  }

  public Header getOriginHeader() {
    return new Header(fields);
  }

  // todo: add type cast rules here
  private boolean checkTypeConvert(FieldType alias, FieldType origin) {
    return true;
  }

  // @ fields: should have the same length with this.fields
  public void setAlias(List<Field> aliasFields) {
    this.aliasFields = new ArrayList<>();
    this.mapping = new ArrayList<>();
    Set<String> nameSet = new HashSet<>();
    for (int i = 0; i < aliasFields.size(); ++i) {
      Field af = aliasFields.get(i);
      if (af.getLevel() != Level.HIDE) {
        if (checkTypeConvert(af.getType(), this.fields.get(i).getType()) 
            && !nameSet.contains(af.getName())) {
          this.aliasFields.add(aliasFields.get(i));
          this.mapping.add(i);
          nameSet.add(af.getName());
        } else {
          throw new RuntimeException("Error when setting alias of virutal header");
        }
      }
    }
  }
}
