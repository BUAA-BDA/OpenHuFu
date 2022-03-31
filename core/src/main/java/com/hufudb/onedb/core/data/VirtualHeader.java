package com.hufudb.onedb.core.data;

import java.util.List;

/*
 *
 */
public class VirtualHeader extends Header {

  private List<Integer> mapping;

  VirtualHeader() {
    super();
  }

  VirtualHeader(List<Field> fields, List<Integer> mappings) {
    super(fields);
    this.mapping = mappings;
  }

  static VirtualHeader of(PublishedTableInfo tableInfo) {
    return new VirtualHeader(tableInfo.getOriginFields(), tableInfo.getMappings());
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

  @Override
  public Level getLevel(int index) {
    int idx = mapping.get(index);
    return super.getLevel(idx);
  }

  @Override
  public int size() {
    return mapping.size();
  }
}
