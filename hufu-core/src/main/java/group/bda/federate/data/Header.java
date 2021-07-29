package group.bda.federate.data;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.lang3.ArrayUtils;

import group.bda.federate.rpc.FederateCommon.HeaderProto;
import group.bda.federate.sql.type.FederateFieldType;

public class Header implements Serializable {
  private static final long serialVersionUID = 10L;
  private final String[] names;
  private final FederateFieldType[] types;
  private final Level[] levels;
  private final boolean hasPrivacy;
  private final boolean hasPrivate;
  private final boolean privacyKnn;
  private final boolean privacyAgg;

  private Header(String[] names, FederateFieldType[] types, Level[] levels, boolean hasPrivacy, boolean hasPrivate, boolean privacyKnn, boolean privacyAgg) {
    this.names = names;
    this.types = types;
    this.levels = levels;
    this.hasPrivacy = hasPrivacy;
    this.hasPrivate = hasPrivate;
    this.privacyKnn = privacyKnn;
    this.privacyAgg = privacyAgg;
  }

  private Header(String[] names, FederateFieldType[] types, Level[] levels) {
    this.names = names;
    this.types = types;
    this.levels = levels;
    this.hasPrivacy = false;
    this.hasPrivate = false;
    this.privacyKnn = false;
    this.privacyAgg = false;
  }

  private Header(String[] names, FederateFieldType[] types) {
    this.names = names;
    this.types = types;
    this.levels = null;
    this.hasPrivacy = false;
    this.hasPrivate = false;
    this.privacyKnn = false;
    this.privacyAgg = false;
  }

  public static Builder newBuilder(int size) {
    return new Builder(size);
  }

  public static IteratorBuilder newBuilder() {
    return new IteratorBuilder();
  }

  public static Header merge(Header left, Header right) {
    IteratorBuilder builder = newBuilder();
    for (int i = 0; i < left.size(); ++i) {
      builder.add(left.getName(i), left.getType(i));
    }
    for (int i = 0; i < right.size(); ++i) {
      builder.add(right.getName(i), right.getType(i));
    }
    return builder.build();
  }

  public String getName(int index) {
    return names[index];
  }

  public String[] getNames() {
    return names;
  }

  public FederateFieldType getType(int index) {
    return types[index];
  }

  public Level getLevel(int index) {
    return levels == null ? Level.PUBLIC : levels[index];
  }

  public int index(String fieldName) {
    int fieldIndex;
    fieldIndex = ArrayUtils.indexOf(names, fieldName);
    if (fieldIndex < 0) {
      throw new RuntimeException("the order field does not exist");
    }
    return fieldIndex;
  }

  public boolean hasPrivacy() {
    return hasPrivacy;
  }

  public boolean hasPrivate() {
    return hasPrivate;
  }

  public boolean isPrivacyKnn() {
    return privacyKnn;
  }

  public boolean isPrivacyAgg() {
    return privacyAgg;
  }

  public FederateFieldType getTypeUnsafe(int index) {
    return types[index];
  }

  public int getGeomFieldIndex() {
    for (int i = 0; i < types.length; ++i) {
      if (types[i].equals(FederateFieldType.POINT)) {
        return i;
      }
    }
    return -1;
  }

  public String getGeomFieldName() {
    for (int i = 0; i < types.length; ++i) {
      if (types[i].equals(FederateFieldType.POINT)) {
        return names[i];
      }
    }
    return null;
  }

  public int size() {
    return names.length;
  }

  public boolean isPublic(int index) {
    return levels == null || levels[index].equals(Level.PUBLIC);
  }

  public HeaderProto toProto() {
    HeaderProto.Builder builder = HeaderProto.newBuilder();
    builder.addAllName(Arrays.asList(names));
    if (levels != null) {
      for (int i = 0; i < types.length; ++i) {
        builder.addType(types[i].ordinal());
        builder.addLevel(levels[i].ordinal());
      }
    } else {
      for (int i = 0; i < types.length; ++i) {
        builder.addType(types[i].ordinal());
      }
    }
    builder.setIsPrivacyAgg(this.privacyAgg);
    return builder.build();
  }

  public static Header fromProto(HeaderProto proto) {
    final int size = proto.getNameCount();
    Builder builder = newBuilder(size);
    if (proto.getLevelCount() != 0) {
      for (int i = 0; i < size; ++i) {
        builder.set(i, proto.getName(i), FederateFieldType.values()[proto.getType(i)],
            Level.values()[proto.getLevel(i)]);
      }
    } else {
      for (int i = 0; i < size; ++i) {
        builder.set(i, proto.getName(i), FederateFieldType.values()[proto.getType(i)]);
      }
    }
    if (proto.getIsPrivacyAgg()) {
      builder.setPrivacyAgg();
    }
    return builder.build();
  }

  @Override
  public String toString() {
    List<String> columnStr = new ArrayList<>();
    for (int i = 0; i < types.length; ++i) {
      columnStr.add(String.format("%s:%s", names[i], types[i].toString()));
    }
    return String.join(" | ", columnStr);
  }

  public String toTableString() {
    List<String> columnStr = new ArrayList<>();
    for (int i = 0; i < types.length; ++i) {
      columnStr.add(String.format("%s:%s", names[i], types[i].toString(),
          levels != null ? levels[i].toString() : Level.PUBLIC.toString()));
    }
    return String.join("\t", columnStr);
  }

  @Override
  public boolean equals(Object obj) {
    return obj == this || (obj instanceof Header && Arrays.equals(names, ((Header) obj).names)
        && Arrays.equals(types, ((Header) obj).types) && Arrays.equals(levels, ((Header) obj).levels));
  }

  public static class Builder {
    private String[] names;
    private FederateFieldType[] types;
    private Level[] levels;
    private boolean hasPrivacy;
    private boolean hasPrivate;
    private boolean privacyKnn;
    private boolean privacyAgg;
    private boolean hasLevel;

    private Builder(int size) {
      names = new String[size];
      types = new FederateFieldType[size];
      levels = new Level[size];
      hasPrivacy = false;
      hasPrivate = false;
      privacyKnn = false;
      privacyAgg = false;
      hasLevel = false;
    }

    public void set(int index, String name, FederateFieldType type) {
      names[index] = name;
      types[index] = type;
      levels[index] = Level.PUBLIC;
    }

    public void set(int index, String name, FederateFieldType type, Level level) {
      names[index] = name;
      types[index] = type;
      levels[index] = level;
      hasLevel = true;
      if (level.ordinal() > Level.PUBLIC.ordinal()) {
        hasPrivacy = true;
        if (level.equals(Level.PRIVATE)) {
          hasPrivate = true;
        }
      }
    }

    public void setPrivacy() {
      hasPrivacy = true;
    }

    public void setPrivacyKnn() {
      this.hasPrivacy = true;
      this.privacyKnn = true;
    }

    public void setPrivacyAgg() {
      this.hasPrivacy = true;
      this.privacyAgg = true;
    }

    public Header build() {
      if (hasPrivacy) {
        return new Header(names, types, levels, hasPrivacy, hasPrivate, privacyKnn, privacyAgg);
      } else if (hasLevel) {
        return new Header(names, types, levels);
      } else {
        return new Header(names, types);
      }
    }

    public void reset() {
      this.names = new String[names.length];
      this.types = new FederateFieldType[types.length];
      this.levels = new Level[levels.length];
    }
  }

  public static class IteratorBuilder {
    private List<String> names;
    private List<FederateFieldType> types;
    private List<Level> levels;
    private boolean hasPrivacy;
    private boolean hasPrivate;
    private boolean privacyKnn;
    private boolean privacyAgg;

    private IteratorBuilder() {
      names = new ArrayList<String>();
      types = new ArrayList<FederateFieldType>();
      levels = new ArrayList<Level>();
    }

    public void add(String name, FederateFieldType type) {
      names.add(name);
      types.add(type);
      levels.add(Level.PUBLIC);
    }

    public void add(String name, FederateFieldType type, Level level) {
      names.add(name);
      types.add(type);
      levels.add(level);
      if (level.ordinal() > Level.PUBLIC.ordinal()) {
        hasPrivacy = true;
        if (level.equals(Level.PRIVATE)) {
          hasPrivate = true;
        }
      }
    }

    public void setPrivacy() {
      hasPrivacy = true;
    }

    public void setPrivacyKnn() {
      this.hasPrivacy = true;
      this.privacyKnn = true;
    }

    public void setPrivacyAgg() {
      this.hasPrivacy = true;
      this.privacyAgg = true;
    }

    public Header build() {
      if (hasPrivacy) {
        return new Header(names.toArray(new String[names.size()]), types.toArray(new FederateFieldType[types.size()]),
            levels.toArray(new Level[levels.size()]), hasPrivacy, hasPrivate, privacyKnn, privacyAgg);
      } else {
        return new Header(names.toArray(new String[names.size()]), types.toArray(new FederateFieldType[types.size()]));
      }
    }

    public int size() {
      return names.size();
    }

    public void clear() {
      names.clear();
      types.clear();
    }
  }
}
