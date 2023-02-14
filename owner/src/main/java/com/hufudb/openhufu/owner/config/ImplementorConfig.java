package com.hufudb.openhufu.owner.config;

import com.hufudb.openhufu.common.exception.ErrorCode;
import com.hufudb.openhufu.common.exception.OpenHuFuException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.SafeConstructor;

public class ImplementorConfig {

  private static final String implementorPath = "owner.yml";

  private final static class IMPLEMENTOR_KEY {
    private final static String PREFIX = "openhufu.implementor.";
    private final static String AGG_PREFIX = PREFIX + "aggregate.";
  }

  public enum Implementor {
    AGG_COUNT(IMPLEMENTOR_KEY.AGG_PREFIX + "count"),
    AGG_MAX(IMPLEMENTOR_KEY.AGG_PREFIX + "max"),
    AGG_MIN(IMPLEMENTOR_KEY.AGG_PREFIX + "min"),
    AGG_SUM(IMPLEMENTOR_KEY.AGG_PREFIX + "sum"),
    AGG_AVG(IMPLEMENTOR_KEY.AGG_PREFIX + "avg"),
    JOIN(IMPLEMENTOR_KEY.PREFIX + "join");

    Implementor(String value) {
      this.value = value;
    }

    final String value;

  }

  static Map<Implementor, String> implementor2ClassMap = new HashMap<>();
  static Map<String, Object> config;

  static {
    loadImplementorConfig();
    implementor2ClassMap.put(Implementor.AGG_SUM,
        getClazz(Implementor.AGG_SUM.value));

  }

  public static String getImplementor(Implementor implementor) {
    return implementor2ClassMap.get(implementor);
  }

  private static void loadImplementorConfig() {
    Yaml yaml = new Yaml(new SafeConstructor());
    InputStream stream = ImplementorConfig.class.getClassLoader()
        .getResourceAsStream(implementorPath);
    config = yaml.load(stream);
  }

  private static String getClazz(String keyStr) {
    String[] keys = keyStr.split("\\.");
    int i = 0;
    for (; i < keys.length - 1; i++) {
      config = (Map<String, Object>) config.get(keys[i]);
      if (null == config) {
        throw new OpenHuFuException(ErrorCode.IMPLEMENTOR_CONFIG_MISSING, keyStr);
      }
    }
    String clazz = (String) config.get(keys[i]);
    if (null == clazz) {
      throw new OpenHuFuException(ErrorCode.IMPLEMENTOR_CONFIG_MISSING, keyStr);
    }
    return clazz;
  }
}
