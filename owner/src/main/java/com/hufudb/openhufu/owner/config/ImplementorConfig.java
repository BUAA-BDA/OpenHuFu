package com.hufudb.openhufu.owner.config;

import com.hufudb.openhufu.common.exception.ErrorCode;
import com.hufudb.openhufu.common.exception.OpenHuFuException;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.SafeConstructor;

public class ImplementorConfig {

  private static final String implementorPath = "owner1.yml";

  private final static class IMPLEMENTOR_KEY {
    private final static String PREFIX = "owner.implementor.";
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

  public static String getImplementorClassName(Implementor implementor) {
    return implementor2ClassMap.get(implementor);
  }

  public static void initImplementorConfig(String implementorPath) {
    loadImplementorConfig(implementorPath);
    implementor2ClassMap.put(Implementor.AGG_COUNT,
            getClazz(Implementor.AGG_COUNT.value));
    implementor2ClassMap.put(Implementor.AGG_MAX,
            getClazz(Implementor.AGG_MAX.value));
    implementor2ClassMap.put(Implementor.AGG_MIN,
            getClazz(Implementor.AGG_MIN.value));
    implementor2ClassMap.put(Implementor.AGG_SUM,
            getClazz(Implementor.AGG_SUM.value));
    implementor2ClassMap.put(Implementor.AGG_AVG,
            getClazz(Implementor.AGG_AVG.value));
    implementor2ClassMap.put(Implementor.JOIN,
            getClazz(Implementor.JOIN.value));
  }
  private static void loadImplementorConfig(String implementorPath) {
    Yaml yaml = new Yaml(new SafeConstructor());
    try {
      InputStream stream = new FileInputStream(implementorPath);
      config = yaml.load(stream);
    }
    catch (FileNotFoundException e) {
      throw new OpenHuFuException(ErrorCode.IMPLEMENTOR_CONFIG_FILE_NOT_FOUND, implementorPath);
    }
  }

  private static String getClazz(String keyStr) {
    String[] keys = keyStr.split("\\.");
    Map<String, Object> configMap = config;
    int i = 0;
    for (; i < keys.length - 1; i++) {
      configMap = (Map<String, Object>) configMap.get(keys[i]);
      if (null == configMap) {
        throw new OpenHuFuException(ErrorCode.IMPLEMENTOR_CONFIG_MISSING, keyStr);
      }
    }
    String clazz = (String) configMap.get(keys[i]);
//    if (null == clazz) {
//      throw new OpenHuFuException(ErrorCode.IMPLEMENTOR_CONFIG_MISSING, keyStr);
//    }
    return clazz;
  }
}
