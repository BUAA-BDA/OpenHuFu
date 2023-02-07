package com.hufudb.openhufu.expression;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.Properties;
import com.hufudb.openhufu.proto.OpenHuFuPlan.Expression;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CusAggFuncFactory {

  private static final Logger LOG = LoggerFactory.getLogger(CusAggFuncFactory.class);

  private static Properties prop = new Properties();

  public static void update(String propPath) {
    try {
      prop.load(new FileInputStream(propPath));
    } catch (IOException e) {
      LOG.error("Load prop file error", e);
    }
  }

  public static <T> T getAggFunc(String aggFuncType, Expression exp) {
    if (!prop.contains(aggFuncType)) {
      return null;
    }
    try {
      String className = prop.getProperty(aggFuncType);
      Class<T> clazz = (Class<T>) Class.forName(className);
      Constructor constructor = clazz.getDeclaredConstructor(Expression.class);
      return (T) constructor.newInstance(exp);
    } catch (ClassNotFoundException e) {
      LOG.error("Class not found", e);
    } catch (NoSuchMethodException e) {
      LOG.error("Declared constructor not found", e);
    } catch (InstantiationException | IllegalAccessException | InvocationTargetException e) {
      LOG.error("New instance error", e);
    }
    return null;
  }

}