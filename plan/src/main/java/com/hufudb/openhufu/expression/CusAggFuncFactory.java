package com.hufudb.openhufu.expression;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.Properties;
import com.hufudb.openhufu.proto.OpenHuFuPlan.Expression;

public class CusAggFuncFactory {
    private static Properties prop = new Properties();
    public static void update(String propPath) {
        try {
            prop.load(new FileInputStream(propPath));
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static <T> T getAggFunc(String aggFuncType, Expression exp) {
        if (!prop.contains(aggFuncType)) {
            return null;
        }
        try {
            String className = prop.getProperty(aggFuncType);
            Class<T> clazz = (Class<T>) Class.forName(className);
            Constructor constructor=clazz.getDeclaredConstructor(Expression.class);
            return (T) constructor.newInstance(exp);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (InstantiationException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        } catch (NoSuchMethodException e) {
            throw new RuntimeException(e);
        } catch (InvocationTargetException e) {
            throw new RuntimeException(e);
        }
        return null;
    }

}