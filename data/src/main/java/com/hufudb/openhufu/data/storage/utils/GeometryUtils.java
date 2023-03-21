package com.hufudb.openhufu.data.storage.utils;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKTReader;
import org.locationtech.jts.io.WKTWriter;

public class GeometryUtils {
  public static byte[] toBytes(Geometry geo) {
    ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
    byte[] bytes;
    try {
      ObjectOutputStream objectOutputStream = new ObjectOutputStream(byteArrayOutputStream);
      objectOutputStream.writeObject(geo);
      bytes = byteArrayOutputStream.toByteArray();
      byteArrayOutputStream.close();
      objectOutputStream.close();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    return bytes;
  }

  public static Geometry fromBytes(byte[] bytes) {
    ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(bytes);
    Geometry geo;
    try {
      ObjectInputStream objectInputStream = new ObjectInputStream(byteArrayInputStream);
      geo = (Geometry) objectInputStream.readObject();
    } catch (IOException e) {
      throw new RuntimeException(e);
    } catch (ClassNotFoundException e) {
      throw new RuntimeException(e);
    }
    return geo;
  }

  public static String toString(Geometry geo) {
    if (geo == null) {
      return null;
    }
    WKTWriter writer = new WKTWriter();
    return writer.write(geo);
  }

  public static Geometry fromString(String wkb) {
    if (wkb == null) {
      return null;
    }
    WKTReader reader = new WKTReader();
    Geometry geo;
    try {
      geo = reader.read(wkb);
    } catch (ParseException e) {
      throw new RuntimeException(e);
    }
    return geo;
  }
}
