package com.hufudb.onedb.backend.utils;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.lang3.StringUtils;

public class TestPing {

    public boolean alive(Pair<String,String> hostPort) {
        if (StringUtils.isBlank(hostPort.getRight())) {
          try {
            InetAddress address = InetAddress.getByName(hostPort.getLeft());
            return address.isReachable(1500);
          } catch (IOException e) {
            return false;
          }
        }
        try (Socket socket = new Socket()) {
          socket.connect(
              new InetSocketAddress(hostPort.getLeft(), Integer.parseInt(hostPort.getRight())), 1500);
        } catch (Exception e) {
          return false;
        }
        return true;
      }
}
