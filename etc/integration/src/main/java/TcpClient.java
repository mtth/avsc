import avro.Math;
import avro.Pair;
import java.io.IOException;
import java.net.InetSocketAddress;
import org.apache.avro.ipc.NettyServer;
import org.apache.avro.ipc.NettyTransceiver;
import org.apache.avro.ipc.specific.SpecificRequestor;

public class TcpClient {

  public static void main(String[] args) throws IOException {
    NettyTransceiver client = new NettyTransceiver(new InetSocketAddress(65111));
    Math proxy = SpecificRequestor.getClient(Math.class, client);
    send(proxy, 12, 48);
    send(proxy, 56, 123);
    send(proxy, 2, 4);
    client.close();
  }

  public static void send(Math proxy, int left, int right) throws IOException {
    Pair pair = new Pair(left, right);
    System.out.println("Sending:  " + pair.toString());
    System.out.println("Received: " + proxy.add(pair));
  }

}
