import avro.Math;
import avro.Pair;
import java.io.IOException;
import java.net.InetSocketAddress;
import org.apache.avro.ipc.NettyServer;
import org.apache.avro.ipc.NettyTransceiver;
import org.apache.avro.ipc.Server;
import org.apache.avro.ipc.specific.SpecificResponder;

public class TcpServer {

  public static class MathImpl implements Math {
    public double add(Pair pair) {
      return (double) (pair.getLeft() + pair.getRight());
    }
  }

  public static void main(String[] args) throws IOException {
    Server server = new NettyServer(new SpecificResponder(Math.class, new MathImpl()), new InetSocketAddress(65111));
    server.start();
  }

}
