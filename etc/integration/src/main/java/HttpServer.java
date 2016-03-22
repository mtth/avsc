import avro.Math;
import avro.Pair;
import java.io.IOException;
import org.apache.avro.ipc.specific.SpecificResponder;

public class HttpServer {

  public static class MathImpl implements Math {
    public double add(Pair pair) {
      return (double) (pair.getLeft() + pair.getRight());
    }
  }

  public static void main(String[] args) throws IOException {
    org.apache.avro.ipc.HttpServer server = new org.apache.avro.ipc.HttpServer(new SpecificResponder(Math.class, new MathImpl()), 8888);
    server.start();
  }

}
