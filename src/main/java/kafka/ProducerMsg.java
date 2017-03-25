package kafka;

/**
 * Created by sponge on 2017/3/25 0025.
 */
public class ProducerMsg {
    public static void main(String[] args) {
        ProducerThread producer1 = new ProducerThread("chuanglitopic", 100000);
        producer1.start();
    }
}
