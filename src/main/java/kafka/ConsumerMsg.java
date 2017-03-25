package kafka;


import java.util.Properties;


/**
 * Created by sponge on 2017/3/25 0025.
 */
public class ConsumerMsg {

    public static void main(String[] args) {


        Properties props = new Properties();
        props.put(org.apache.kafka.clients.consumer.ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "dckfa01:6667,dckfa02:6667,dckfa03:6667,dckfa04:6667,dckfa05:6667");
        props.put(org.apache.kafka.clients.consumer.ConsumerConfig.GROUP_ID_CONFIG, "Consumer");
        props.put("security.protocol", "SASL_PLAINTEXT");

        ConsumerThread consumerThread1 = new ConsumerThread("chuanglitopic", props, "myThread-1");

        consumerThread1.start();

        //executor shutdown logic is skipped
    }
}