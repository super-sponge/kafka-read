package kafka;


import kafka.utils.ShutdownableThread;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Collections;
import java.util.Properties;

/**
 * Created by sponge on 2017/3/24 0024.
 */
public class ConsumerThread extends ShutdownableThread
{
    private final KafkaConsumer<String, String> consumer;
    private final String topic;

    public ConsumerThread(String topic, Properties properties , String threadName)
    {
        super(threadName, false);
        Properties props = new Properties();
//        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "xj2:6667,xj3:6667");
//        props.put(ConsumerConfig.GROUP_ID_CONFIG, "Consumer");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,   "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");

        if (properties != null) {
            props.putAll(properties);
        }
        consumer = new KafkaConsumer<>(props);
        this.topic = topic;
    }

    @Override
    public void doWork() {
        consumer.subscribe(Collections.singletonList(this.topic));
        ConsumerRecords<String, String> records = consumer.poll(1000);
        for (ConsumerRecord<String, String> record : records) {
            System.out.println("Received message : (" + record.key() + ", " + record.value() + ") at offset "
                    + record.offset() + " from thread :" + this.getName());
        }
    }


    @Override
    public boolean isInterruptible() {
        return false;
    }
}