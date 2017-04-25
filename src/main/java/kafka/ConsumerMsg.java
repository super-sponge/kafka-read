package kafka;


import org.apache.commons.cli.*;

import java.util.Properties;


/**
 * Created by sponge on 2017/3/25 0025.
 */
public class ConsumerMsg {

    public static void main(String[] args) {


        Options opts = new Options();
        opts.addOption("t",  true, "Topic name");
        opts.addOption("h", false, "Help message");
        CommandLineParser parser = new DefaultParser();
        CommandLine cl;
        try {
            cl = parser.parse(opts, args);
            if (cl.getOptions().length > 0 ) {
                if (cl.hasOption('h')) {
                    HelpFormatter hf = new HelpFormatter();
                    hf.printHelp("May Options", opts);
                } else {
                    String topic = cl.getOptionValue("t");
                    consumerMsg(topic);
                }
            } else {
                HelpFormatter hf = new HelpFormatter();
                hf.printHelp("May Options", opts);
            }
        } catch (ParseException e) {
            e.printStackTrace();
            HelpFormatter hf = new HelpFormatter();
            hf.printHelp("May Options", opts);
        }
    }
    private static void consumerMsg(String topic) {

        // consumer message
        Properties props = new Properties();
        props.put(org.apache.kafka.clients.consumer.ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "dckfa01:6667,dckfa02:6667,dckfa03:6667,dckfa04:6667,dckfa05:6667");
        props.put(org.apache.kafka.clients.consumer.ConsumerConfig.GROUP_ID_CONFIG, "Consumer");
        props.put("security.protocol", "SASL_PLAINTEXT");

        ConsumerThread consumerThread1 = new ConsumerThread(topic, props, "myThread-1");
        ConsumerThread consumerThread2 = new ConsumerThread(topic, props, "myThread-2");

        consumerThread1.start();
        consumerThread2.start();
    }
}