package kafka;

import org.apache.commons.cli.*;

/**
 * Created by sponge on 2017/3/25 0025.
 */
public class ProducerMsg {
    public static void main(String[] args) {

        Options opts = new Options();
        opts.addOption("t",  true, "Topic name");
        opts.addOption("n",  true, "Numbers events");
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
                    String number = cl.getOptionValue("n");

                    ProducerThread producer1 = new ProducerThread(topic, Integer.parseInt(number));
                    ProducerThread producer2 = new ProducerThread(topic, Integer.parseInt(number));
                    producer1.start();
                    producer2.start();

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
}
