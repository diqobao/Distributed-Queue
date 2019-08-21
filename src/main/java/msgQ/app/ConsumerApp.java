package msgQ.app;

import msgQ.consumer.Consumer;

import java.util.Properties;

public class ConsumerApp {
    public static void run(Properties configs) {
        Consumer consumer = new Consumer(configs);
        try {
            consumer.start();
            consumer.subscribeTopics(new String[]{"topic1"});
            String[] subscriptions = consumer.getSubscriptions();
            consumer.unsubscribeTopics(new String[]{"topic1"});
            consumer.stop();
        }catch (Exception e) {
            e.printStackTrace();
        }
    }
}
