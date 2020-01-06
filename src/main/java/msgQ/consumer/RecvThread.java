package msgQ.consumer;

import msgQ.consumer.Consumer;
import msgQ.consumer.ConsumerRecord;

import java.util.concurrent.BlockingQueue;

public class RecvThread extends Thread {
    private final Consumer consumer;

    RecvThread(Consumer consumer) {
        this.consumer = consumer;
    }

    @Override
    public void run() {
        try {
            while(true) {
                consumer.pollRecord();
                Thread.sleep(1000);
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}

