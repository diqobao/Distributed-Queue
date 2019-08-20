package msgQ.producer;

import msgQ.common.Constants;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.*;

public class SendThread extends Thread {
    private BlockingQueue<ProducerRecord<?>> queue;
    private Map<ProducerRecord<?>, CompletableFuture<ProducerMetaRecord>> futureMap = new HashMap<>();
    private ExecutorService executor;
    private int timeOut;

    public SendThread() {
        this.queue = new LinkedBlockingQueue<>();
        this.futureMap = new HashMap<>();
        this.executor = Executors.newFixedThreadPool(Constants.NUM_THREADS);
    }

    public void send(ProducerRecord<?> prod, CompletableFuture<ProducerMetaRecord> future) {
        queue.add(prod);
        futureMap.put(prod, future);
    }

    @Override
    public void run() {
        while (true) {
            CountDownLatch countLatch = new CountDownLatch(1);
            ProducerRecord<?> record = queue.poll();
            if (record == null) {
                continue;
            }
            executor.execute(new SendHandler(record, countLatch));
            try {
                countLatch.await(timeOut, TimeUnit.MILLISECONDS);
            } catch (InterruptedException ie) {
                ie.printStackTrace();
            }
        }
    }
}
