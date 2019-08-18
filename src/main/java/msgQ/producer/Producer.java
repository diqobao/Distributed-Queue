package msgQ.producer;

import java.util.concurrent.CompletableFuture;

public class Producer<T> {
    private SendThread sendThread;


    public Producer () {
        this.sendThread = new SendThread();
    }

    public CompletableFuture<ProducerMetaRecord> publish(ProducerRecord<T> record) {

        CompletableFuture<ProducerMetaRecord> future = new CompletableFuture<>();
        this.sendThread.send(record, future);
        return future;
    }


}