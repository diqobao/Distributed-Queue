package msgQ.producer;

import msgQ.common.Constants;

import java.util.concurrent.CompletableFuture;

public class Producer<T> {
    private SendThread sendThread;

    public Producer () {
        this.sendThread = new SendThread();
    }

    public CompletableFuture<ProducerMetaRecord> publish(ProducerRecord<T> record) {
        int groupId = record.getTopic().hashCode() % Constants.NUM_GROUPS;
        record.setGroupId(groupId);
        CompletableFuture<ProducerMetaRecord> future = new CompletableFuture<>();
        this.sendThread.send(record, future);
        return future;
    }
}