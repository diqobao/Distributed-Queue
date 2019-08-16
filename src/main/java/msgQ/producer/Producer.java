package msgQ.producer;

import java.util.Properties;
import java.util.concurrent.CompletableFuture;

public class Producer<T> {
    
    public CompletableFuture<ProducerMetaRecord> publish(ProducerRecord<T> record) {
        return null;
    }

}