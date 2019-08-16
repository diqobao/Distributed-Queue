package msgQ.producer;

import java.util.UUID;

public class ProducerMetaRecord {
    private String topic;
    private UUID uuid;
    private boolean succ;

    public String getTopic() {
        return this.topic;
    }

    public UUID getUuid() {
        return this.uuid;
    }

    public boolean getSucc() {
        return this.succ;
    }
}
