package msgQ.producer;

import msgQ.common.Record;

import java.util.UUID;

public class ProducerRecord<T> implements Record<T> {
    private String topic;
    private T value;
    private UUID uuid;
    private int groupId = -1;

    @Override
    public String getTopic() {
        return this.topic;
    }

    @Override
    public T getValue() {
        return this.value;
    }

    public UUID getUuid() {
        return this.uuid;
    }

    public int getGroupId() {
        return this.groupId;
    }


}
