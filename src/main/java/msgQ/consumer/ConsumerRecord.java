package msgQ.consumer;

import java.util.UUID;

import msgQ.common.Record;

public class ConsumerRecord<T> implements Record<T> {
    private String topic;
    private T value;
    private UUID uuid;
    private int groupId = -1;

    ConsumerRecord(MessagePushProto.ConsumerRecordReq consumerRecordReq) {
        this.topic = consumerRecordReq.getTopic();
        this.value = (T) consumerRecordReq.getMessage();
        this.uuid = UUID.fromString(consumerRecordReq.getUuid());
    }

    public ConsumerRecord(UUID uuid, String topic, T value) {
        this.topic = topic;
        this.value = value;
        this.uuid = uuid;
    }

    public void setGroupId(int id) {
        this.groupId = id;
    }

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

    public String toString() {
        return String.format("uuid: %s topic: %s msg: %s", uuid.toString(), topic, value);
    }
}
