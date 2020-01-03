package msgQ.broker;

import msgQ.common.Record;

import java.util.UUID;

public class BrokerRecord<T> implements Record<T> {
    private String topic;
    private T value;
    private UUID uuid;
    private int groupId = -1;
    private long timestamp;

    BrokerRecord(UUID _uuid, String _topic, T _value, long _timestamp) {
        uuid = _uuid;
        topic = _topic;
        value = _value;
        timestamp = _timestamp;
    }

    BrokerRecord(MessageDeliveryProto.RecordReq recordReq, long _timestamp) {
        this.topic = recordReq.getTopic();
        this.value = (T) recordReq.getMessage();
        this.uuid = UUID.fromString(recordReq.getUuid());
        timestamp = _timestamp;
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


}
