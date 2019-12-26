package msgQ.broker;

import msgQ.common.Record;

import java.util.UUID;

public class BrokerRecord<T> implements Record<T> {
    private String topic;
    private T value;
    private UUID uuid;
    private int groupId = -1;

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
