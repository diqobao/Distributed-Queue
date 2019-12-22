package msgQ.broker;

import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;

import msgQ.consumer.ConsumerRecord;
import msgQ.consumer.MessagePushGrpc;
import msgQ.consumer.MessagePushProto.*;
import org.apache.kafka.common.protocol.types.Field;


public class MessagePushClient {
    private static final Logger logger = Logger.getLogger(MessagePushClient.class.getName());

    private final ManagedChannel channel;
    private final MessagePushGrpc.MessagePushStub asyncStub;
    private final MessagePushGrpc.MessagePushBlockingStub blockingStub;

    public MessagePushClient(String host, int port) {
        ManagedChannelBuilder channelBuilder = ManagedChannelBuilder.forAddress(host, port).usePlaintext();
        channel = channelBuilder.build();
        blockingStub = MessagePushGrpc.newBlockingStub(channel);
        asyncStub = MessagePushGrpc.newStub(channel);
    }

    public void pushMsg(ConsumerRecord record) {
        ConsumerRecordReq consumerRecordReq = ConsumerRecordReq.newBuilder()
                .setTopic(record.getTopic()).setMessage((String) record.getValue()).setUuid(record.getUuid().toString()).build();

        StreamObserver<RecordReply> responseObserver;
        RecordReply reply;
        try {
            reply = blockingStub.pushMsg(consumerRecordReq);
            if(!reply.getMessage().equals("ok")) {
                // TODO: try again?
            }
        } catch (StatusRuntimeException e) {
            e.printStackTrace();
        }
    }

    public void shutdown() throws InterruptedException {
        channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
    }

    public static void main(String[] args) throws InterruptedException {
        MessagePushClient client = new MessagePushClient("localhost", 5001);
        try {
            ConsumerRecord<String> record1 = new ConsumerRecord<>(UUID.randomUUID(), "topic", "msg1");
            ConsumerRecord<String> record2 = new ConsumerRecord<>(UUID.randomUUID(), "topic", "msg2");
            client.pushMsg(record1);
            client.pushMsg(record2);
        } finally {
            client.shutdown();
        }
    }

}
