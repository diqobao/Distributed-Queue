package msgQ.broker;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import msgQ.broker.MessageDeliveryGrpc;
import msgQ.broker.MessageDeliveryProto;

import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

public class ReplicationClient {
    private static final Logger logger = Logger.getLogger(MessagePushClient.class.getName());

    private final ManagedChannel channel;
    private final MessageDeliveryGrpc.MessageDeliveryStub asyncStub;
    private final MessageDeliveryGrpc.MessageDeliveryBlockingStub blockingStub;

    ReplicationClient(String host, String port) {
        ManagedChannelBuilder channelBuilder = ManagedChannelBuilder.forAddress(host, Integer.parseInt(port)).usePlaintext();
        channel = channelBuilder.build();
        blockingStub = MessageDeliveryGrpc.newBlockingStub(channel);
        asyncStub = MessageDeliveryGrpc.newStub(channel);
    }

    ReplicationClient(ManagedChannelBuilder channelBuilder) {
        channel = channelBuilder.build();
        blockingStub = MessageDeliveryGrpc.newBlockingStub(channel);
        asyncStub = MessageDeliveryGrpc.newStub(channel);
    }

    void replicateMsg(BrokerRecord record) {
        MessageDeliveryProto.RecordReq consumerRecordReq = MessageDeliveryProto.RecordReq.newBuilder()
                .setTopic(record.getTopic()).setTimestamp(record.getTimestamp()).setMessage((String) record.getValue()).setUuid(record.getUuid().toString()).build();

        StreamObserver<MessageDeliveryProto.RecordReply> responseObserver;
        MessageDeliveryProto.RecordReply reply;
        try {
            reply = blockingStub.replicateMsg(consumerRecordReq);
            if(reply.getCode() != 200) {
                // TODO: try again?
            }
        } catch (StatusRuntimeException e) {
            e.printStackTrace();
        }
    }

    void shutdown() throws InterruptedException {
        channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
    }

    public static void main(String[] args) throws InterruptedException {
    }
}
