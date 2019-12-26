package msgQ.producer;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;

import msgQ.broker.MessageDeliveryGrpc;
import msgQ.broker.MessageDeliveryProto.RecordReply;
import msgQ.broker.MessageDeliveryProto.RecordReq;


class MessageDeliveryClient {
    private static final Logger logger = Logger.getLogger(MessageDeliveryClient.class.getName());

    private final ManagedChannel channel;
    private final MessageDeliveryGrpc.MessageDeliveryStub asyncStub;
    private final MessageDeliveryGrpc.MessageDeliveryBlockingStub blockingStub;

    MessageDeliveryClient(String host, int port) {
        ManagedChannelBuilder channelBuilder = ManagedChannelBuilder.forAddress(host, port).usePlaintext();
        channel = channelBuilder.build();
        blockingStub = MessageDeliveryGrpc.newBlockingStub(channel);
        asyncStub = MessageDeliveryGrpc.newStub(channel);
    }

    MessageDeliveryClient(ManagedChannelBuilder channelBuilder) {
        channel = channelBuilder.build();
        blockingStub = MessageDeliveryGrpc.newBlockingStub(channel);
        asyncStub = MessageDeliveryGrpc.newStub(channel);
    }

    MessageDeliveryClient(ManagedChannel _channel) {
        channel = _channel;
        blockingStub = MessageDeliveryGrpc.newBlockingStub(channel);
        asyncStub = MessageDeliveryGrpc.newStub(channel);
    }

    public void publishMsg(List<ProducerRecord> producerRecords) throws InterruptedException {
        final CountDownLatch finishLatch = new CountDownLatch(1);
        StreamObserver<RecordReply> responseObserver = new StreamObserver<RecordReply>() {
            @Override
            public void onNext(RecordReply recordReply) {
            }

            @Override
            public void onError(Throwable t) {
                logger.warning(t.getMessage());
                finishLatch.countDown();
            }

            @Override
            public void onCompleted() {
                logger.info("Finished batch");
                finishLatch.countDown();
            }
        };

        StreamObserver<RecordReq> requestObserver = asyncStub.publishMsg(responseObserver);
        try {
            for (ProducerRecord producerRecord : producerRecords) {
                RecordReq recordReq = producerRecord.toRecordReq();
                // Sleep for a bit before sending the next one.
                requestObserver.onNext(recordReq);
                Thread.sleep(100);
                if (finishLatch.getCount() == 0) {
                    return;
                }
            }
        } catch (RuntimeException e) {
            // Cancel RPC
            requestObserver.onError(e);
            throw e;
        }
        requestObserver.onCompleted();
        if (!finishLatch.await(1, TimeUnit.MINUTES)) {
            logger.warning("request can not finish within 1 minutes");
        }
    }

    public void shutdown() throws InterruptedException {
        channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
    }
}
