package msgQ.consumer;


import io.grpc.stub.StreamObserver;

public class MessagePushThread extends MessagePushGrpc.MessagePushImplBase {
    @Override
    public void pushMsg(ConsumerMessagePush.RecordReq req, StreamObserver<ConsumerMessagePush.RecordReply> responseObserver) {
        ConsumerMessagePush.RecordReply reply = ConsumerMessagePush.RecordReply.newBuilder()
                .setMessage("ok")
                .build();
//        req.getTopic()
        responseObserver.onNext(reply);
        responseObserver.onCompleted();
    }
}
