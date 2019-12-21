package msgQ.consumer;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import org.apache.zookeeper.CreateMode;
import org.apache.curator.framework.CuratorFramework;

import java.io.IOException;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import msgQ.consumer.ConsumerUtils.State;
import msgQ.common.ZkUtils;
import msgQ.consumer.MessagePushProto.*;

public class Consumer {
    private final int PORT;
    private static final Logger logger = Logger.getLogger(Consumer.class.getName());
    private final Server server;
    UUID uuid;
    private State curState;
    private Set<String> subcriptions;
    private String zkPath;
    private CuratorFramework zkClient;
    private BlockingQueue<ConsumerRecord> records;

    public Consumer(Properties configs) {
        this.curState = State.LATENT;
        this.subcriptions = new HashSet<>();
        this.PORT = 43; // TODO: port number
        this.zkPath = configs.getProperty("path", "");
        this.zkClient = ZkUtils.newZkClient(zkPath, 1000,1000);
        this.records = new LinkedBlockingDeque<>();
        server = ServerBuilder.forPort(PORT).addService(new MessagePushService(records))
                .build();
    }

    /**
     * Start consumer
     */
    public void start() throws Exception {
        if(this.curState != State.LATENT) {
            throw new Exception(); //TODO: illegal exception
        }
        try {
            zkClient.create().creatingParentContainersIfNeeded().withMode(CreateMode.EPHEMERAL).forPath(this.zkPath);
        } catch (Exception e) {
            e.printStackTrace();
        }
        server.start();
        this.curState = State.STARTED;
        logger.info("Server started, listening on " + PORT);
    }

    /**
     * Terminate consumer
     */
    public void stop() throws Exception {
        if(this.curState != State.STARTED) {
            throw new Exception(); //TODO: illegal exception
        }
        this.curState = State.STOPPED;
        unsubscribeTopics(getSubscriptions());
        this.zkClient.close();
        server.shutdown().awaitTermination(30, TimeUnit.SECONDS);
    }

    private void _subscribeTopic(String topic) throws Exception {
        subcriptions.add(topic);
        String path = "subscribe/"+ topic + "consumer";
        zkClient.create().creatingParentContainersIfNeeded().forPath(path);
    }

    private void _unsubscribeTopic(String topic) throws Exception {
        subcriptions.remove(topic);
        String path =  "subscribe/" + "/"+ topic;
        zkClient.delete().deletingChildrenIfNeeded().forPath(path);
    }

    public void subscribeTopics(String[] topics) throws Exception {
        for(String topic: topics) {
            _subscribeTopic(topic);
        }
    }

    public void unsubscribeTopics(String[] topics) throws Exception {
        for(String topic: topics) {
            _unsubscribeTopic(topic);
        }
    }

    /**
     * return list of all subscribed topics
     *
     */
    public String[] getSubscriptions() {
        return subcriptions.toArray(new String[0]);
    }

    /**
     * printRecord
     * TODO: print newest records
     */
    private void printRecord() {
        records.poll();
    }

    private static class MessagePushService extends MessagePushGrpc.MessagePushImplBase {
        private final BlockingQueue<ConsumerRecord> records;

        MessagePushService(BlockingQueue<ConsumerRecord> _records) {
            this.records = _records;
        }

        @Override
        public void pushMsg(ConsumerRecordReq consumerRecordReq, StreamObserver<RecordReply> responseObserver) {
            responseObserver.onNext(checkFeature(consumerRecordReq));
            responseObserver.onCompleted();
        }

        private RecordReply checkFeature(ConsumerRecordReq recordReq) {
            records.offer(new ConsumerRecord(recordReq));
            return RecordReply.newBuilder().setUuid(recordReq.getUuid()).setMessage("ok").build();
        }
    }
}
