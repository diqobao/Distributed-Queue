package msgQ.consumer;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import org.apache.curator.framework.CuratorFramework;
import java.nio.file.Paths;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

import msgQ.consumer.ConsumerUtils.State;
import msgQ.consumer.MessagePushProto.*;
import static msgQ.common.Constants.SUBSCRIPTION_PATH;

public class Consumer {
    private final int PORT;
    private static final Logger LOGGER = Logger.getLogger(Consumer.class.getName());
    private final Server server;
    UUID uuid;
    private State curState;
    private Set<String> subcriptions;
    private String address;
    private String zkPath;
    private CuratorFramework zkClient;
    private BlockingQueue<ConsumerRecord> records;
    private Thread recvThread;

    public Consumer(Properties configs) {
        this.curState = State.LATENT;
        this.subcriptions = new HashSet<>();
        this.PORT = 5001;
        this.zkPath = configs.getProperty("path", "");
//        this.zkClient = ZkUtils.newZkClient(zkPath, 1000, 1000);
        this.records = new LinkedBlockingDeque<>();
        this.server = ServerBuilder.forPort(PORT).addService(new MessagePushService(records))
                .build();
        this.recvThread = new RecvThread(this);
    }

    /**
     * Start consumer
     */
    public synchronized void start() throws Exception {
        if(this.curState != State.LATENT) {
            throw new Exception(); //TODO: illegal exception
        }
//        try {
//            zkClient.create().creatingParentContainersIfNeeded().withMode(CreateMode.EPHEMERAL).forPath(this.zkPath);
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
        server.start();
        recvThread.start();
        this.curState = State.STARTED;
        LOGGER.info("Server started, listening on " + PORT);
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
        recvThread.join();
    }

    /**
     * return list of all subscribed topics
     *
     */
    public String[] getSubscriptions() {
        return subcriptions.toArray(new String[0]);
    }

    public void pollRecord() {
        if(!records.isEmpty()) {
            System.out.println(records.poll().toString());
        }
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

    private void _subscribeTopic(String topic) throws Exception {
        subcriptions.add(topic);
        String path  = Paths.get(SUBSCRIPTION_PATH, topic, "" + PORT).toString();
        zkClient.create().creatingParentContainersIfNeeded().forPath(path);
    }

    private void _unsubscribeTopic(String topic) throws Exception {
        subcriptions.remove(topic);
        String path = Paths.get(SUBSCRIPTION_PATH, topic, "" + PORT).toString();
        zkClient.delete().deletingChildrenIfNeeded().forPath(path);
    }

    /**
     * printRecord
     * TODO: print newest records
     */
    private void printRecord() {
        records.poll().toString();
    }

    private void blockUntilShutdown() throws InterruptedException {
        if (server != null) {
            server.awaitTermination();
        }
    }

    private static class MessagePushService extends MessagePushGrpc.MessagePushImplBase {
        private final BlockingQueue<ConsumerRecord> records;

        MessagePushService(BlockingQueue<ConsumerRecord> _records) {
            this.records = _records;
        }

        @Override
        public void pushMsg(ConsumerRecordReq consumerRecordReq, StreamObserver<RecordReply> responseObserver) {
            responseObserver.onNext(addNewRecord(consumerRecordReq));
            responseObserver.onCompleted();
        }

        private RecordReply addNewRecord(ConsumerRecordReq recordReq) {
            records.offer(new ConsumerRecord(recordReq));
            return RecordReply.newBuilder().setUuid(recordReq.getUuid()).setMessage("ok").build();
        }
    }

    public static void main(String[] args) throws Exception {
//        Properties configs = new Properties();
//        configs.put("host", "localhost");
//        configs.put("port", 5001);
//        configs.put("zkAddr", "ZkAddr");
//        Consumer consumer = new Consumer(configs);
//        consumer.start();
//        consumer.blockUntilShutdown();
        String path = Paths.get("localhost", "2332").toString();
        System.out.println(path);
    }
}
