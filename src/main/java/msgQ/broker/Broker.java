package msgQ.broker;

import io.grpc.stub.StreamObserver;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.leader.LeaderLatch;
import org.apache.curator.framework.recipes.leader.LeaderLatchListener;
import org.apache.curator.utils.CloseableUtils;

import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Logger;

import msgQ.broker.BrokerUtils.*;
import msgQ.common.ZkUtils;
import msgQ.broker.MessageDeliveryProto.*;

import static msgQ.common.Constants.*;


public class Broker {
    private static final Logger LOGGER = Logger.getLogger(Broker.class.getName());
    private final int PORT;
    private final String PATH;
    private final String BROKERID;
    private String zkAddress;
    private int groupId;
    private State state;
    private boolean isLeader;
    private CuratorFramework zkClient;
    private LeaderLatch leaderLatch;
    // consumer -> records
    private ConcurrentHashMap<String, BlockingQueue<BrokerRecord>> recordsMap;
    private HashMap<String, MessagePushClient> messagePushClientsMap;
//    private MessagePushClient messagePushClient;
    AtomicLong timestamp;

    public Broker(int _groupId, Properties brokerConfigs, int _port) {
        BROKERID = brokerConfigs.getProperty("id");
        groupId = _groupId;
        PORT = _port;
        PATH = Paths.get(BROKER_PATH,"" + groupId).toString();
        zkAddress = brokerConfigs.getProperty("zkAddress");
        timestamp = new AtomicLong(0);
        isLeader = false;
        recordsMap = new ConcurrentHashMap<>();
        messagePushClientsMap = new HashMap<>();
//        messagePushClient = new MessagePushClient("localhost", PORT);
        state = State.LATENT;
    }

    private void register() {
        zkClient = ZkUtils.newZkClient(zkAddress, 5000, 50000);
        zkClient.start();
        try {
            zkClient.getZookeeperClient().blockUntilConnectedOrTimedOut();
            if (zkClient.checkExists().forPath(PATH) == null)
                zkClient.create().creatingParentsIfNeeded().forPath(PATH);
            registerLeaderElection();
        } catch (Exception e) {
            LOGGER.info("Connection failed");
            e.printStackTrace();
        }
    }

    /**
     * TODO: implement delivery thread
     */
    private void spawnDeliverThread() {
        new DeliverThread(this).start();
    }

    public void start() {
        this.register();
    }

    private void stop() {
        if (state != State.STARTED) {
            return; // TODO: throw exception
        }
        CloseableUtils.closeQuietly(leaderLatch);
        CloseableUtils.closeQuietly(zkClient);
    }

    private void registerLeaderElection() throws Exception {
        leaderLatch = new LeaderLatch(zkClient, PATH, BROKERID);
        leaderLatch.addListener(new LeaderLatchListener() {
            @Override
            public void isLeader() {
                isLeader = true;
                LOGGER.info("LEADER ELECTION: is now primary node");
                updatePrimaryBroker();
                spawnDeliverThread();
            }

            @Override
            public void notLeader() {
                isLeader = false;
                LOGGER.info("No longer leader");
            }
        });
        leaderLatch.start();
    }

    private void updatePrimaryBroker() {
        // TODO: update information in zookeeper
        try {
            if (zkClient.checkExists().forPath(PRIMARY_PATH) == null) {
                zkClient.create().creatingParentsIfNeeded().forPath(PRIMARY_PATH);
            }
        } catch (Exception e) {
            LOGGER.warning("updating failed");
        }
    }

    public List<String> getSubscribersForTopic(String topic) {
        String topicPath = Paths.get(REPLICA_PATH, topic).toString();
        try {
            return zkClient.getChildren().forPath(topicPath);
        } catch (Exception e) {
            LOGGER.warning(e.getMessage());
        }
        return new ArrayList<>();
    }

    /**
     * Handler for each single incoming record from producer
     * TODO: implement handler
     */
    private void incomingRecordHandler(BrokerRecord record) throws InterruptedException {
        String topic = record.getTopic();
        String message = (String) record.getValue();
        List<String> subscribers = getSubscribersForTopic(topic);
        for(String consumer: subscribers) {
            if(recordsMap.containsKey(consumer)) recordsMap.put(consumer, new LinkedBlockingQueue<>());
            recordsMap.get(consumer).put(record);
        }
    }

    public void sendNewRecords() throws InterruptedException {
        for(String consumer: recordsMap.keySet()) {
            if(!messagePushClientsMap.containsKey(consumer)) {
                int port = Integer.parseInt(consumer);
                messagePushClientsMap.put(consumer, new MessagePushClient(LOCALHOST, port));
            }
            messagePushClientsMap.get(consumer).pushMsg(recordsMap.get(consumer).take());

        }
    }

    public boolean isPrimary() {
        return isLeader;
    }

    public long getCurrentTimestamp() {
        return timestamp.get();
    }

    private static class MessageDeliveryService extends MessageDeliveryGrpc.MessageDeliveryImplBase {
        Broker broker;

        MessageDeliveryService(Broker _broker) {
            broker = _broker;
        }

        @Override
        public StreamObserver<MessageDeliveryProto.RecordReq> publishMsg(final StreamObserver<MessageDeliveryProto.RecordReply> responseObserver) {
            return new StreamObserver<MessageDeliveryProto.RecordReq>() {
                @Override
                public void onNext(MessageDeliveryProto.RecordReq recordReq) {
                    BrokerRecord record = new BrokerRecord(recordReq, broker.timestamp.addAndGet(1));
                    try {
                        broker.incomingRecordHandler(record);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }

                @Override
                public void onError(Throwable t) {
                }

                @Override
                public void onCompleted() {
                    responseObserver.onNext(MessageDeliveryProto.RecordReply.newBuilder()
                            .setUuid("").setMessage("ok").setCode(200).build());
                    responseObserver.onCompleted();
                }
            };
        }
    }
}
