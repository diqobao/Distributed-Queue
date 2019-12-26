package msgQ.broker;

import io.grpc.stub.StreamObserver;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.leader.LeaderLatch;
import org.apache.curator.framework.recipes.leader.LeaderLatchListener;
import org.apache.curator.utils.CloseableUtils;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Logger;

import msgQ.broker.BrokerUtils.*;
import msgQ.common.ZkUtils;
import msgQ.broker.MessageDeliveryProto.*;


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
    private ConcurrentHashMap<String, ConcurrentHashMap<String, BlockingQueue<?>>> recordsMap;
    private MessagePushClient messagePushClient;
    private long timestamp;

    public Broker(int _groupId, Properties brokerConfigs, int _port) {
        BROKERID = brokerConfigs.getProperty("id");
        groupId = _groupId;
        PORT = _port;
        PATH = String.format("/%s/%d", "brokers", groupId);
        zkAddress = brokerConfigs.getProperty("zkAddress");
        timestamp = 0;
        isLeader = false;
        recordsMap = new ConcurrentHashMap<>();
        messagePushClient = new MessagePushClient("localhost", PORT);
        state = State.LATENT;
    }

    private void register() {
        zkClient = ZkUtils.newZkClient(zkAddress, 5000, 50000);
        zkClient.start();
        try {
            zkClient.getZookeeperClient().blockUntilConnectedOrTimedOut();
            if (zkClient.checkExists().forPath(PATH) == null)
                zkClient.create().creatingParentsIfNeeded().forPath(PATH);
            leaderLatch = new LeaderLatch(zkClient, PATH, BROKERID);
            leaderLatch.addListener(new LeaderLatchListener() {
                @Override
                public void isLeader() {
                    isLeader = true;
                    LOGGER.info("LEADER ELECTION: is now primary node");
//                    if (zkClient.checkExists().forPath(PATH) == null)
//                        zkClient.create().creatingParentsIfNeeded().forPath(PATH);
                    spawnDeliverThread();
                    // TODO: update information in zookeeper
                }

                @Override
                public void notLeader() {
                    isLeader = false;
                    LOGGER.info("no longer leader");
                }
            });
            leaderLatch.start();
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

    /**
     * Handler for each single incoming record from producer
     * TODO: implement handler
     */
    private void incomingRecordHandler(BrokerRecord record) {
        String topic = record.getTopic();
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
                    BrokerRecord record = new BrokerRecord(); // TODO: implement create the record
                    broker.incomingRecordHandler(record);
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
