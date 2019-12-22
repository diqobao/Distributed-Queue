package msgQ.broker;

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
    private ConcurrentHashMap<String, BlockingQueue<?>> recordsMap;
    private MessagePushClient messagePushClient;
    private long timestamp;

    public Broker(int _groupId, Properties brokerConfigs, int _port) {
        BROKERID = brokerConfigs.getProperty("id");
        groupId = _groupId;
        PORT = _port;
        PATH = String.format("/%s/%d", "replica", groupId);
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
                    spawnDeliverThread();
                    // TODO: update information in zookeeper
                }

                @Override
                public void notLeader() {
                    LOGGER.info("no longer leader");
                }
            });
            leaderLatch.start();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * */
    private void spawnDeliverThread() {

    }

    public void start() {
        this.register();
    }

    private void stop() {
        if(state != State.STARTED) {
            return ; // TODO: throw exception
        }
        CloseableUtils.closeQuietly(leaderLatch);
        CloseableUtils.closeQuietly(zkClient);
    }
}
