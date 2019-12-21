package msgQ.broker;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.leader.LeaderLatch;

import msgQ.common.ZkUtils;


public class Broker {
    private int groupId;
    private CuratorFramework zkClient;

    public Broker() {
        groupId = 0;
        zkClient = ZkUtils.newZkClient("", 5000, 50000);
    }

    private void register() {
        zkClient.start();
    }

    private void spawnDeliverThread() {

    }

    public void start() {
        this.register();
    }
}
