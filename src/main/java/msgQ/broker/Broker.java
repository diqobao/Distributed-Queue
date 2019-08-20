package msgQ.broker;

import msgQ.common.ZkUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.leader.LeaderLatch;

public class Broker {
    int groupId;
    CuratorFramework zkClient;
    public Broker() {

    }

    private void register() {

    }

    private void SpawnDeliverThread() {

    }

    public void start() {
        this.register();
    }
}
