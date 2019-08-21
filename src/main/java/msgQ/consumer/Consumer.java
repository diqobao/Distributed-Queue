package msgQ.consumer;

import com.linecorp.armeria.server.Server;
import com.linecorp.armeria.server.ServerBuilder;
import com.linecorp.armeria.server.grpc.GrpcServiceBuilder;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import java.util.*;
import java.util.concurrent.BlockingQueue;

import msgQ.common.ZkUtils;
import org.apache.zookeeper.CreateMode;

public class Consumer {

    UUID uuid;
    private State curState;
    private Set<String> subcriptions;
    private String zkPath;
    private CuratorFramework zkClient;
    private HashMap<String, BlockingQueue<ConsumerRecord>> records;
    private enum State
    {
        LATENT,
        STARTED,
        STOPPED
    }


    public Consumer(Properties configs) {
        this.curState = State.LATENT;
        this.subcriptions = new HashSet<>();
        this.zkPath = configs.getProperty("path", "");
        this.zkClient = ZkUtils.buildZkClient(this.zkPath, 1000,1000);
    }

    /**
     * Start consumer
     *
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
        ServerBuilder sb = new ServerBuilder();
        sb.service(new GrpcServiceBuilder().addService(new MessagePushThread())
                .build());
        Server server = sb.build();
        server.start();
        this.curState = State.STARTED;
    }

    /**
     * Terminate consumer
     *
     */
    public void stop() throws Exception {
        if(this.curState != State.STARTED) {
            throw new Exception(); //TODO: illegal exception
        }
        unsubscribeTopics(getSubscriptions());
        this.zkClient.close();
        this.curState = State.STOPPED;
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
}