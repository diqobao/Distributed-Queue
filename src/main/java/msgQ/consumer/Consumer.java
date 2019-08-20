package msgQ.consumer;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import java.util.*;
import java.util.concurrent.BlockingQueue;

import msgQ.common.ZkUtils;

public class Consumer {

    int uuid;
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

    /**
    * Set up a new consumer
    *
    */
    public Consumer(String path) {
        this.curState = State.LATENT;
        this.subcriptions = new HashSet<>();
        this.zkPath = path;
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
            zkClient.create().creatingParentContainersIfNeeded().forPath(this.zkPath);
        } catch (Exception e) {

        }
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
        // TODO: Unsubscribe all topics
        this.zkClient.close();
        this.curState = State.STOPPED;
    }

    /**
     * Subscribe to topics
     *
     */
    public void subscribeTopic(String topic) throws Exception {
        subcriptions.add(topic);

    }

    /**
     * Unsubscribe to topics
     *
     */
    public void unsubscribeTopic(String topic) throws Exception {

    }

    /**
     * return list of all subscribed topics
     *
     */
    public String[] getSubscriptions() {
        return subcriptions.toArray(new String[0]);
    }
}