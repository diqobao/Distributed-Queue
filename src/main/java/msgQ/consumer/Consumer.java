package msgQ.consumer;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import msgQ.common.ZkUtils;

public class Consumer {

    int uuid;
    private State curState;
    private Set<String> subcriptions;
    private String zkPath;
    private CuratorFramework zkClient;
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
            this.curState = State.STARTED;
        } catch (Exception e) {

        }
    }

    /**
     * Terminate consumer
     *
     */
    public void stop() throws Exception {
        if(this.curState != State.STARTED) {
            throw new Exception(); //TODO: illegal exception
        }
        this.zkClient.close();
    }

    /**
     * Subscribe to topics
     *
     */
    public void subscribe() throws Exception {

    }

    /**
     * Unsubscribe to topics
     *
     */
    public void unsubscribe() throws Exception {

    }

    /**
     * return list of all subscribed topics
     *
     */
    public List<String> getSubscriptions() {
        List<String> listOfSubscriptions = new ArrayList<>(subcriptions.size());
        // TODO: imp
        return listOfSubscriptions;
    }
}