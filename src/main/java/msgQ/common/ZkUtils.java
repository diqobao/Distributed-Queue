package msgQ.common;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;

public class ZkUtils {

    static RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);

    /*
    * */
    public static CuratorFramework newZkClient(String connectionString, int connectionTimeoutMs, int sessionTimeoutMs) {
        return CuratorFrameworkFactory.builder()
                .connectString(connectionString)
                .retryPolicy(retryPolicy)
                .connectionTimeoutMs(connectionTimeoutMs)
                .sessionTimeoutMs(sessionTimeoutMs)
                .build();
    }

    /*
    * */
    //LeaderSelectorListener listener = new LeaderSelectorListenerAdapter()
    //{
    //    public void takeLeadership(CuratorFramework client) throws Exception
    //    {
    //        // this callback will get called when you are the leader
    //        // do whatever leader work you need to and only exit
    //        // this method when you want to relinquish leadership
    //    }
    //}
    //
    //LeaderSelector selector = new LeaderSelector(client, path, listener);
    //selector.autoRequeue();  // not required, but this is behavior that you will probably expect
    //selector.start();
}
