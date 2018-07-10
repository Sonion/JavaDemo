package distributedLock;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.apache.curator.retry.ExponentialBackoffRetry;

import java.util.concurrent.TimeUnit;

/**
 * Created by lewis on 2018/07/09
 */
public class CuratorZookeeperLock {
    public static void main(String[] args) throws Exception {
        //创建zookeeper的客户端
        RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 1);
        CuratorFramework client = CuratorFrameworkFactory.newClient("127.0.0.1:2181", retryPolicy);
        client.start();

        //创建分布式锁, 锁空间的根节点路径为/curator/lock
        InterProcessMutex lock = new InterProcessMutex(client, "/curator/lock");

        //获得了锁, 进行业务流程
        Long maxWait = 10L;
        if ( lock.acquire(maxWait, TimeUnit.SECONDS) ){
            try
            {
                //获得了锁, 进行业务流程
            }
            finally
            {
                lock.release();
            }
        }
        //关闭客户端
        client.close();
    }
}
