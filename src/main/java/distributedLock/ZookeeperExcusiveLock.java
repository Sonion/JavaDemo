package distributedLock;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;

/**
 * Created by lewis on 2018/07/10
 */
public class ZookeeperExcusiveLock implements Watcher {
    private ZooKeeper zk;
    private static String root = "/locks";
    private static String lockName = root + "/lock";
    private CountDownLatch connectedLatch;      //用于连接初始化
    private CountDownLatch latch;               //用于wait
    private String host;
    private String port;
    private int timeOut;

    public ZookeeperExcusiveLock(String host, String port, int timeOut) {
        this.host = host;
        this.port = port;
        this.timeOut = timeOut;
        this.connectedLatch = new CountDownLatch(1);
    }

    public void process(WatchedEvent event) {
        if (Event.KeeperState.SyncConnected == event.getState()) {
            connectedLatch.countDown();
        }
        if (Event.EventType.NodeDeleted == event.getType()) {
            if (latch != null) {
                latch.countDown();
            }
        }
    }

    public void connected() throws IOException {
        zk = new ZooKeeper(host + ":" + port, timeOut, this);
        if (ZooKeeper.States.CONNECTING == zk.getState()) {
            try {
                connectedLatch.await();
            } catch (InterruptedException e) {
                throw new IllegalStateException(e);
            }
        }
    }

    public void lock() {
        try {
            // 没有获得锁，一直try
            while (!tryLock()) {
                // 等待锁
                waitForLock();
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public boolean tryLock() {
        try {
            //检查lock节点是否存在, 注册监听
            Stat stat = zk.exists(lockName, true);
            if (stat != null) {
                // 该节点已经创建，表示其他client持有锁
                System.out.println(lockName + " is hold by other client");
                return false;
            }
            String cLockNode = zk.create(lockName, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
            if (cLockNode != null && cLockNode.equals(lockName)) {
                System.out.println(lockName + " is created and hold the lock");
                return true;
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return false;
    }

    private boolean waitForLock() throws InterruptedException, KeeperException {
        // 检查时候存在lockName，并注册监听
        Stat stat = zk.exists(lockName, true);
        if (stat != null) {
            System.out.println("waiting for " + lockName);
            this.latch = new CountDownLatch(1);
            this.latch.await();
            this.latch = null;
        }
        return true;
    }

    public void unlock() {
        try {
            zk.delete(lockName, -1);
            System.out.println(lockName + " is release");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void disConnected() throws IOException {
        try {
            zk.close();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        String host = "localhost";
        String port = "2181";
        int timeOut = 2000;

        ZookeeperExcusiveLock excusiveLock = new ZookeeperExcusiveLock(host, port, timeOut);
        excusiveLock.connected();
        try {
            excusiveLock.lock();
            Thread.sleep(3000);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            excusiveLock.unlock();
            excusiveLock.disConnected();
        }
    }
}
