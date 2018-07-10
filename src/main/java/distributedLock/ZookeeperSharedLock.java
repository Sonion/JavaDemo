package distributedLock;

import org.apache.zookeeper.*;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.*;
import java.util.concurrent.CountDownLatch;

/**
 * Created by lewis on 2018/07/10
 */
public class ZookeeperSharedLock implements Watcher {
        private ZooKeeper zk;
        private static final String root = "/locks";
        private static final String SPLIT = "-";
        private static final String TYPE_WRITE  = "W";
        private static final String TYPE_READ   = "R";
        private CountDownLatch connectedLatch;      //用于连接初始化
        private CountDownLatch latch;               //用于wait
        private String lockName;
        private String host;
        private String port;
        private String ip;
        private int timeOut;

        public ZookeeperSharedLock(String host, String port, int timeOut) {
            this.host = host;
            this.port = port;
            this.timeOut = timeOut;
            this.connectedLatch = new CountDownLatch(1);
            try {
                this.ip = InetAddress.getLocalHost().getHostAddress();
            } catch (UnknownHostException e) {
                e.printStackTrace();
            }
        }

        public void process(WatchedEvent event) {
            if(Event.KeeperState.SyncConnected == event.getState()){
                connectedLatch.countDown();
            }
            if(Event.EventType.NodeChildrenChanged==event.getType()){
                if(latch!=null){
                    latch.countDown();
                }
            }
        }

        public void connected() throws IOException {
            zk = new ZooKeeper(host+":"+port,timeOut, this);
            if (ZooKeeper.States.CONNECTING == zk.getState()) {
                try {
                    connectedLatch.await();
                } catch (InterruptedException e) {
                    throw new IllegalStateException(e);
                }
            }
            System.out.println("connected!!!!");
        }

        private String getSeq(){
            if(lockName==null)
                return null;
            return lockName.substring(lockName.lastIndexOf(SPLIT));
        }

        public void lock(String type) {
            try {
                while(!tryLock(ip, type, getSeq())){
                    // 等待锁
                    waitForLock();
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        public class LockNode implements Comparable{
            private String ip;
            private String type;
            private String seq;
            private String node;
            private int order;

            public LockNode(String node,String root) {
                if(node!=null){
                    String[] arr = node.split(SPLIT);
                    this.ip=arr[0];
                    this.type=arr[1];
                    this.seq=arr[2];
                }
                this.node = root+"/"+node;
            }

            @Override
            public int compareTo(Object o) {
                LockNode lockNode = (LockNode)o;
                return Integer.valueOf(this.seq)-Integer.valueOf(lockNode.seq);
            }

            public String getType() {
                return type;
            }

            public String getNode() {
                return node;
            }

            public int getOrder() {
                return order;
            }

            public void setOrder(int order) {
                this.order = order;
            }
        }

        /**
         * node format: 192.168.0.1-R-000000012
         * @param type
         * @return
         */
        public boolean tryLock(String ip, String type, String seq) {
            try {
                lockName = root+"/"+ip+SPLIT+type+SPLIT;

                if(seq==null||seq.length()==0){
                    lockName=zk.create(lockName, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
                }else{
                    lockName += seq;
                }
                List<String> childred = zk.getChildren(root,false);
                List<LockNode> lockNodes = new ArrayList<LockNode>();
                for(String child:childred){
                    lockNodes.add(new LockNode(child,root));
                }
                // 排序，排序完了存入map,便于查找
                Collections.sort(lockNodes);
                Map<String,LockNode> lockNodeMap = new HashMap<String, LockNode>();
                for(int i=0;i<lockNodes.size();i++){
                    LockNode ln = lockNodes.get(i);
                    ln.setOrder(i);
                    lockNodeMap.put(ln.getNode(),ln);
                }

                LockNode lockNode = lockNodeMap.get(lockName);

                // 对于读和写，如果都是第一个节点，持有锁
                if(lockNode.getOrder()==0){
                    return true;
                }
                // 对于序号未排在第一的读请求，如果比其需要小的都是读请求，也能持有锁
                if(TYPE_READ.equals(type)){
                    for(int i=0;i<lockNode.getOrder();i++){
                        LockNode ln = lockNodes.get(i);
                        if(TYPE_WRITE.equals(ln.getType())){
                            System.out.println(lockName + " is holded by other client");
                            return false;
                        }
                    }
                    System.out.println(lockName + " hold lock!!!!");
                    return true;
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            return false;
        }

        private boolean waitForLock() throws InterruptedException, KeeperException {
            // 检查root下面子节点，并注册监听
            List<String> children = zk.getChildren(root ,true);
            if(children != null && !children.isEmpty()){
                System.out.println("waiting for " + lockName);
                this.latch = new CountDownLatch(1);
                this.latch.await();
                this.latch = null;
            }
            return true;
        }

        public void unlock() {
            if(lockName==null || lockName.length()==0)
                return;
            try {
                zk.delete(lockName, -1);
                System.out.println(lockName + " is release");
                this.lockName = null;
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

        public static void main(String[] args) throws IOException, InterruptedException, KeeperException {
            String host = "127.0.0.1";
            String port = "2181";
            int timeOut = 20000;

            ZookeeperSharedLock excusiveLock = new ZookeeperSharedLock(host, port, timeOut);
            excusiveLock.connected();

            try {
                excusiveLock.lock(TYPE_READ);
                Thread.sleep(5000);
                System.out.println("sleep end , start release lock");
            }catch (Exception e){
                e.printStackTrace();
            }finally {
                excusiveLock.unlock();
                excusiveLock.disConnected();
            }
        }
    }