package distributedLock;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.Transaction;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by lewis on 2018/07/06
 */
public class RedisWatchLock {
    private static final String redisHost = "127.0.0.1";
    private static final int port = 6379;
    private static JedisPoolConfig config;
    private static JedisPool pool;

    private static ExecutorService service;
    private static int count = 10;

    private static CountDownLatch latch;
    private static AtomicInteger Countor = new AtomicInteger(0);
    static {
        config = new JedisPoolConfig();
        config.setMaxIdle(10);
        config.setMaxWaitMillis(1000);
        config.setMaxTotal(30);
        pool = new JedisPool(config, redisHost, port);
        service = Executors.newFixedThreadPool(10);

        latch = new CountDownLatch(count);
    }
    public static void main(String args[]) {
        int count = 10;
        String ThreadNamePrefix = "thread-";
        Jedis cli = pool.getResource();
        cli.del("redis_inc_key");// 先删除既定的key
        cli.set("redis_inc_key", String.valueOf(1));// 设定默认值
        for (int i = 0; i < count; i++) {
            Thread th = new Thread(new TestThread(pool));
            th.setName(ThreadNamePrefix + i);
            System.out.println(th.getName() + "inited...");
            service.submit(th);
        }
        service.shutdown();
        try {
            latch.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        System.out.println("all sub thread sucess");
        System.out.println("countor is " + Countor.get());
        String countStr = cli.get("redis_inc_key");
        System.out.println(countStr);
    }
    public static class TestThread implements Runnable {
        private String incKeyStr = "redis_inc_key";
        private Jedis cli;
        private JedisPool pool;
        public TestThread(JedisPool pool) {
            cli = pool.getResource();
            this.pool = pool;
        }
        public void run() {
            try {
                for (int i = 0; i < 100; i++) {
                    actomicAdd();//生产环境中批量操作尽量使用redisPipeLine!!
                }
            } catch (Exception e) {
                pool.returnBrokenResource(cli);
            } finally {
                pool.returnResource(cli);
                latch.countDown();
            }
        }
        /**
         * 0 watch key
         * 1 multi
         * 2 set key value(queued)
         * 3 exec
         *
         * return null：fail
         * reurn  "ok": succeed
         *
         * watch每次都需要执行(注册)
         */
        public void actomicAdd() {
            cli.watch(incKeyStr);// 0.watch key
            boolean flag = true;
            while (flag) {
                String countStr = cli.get("redis_inc_key");
                int countInt = Integer.parseInt(countStr);
                int expect = countInt + 1;
                Transaction tx = cli.multi(); // 1.multi
                tx.set(incKeyStr, String.valueOf(expect));// 2.set key value
                // (queued)
                List<Object> list = tx.exec();// 3.exec
                if (list == null) {
                    System.out.println("fail");
                    continue;
                } else {
                    flag = false;
                    System.out.println("succeed");
                }
                System.out.println("my expect num is " + expect);
                System.out.println("seting....");
            }
            Countor.incrementAndGet();
        }
    }
}
