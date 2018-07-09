package distributedLock;

/**
 * Created by lewis on 2018/07/06
 */
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RedisLockDemo {
    static long i = 0l;

    final static Logger LOG = LoggerFactory.getLogger(RedisLockDemo.class);
    static RedisLock redisLock = new RedisLock();

    public static void main(String[] args) throws InterruptedException {
        new RedisLockDemo().testWithLock();
//        new RedisLockDemo().testWithOutLock();
    }

    public void testWithLock() throws InterruptedException {
        ExecutorService ex = Executors.newFixedThreadPool(100);
        CountDownLatch countDownLatch = new CountDownLatch(1000);
        for (int i = 0; i < 1000; i++) {
            ex.execute(getTaskWithLock(countDownLatch));
        }
        countDownLatch.await();
        System.out.println("before shut down " + i);
        ex.shutdown();
    }

    public void testWithOutLock() throws InterruptedException {
        ExecutorService ex = Executors.newFixedThreadPool(100);
        CountDownLatch countDownLatch = new CountDownLatch(1000);
        for (int i = 0; i < 1000; i++) {
            ex.execute(getTaskWithOutLock(countDownLatch));
        }
        countDownLatch.await();
        System.out.println("before shut down " + i);
        ex.shutdown();
    }

    private static Runnable getTaskWithOutLock(final CountDownLatch countDownLatch) {
        return new Runnable() {
            @Override
            public void run() {
                // redisLock.lock();
                // do some business
                try {
                    i++;
                    Thread.yield();
                    i--;
                    Thread.yield();
                    i++;
                    LOG.info("business...............       ." + Thread.currentThread().getName());
                    // TimeUnit.SECONDS.sleep(1);
                } finally {
                    // redisLock.unlock();
                    countDownLatch.countDown();
                }
            }
        };
    }

    private static Runnable getTaskWithLock(final CountDownLatch countDownLatch) {
        return new Runnable() {
            @Override
            public void run() {
                redisLock.lock();
                // do some business
                try {
                    i++;
                    i--;
                    i++;
                    LOG.info("business...............       ." + Thread.currentThread().getName());
                    // TimeUnit.SECONDS.sleep(1);
                } finally {
                    redisLock.unlock();
                    countDownLatch.countDown();
                }
            }
        };
    }
}