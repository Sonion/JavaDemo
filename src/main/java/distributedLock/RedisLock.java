package distributedLock;

import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
/**
 * Created by lewis on 2018/07/06
 */
public class RedisLock implements Lock {
    final static Logger LOG = LoggerFactory.getLogger(RedisLockDemo.class);
    static JedisPool jedisPool = null;
    static {
        JedisPoolConfig config = new JedisPoolConfig();
        config.setMaxTotal(1000);
        jedisPool = new JedisPool(config, "127.0.0.1");
    }
    private final static int GOT_LOCK = 1;
//    private String key_redis = "redisLock__" + UUID.randomUUID();
        private String key_redis = "exists-key2";


    @Override
    public void lock() {
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            //lock:setnx && expire
            while (!Thread.interrupted()) {// 自旋
                if ("OK".equals(jedis.set(key_redis, "true"))) {// 成功获取锁
                    LOG.info("add lock: " + key_redis);
                    jedis.expire(key_redis, 20);
                    return;
                }
            }
        } finally {
            if (null != jedis) {
                jedis.close();
            }
        }
    }

    @Override
    public void unlock() {
        LOG.info("-0-------------------------------------------------");
        Jedis jedis = null;
        //unlock：delete

        try {
            jedis = jedisPool.getResource();
            LOG.info("-1-------------------------------------------------");
            LOG.info("unlock: " + key_redis + "success " + jedis.del(key_redis));
        } finally {
            if (null != jedis) {
                jedis.close();
            }
        }
    }

    @Override
    public void lockInterruptibly() throws InterruptedException {

    }

    @Override
    public boolean tryLock() {
        return false;
    }

    @Override
    public boolean tryLock(long time, TimeUnit unit) throws InterruptedException {
        return false;
    }

    @Override
    public Condition newCondition() {
        return null;
    }

    public static void main(String[] args) {
        for (int i = 0; i < 100; i++) {
            Jedis jedis = jedisPool.getResource();
            System.out.println(jedis);
            jedis.close();

        }
    }
}

