package distributedLock;

import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.StringRedisTemplate;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisCluster;

import java.util.Collections;
import java.util.UUID;

/**
 * Created by lewis on 2018/07/09
 */
public class NewRedisLock {
    public static final String OK = "OK";
    //其实很简单，首先获取锁对应的value值，检查是否与加锁的lockValue相等，如果相等则删除锁（解锁）。
    // 那么为什么要使用Lua语言来实现呢？因为要确保此操作是原子性的。
    //简单来说，就是在eval命令执行Lua代码的时候，Lua代码将被当成一个命令去执行，并且直到eval命令执行完成，Redis才会执行其他命令。
    public static final String UNLOCK_LUA = "if redis.call('get', KEYS[1]) == ARGV[1] then return redis.call('del', KEYS[1]) else return 0 end";

    /**
     * 加锁
     * @param keyName redis key name
     * @param stringRedisTemplate stringRedisTemplate
     * @param expireSeconds 锁定的最大时长
     * @return 锁定结果
     */
    public static LockRes tryLock(String keyName, StringRedisTemplate stringRedisTemplate, Integer expireSeconds) {

        // 将value设置为当前时间戳+随机数
        String lockValue = System.currentTimeMillis() + UUID.randomUUID().toString();

        String redisLockResult = stringRedisTemplate.execute((RedisCallback<String>) connection -> {
            Object nativeConnection = connection.getNativeConnection();
            String result = null;
            // 集群
            if (nativeConnection instanceof JedisCluster) {
                result = ((JedisCluster) nativeConnection).set(keyName, lockValue, "NX", "EX", expireSeconds);
            }
            // 单机
            if (nativeConnection instanceof Jedis) {
                result = ((Jedis) nativeConnection).set(keyName, lockValue, "NX", "EX", expireSeconds);
            }
            return result;
        });

        if (OK.equalsIgnoreCase(redisLockResult)) {
            return new LockRes(true, keyName, lockValue);
        } else {
            return new LockRes(false, keyName, null);
        }
    }

    /**
     * 释放锁
     * @param lockRes
     * @param stringRedisTemplate
     * @return
     */
    public static Boolean unlock(LockRes lockRes, StringRedisTemplate stringRedisTemplate) {
        if (lockRes.isFlag()) {
            return stringRedisTemplate.execute((RedisCallback<Boolean>) connection -> {
                Object nativeConnection = connection.getNativeConnection();
                Long result = 0L;

                // 集群
                if (nativeConnection instanceof JedisCluster) {
                    result = (Long) ((JedisCluster) nativeConnection).eval(UNLOCK_LUA, Collections.singletonList(lockRes.getKey()), Collections.singletonList(lockRes.getValue()));
                }

                // 单机
                if (nativeConnection instanceof Jedis) {
                    result = (Long) ((Jedis) nativeConnection).eval(UNLOCK_LUA, Collections.singletonList(lockRes.getKey()), Collections.singletonList(lockRes.getValue()));
                }

                return result == 1L;
            });
        } else {
            return true;
        }
    }
}
