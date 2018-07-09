package distributedLock;

import lombok.AllArgsConstructor;
import lombok.Data;
/**
 * Created by lewis on 2018/07/09
 */

@Data
@AllArgsConstructor
public class LockRes {
    // 是否拿到锁，false：没拿到，true：拿到
    private boolean flag;
    // 缓存的键
    private String key;
    // 缓存的值
    private String value;
}

