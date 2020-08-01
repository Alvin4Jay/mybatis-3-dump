/**
 * Copyright 2009-2020 the original author or authors.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.ibatis.cache.decorators;

import org.apache.ibatis.cache.Cache;
import org.apache.ibatis.logging.Log;
import org.apache.ibatis.logging.LogFactory;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * The 2nd level cache transactional buffer. 只能避免 脏读，不能避免 不可重复读
 * <p>
 * This class holds all cache entries that are to be added to the 2nd level cache during a Session.
 * Entries are sent to the cache when commit is called or discarded if the Session is rolled back.
 * Blocking cache support has been added. Therefore any get() that returns a cache miss
 * will be followed by a put() so any lock associated with the key can be released.
 *
 * @author Clinton Begin
 * @author Eduardo Macarron
 */
public class TransactionalCache implements Cache {

    private static final Log log = LogFactory.getLog(TransactionalCache.class);

    /** 被装饰者 */
    private final Cache delegate;
    /** 在事务被提交前，所有从数据库中查询的结果将缓存在此集合中 */
    private final Map<Object, Object> entriesToAddOnCommit;
    /** 在事务被提交前，当缓存未命中时，CacheKey 将会被存储在此集合中 */
    private final Set<Object> entriesMissedInCache;
    /** 事务提交时是否清除缓存 */
    private boolean clearOnCommit;

    public TransactionalCache(Cache delegate) {
        this.delegate = delegate;
        this.clearOnCommit = false;
        this.entriesToAddOnCommit = new HashMap<>();
        this.entriesMissedInCache = new HashSet<>();
    }

    @Override
    public String getId() {
        return delegate.getId();
    }

    @Override
    public int getSize() {
        return delegate.getSize();
    }

    @Override
    public Object getObject(Object key) {
        // issue #116 查询 delegate key所代表的缓存
        Object object = delegate.getObject(key);
        if (object == null) {
            // 缓存未命中，则将 key 存入到 entriesMissedInCache 中
            entriesMissedInCache.add(key);
        }
        // issue #146
        if (clearOnCommit) {
            return null;
        } else {
            return object;
        }
    }

    @Override
    public void putObject(Object key, Object object) {
        // 将键值对存入到 entriesToAddOnCommit 中，而非 delegate 缓存中
        entriesToAddOnCommit.put(key, object);
    }

    @Override
    public Object removeObject(Object key) {
        return null;
    }

    @Override
    public void clear() {
        clearOnCommit = true;
        // 清空 entriesToAddOnCommit，但不清空 delegate 缓存
        entriesToAddOnCommit.clear();
    }

    public void commit() {
        // 根据 clearOnCommit 的值决定是否清空 delegate
        if (clearOnCommit) {
            delegate.clear();
        }
        // 刷新未缓存的结果到 delegate 缓存中
        flushPendingEntries();
        // 重置 entriesToAddOnCommit 和 entriesMissedInCache
        reset();
    }

    public void rollback() {
        unlockMissedEntries();
        reset();
    }

    private void reset() {
        clearOnCommit = false;
        // 清空集合
        entriesToAddOnCommit.clear();
        entriesMissedInCache.clear();
    }

    private void flushPendingEntries() {
        for (Map.Entry<Object, Object> entry : entriesToAddOnCommit.entrySet()) {
            // 将 entriesToAddOnCommit 中的内容转存到 delegate 中
            delegate.putObject(entry.getKey(), entry.getValue());
        }
        for (Object entry : entriesMissedInCache) {
            if (!entriesToAddOnCommit.containsKey(entry)) {
                // 存入空值
                delegate.putObject(entry, null);
            }
        }
    }

    private void unlockMissedEntries() {
        for (Object entry : entriesMissedInCache) {
            try {
                // 调用 removeObject 进行解锁
                delegate.removeObject(entry);
            } catch (Exception e) {
                log.warn("Unexpected exception while notifiying a rollback to the cache adapter. "
                    + "Consider upgrading your cache adapter to the latest version. Cause: " + e);
            }
        }
    }

}
