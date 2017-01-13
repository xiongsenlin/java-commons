package org.xsl.common.cache;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalListener;
import org.xsl.common.base.Callback;

import java.util.concurrent.TimeUnit;

/**
 * 对guava中{@link Cache}的封装，维护了一个缓存队列，缓存队列的超时时间呈指数增长，例如第一个队列
 * 的超时时间为4秒，第二个为8秒，第三个为16秒，以此类推，该数据结构适合用在回退匹配算法中
 * Created by xiongsenlin on 15/7/2.
 */
public class GuavaBackoffCache {
    private Callback callback;
    private Cache<String, CacheItem> [] cacheList;

    /**
     * 默认的缓存容量大小
     */
    private static int defaultCacheSizeLimit = 50000;

    /**
     * 默认的回退匹配次数
     */
    private static int defaultJoinTimes = 3;

    /**
     * 默认的第一次超时时间以及每次超时时间的递增倍数
     */
    private static int defaultBeginExpireTime = 4;
    private static int defaultFactor = 2;

    public GuavaBackoffCache(RemovalListener listener) {
        this(defaultJoinTimes, defaultBeginExpireTime, defaultFactor, defaultCacheSizeLimit, listener, null);
    }

    public GuavaBackoffCache(int backOffTimes, RemovalListener listener) {
        this(backOffTimes, defaultBeginExpireTime, defaultFactor, defaultCacheSizeLimit, listener, null);
    }

    public GuavaBackoffCache(int backOffTimes, RemovalListener listener, Callback cb) {
        this(backOffTimes, defaultBeginExpireTime, defaultFactor, defaultCacheSizeLimit, listener, cb);
    }

    public GuavaBackoffCache(int backOffTimes, int beginExpire, int factor, RemovalListener listener, Callback cb) {
        this(backOffTimes, beginExpire, factor, defaultCacheSizeLimit, listener, cb);
    }

    /**
     * @param backOffTimes   数据放入缓存的次数
     * @param beginExpire    第一次数据进入缓存后的超时时间
     * @param factor         后续数据每次重新放入缓存的时候超时时间相对于前一次的增加比例
     * @param sizeLimit      单个缓存容量大小的限制，当缓存中数据大小超过此值时，数据会被强制退出缓存
     * @param listener       监听器，当数据退出缓存时，会调用监听器对应的监听函数
     * @param callback       当数据最后一次出缓存的时候调用的回调函数
     */
    public GuavaBackoffCache(int backOffTimes, int beginExpire, int factor,
                             int sizeLimit, RemovalListener listener, Callback callback) {
        if (backOffTimes < 0 || beginExpire <= 0 || factor <= 0 || sizeLimit <= 0) {
            throw new RuntimeException("invalid init params");
        }

        this.callback = callback;
        this.cacheList = new Cache[backOffTimes];

        int expire = beginExpire;
        for (int i = 0; i < backOffTimes; i++) {
            CacheBuilder cacheBuilder = CacheBuilder.newBuilder()
                    .expireAfterWrite(expire, TimeUnit.SECONDS)
                    .maximumSize(sizeLimit);
            if (listener != null) {
                cacheBuilder.removalListener(listener);
            }

            Cache<String, CacheItem> cacheItem = cacheBuilder.build();

            cacheList[i] = cacheItem;
            expire *= factor;
        }

        if (backOffTimes > 0) {
            this.refreshCache();
        }
    }

    /**
     * 将数据放回缓存中，如果放回次数超过回退匹配的次数，则调用回调函数（如果存在的话），
     * 然后数据在缓存中的生命周期就结束了
     * @param key
     * @param cacheItem
     */
    public void addData(String key, CacheItem cacheItem) {
        int times = cacheItem.getTimes();
        cacheItem.setTimes(times + 1);

        if (times >= this.cacheList.length) {
            if (this.callback != null){
                cacheItem.setKey(key);
                this.callback.callback(cacheItem);
            }
        }
        else {
            this.cacheList[times].put(key, cacheItem);
        }
    }

    /**
     * 检查某条数据是否还在缓存中
     * @param key
     * @return
     */
    public boolean exist(String key) {
        for (int i = 0; i < this.cacheList.length; i++) {
            if (this.cacheList[i].getIfPresent(key) != null) {
                return true;
            }
        }

        return false;
    }

    /**
     * 获取整个缓存当前的容量大小
     * @return
     */
    public int getDataSize() {
        int result = 0;
        for (int i = 0; i < this.cacheList.length; i++) {
            result += this.cacheList[i].size();
        }
        return result;
    }

    /**
     * {@link Cache}不会自动刷新，只是在添加删除数据时做一些刷新操作，所以需要定时手动对缓存进行刷新操作
     */
    private void refreshCache() {
        Thread cleaner = new Thread(new Runnable() {
            public void run() {
                while (true) {
                    for (int i = 0; i < cacheList.length; i++) {
                        cacheList[i].cleanUp();
                    }
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }
        });
        cleaner.setDaemon(true);
        cleaner.start();
    }

    public static class CacheItem <T> {
        private String key;
        private T data;

        private int times = 0;
        private long sTime = System.currentTimeMillis();

        public CacheItem(T data) {
            this.data = data;
        }

        public T getData() {
            return data;
        }

        public void setData(T data) {
            this.data = data;
        }

        public int getTimes() {
            return times;
        }

        public void setTimes(int times) {
            this.times = times;
        }

        public String getKey() {
            return key;
        }

        public void setKey(String key) {
            this.key = key;
        }

        @Override
        public String toString() {
            long now = System.currentTimeMillis();
            return "cachedTime: " + (now - this.sTime)
                    + "(ms) times: " + this.times + " data: " + this.data.toString();
        }
    }
}
