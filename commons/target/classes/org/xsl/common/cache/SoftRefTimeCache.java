package org.xsl.common.cache;

import org.xsl.common.base.Callback;
import java.lang.ref.SoftReference;
import java.util.HashMap;
import java.util.Map;

/**
 * 功能类似storm中的TimeCacheMap，区别在于TimeCacheMap中的数据除非到了超时时间
 * 否则无论如何是不会被清理的，但SoftRefTimeCache中存的数据是java中的SoftReference
 * 当内存紧张时，数据会被垃圾回收器回收掉
 * Created by xiongsenlin on 15/7/3.
 */
public class SoftRefTimeCache<K, V> {
    private static final int DEFAULT_SIZE = 3;

    private SoftReference<HashMap<K, V>>[] buckets;

    private Thread cleaner;
    private Callback callback;
    private Object lock = new Object();

    private int bucketSize;
    private int currentIndex, removeIndex;

    /**
     * @param expire  数据超时时间，单位秒，数据加入缓存的时间超过该时间，数据肯定会
     *                被清理掉，但是也有可能由于内存紧张，数据被提前清理掉
     * @param bs      缓存中的槽数量，默认为3
     * @param cb      回调函数，当数据在缓存中超时时，会调用该函数
     */
    public SoftRefTimeCache(int expire, final int bs, final Callback cb) {
        if (expire < 0 || bs < 2) {
            throw new RuntimeException("Invalid init params");
        }

        this.callback = cb;
        this.bucketSize = bs;
        this.removeIndex = 0;
        this.currentIndex = this.bucketSize - 1;

        this.buckets = new SoftReference[this.bucketSize];
        for (int i = 0 ; i < this.bucketSize; i++) {
            buckets[i] = new SoftReference<>(new HashMap<K, V>());
        }

        final long expireMillis = expire * 1000L;
        final long sleepTime = expireMillis / (this.bucketSize - 1);
        this.cleaner = new Thread(new Runnable() {
            public void run() {
                while (true) {
                    synchronized (lock) {
                        HashMap<K, V> map = buckets[removeIndex].get();
                        if (map != null) {
                            for (Map.Entry<K, V> entry : map.entrySet()) {
                                if (callback != null) {
                                    callback.callback(entry);
                                }
                            }
                            map.clear();
                        }
                        removeIndex = (removeIndex + 1) % bucketSize;
                        currentIndex = (currentIndex + 1) % bucketSize;
                    }

                    try {
                        Thread.sleep(sleepTime);
                    } catch (InterruptedException e) {
                        break;
                    }
                }
            }
        });
        this.cleaner.setDaemon(true);
        this.cleaner.start();
    }

    public SoftRefTimeCache(int expire, int bucketSize) {
        this(expire, bucketSize, null);
    }

    public SoftRefTimeCache(int expire) {
        this(expire, DEFAULT_SIZE, null);
    }

    public V get(K key) {
        synchronized (this.lock) {
            for (int i = 0; i < this.bucketSize; i++) {
                Map<K, V> map = this.buckets[i].get();
                if (map != null && map.containsKey(key)) {
                    return map.get(key);
                }
            }
            return null;
        }
    }

    public void put(K key, V value) {
        synchronized (this.lock) {
            SoftReference<HashMap<K, V>> softReference = this.buckets[this.currentIndex];
            HashMap<K, V> map = softReference.get();
            if (map == null) {
                map = new HashMap<>();
                this.buckets[currentIndex] = new SoftReference<>(map);
            }
            map.put(key, value);
        }
    }

    public void remove(K key) {
        synchronized (this.lock) {
            for (int i = 0; i < this.bucketSize; i++) {
                Map<K, V> map = this.buckets[i].get();
                if (map != null && map.containsKey(key)) {
                    map.remove(key);
                }
            }
        }
    }

    public int size() {
        int res = 0;
        synchronized (this.lock) {
            for (int i = 0; i < this.bucketSize; i++) {
                HashMap<K, V> map = this.buckets[i].get();
                if (map != null) {
                    res += map.size();
                }
            }
        }
        return res;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("currentIndex: " + this.currentIndex);
        sb.append(" removeIndex: " + this.removeIndex);
        sb.append(" cacheSize: " + this.size());
        return sb.toString();
    }
}
