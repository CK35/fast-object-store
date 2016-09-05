package de.ck35.metricstore.fs;

import java.io.Closeable;
import java.util.Deque;
import java.util.LinkedList;
import java.util.Observable;
import java.util.Observer;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import com.google.common.base.Predicate;
import com.google.common.base.Supplier;
import com.google.common.collect.AbstractIterator;

import de.ck35.metricstore.StoredMetric;
import de.ck35.metricstore.util.io.MetricsIOException;

public class StoredMetricReadCache extends AbstractIterator<StoredMetric> implements Predicate<StoredMetric>, Observer, Closeable {

    private final Supplier<Integer> maxCacheSizeSetting;

    private final Lock cacheLock;
    private final Condition cacheCondition;
    private final Deque<StoredMetric> cache;
    private boolean closed;
    
    public StoredMetricReadCache(Supplier<Integer> maxCacheSizeSetting) {
        this(maxCacheSizeSetting, new ReentrantLock(), new LinkedList<StoredMetric>());
    }
    protected StoredMetricReadCache(Supplier<Integer> maxCacheSizeSetting, 
                                    Lock cacheLock,
                                    Deque<StoredMetric> cache) {
        this.maxCacheSizeSetting = maxCacheSizeSetting;
        this.cacheLock = cacheLock;
        this.cacheCondition = cacheLock.newCondition();
        this.cache = cache;
        this.closed = false;
    }
    
    @Override
    public boolean apply(StoredMetric input) {
        this.cacheLock.lock();
        try {
            if(closed) {
                return false;
            }
            while(cache.size() >= maxCacheSizeSetting.get()) {
                try {
                    cacheCondition.await();
                } catch (InterruptedException e) {
                    return false;
                }
            }
            cache.addLast(input);
            cacheCondition.signalAll();
            return true;
        } finally {
            this.cacheLock.unlock();
        }
    }

    @Override
    protected StoredMetric computeNext() {
        this.cacheLock.lock();
        try {
             while(cache.isEmpty()) {
                 if(closed) {
                     return endOfData();
                 } else {
                     try {
                        cacheCondition.await();
                    } catch (InterruptedException e) {
                        throw new MetricsIOException("Could not await next StoredMetric while reading.", e);
                    }
                 }
             }
             StoredMetric storedMetric = cache.removeFirst();
             cacheCondition.signalAll();
            return storedMetric;
        } finally {
            this.cacheLock.unlock();
        }
    }

    @Override
    public void update(Observable o, Object arg) {
        close();
    }
    
    @Override
    public void close() {
        this.cacheLock.lock();
        try {
            closed = true;
            cacheCondition.signalAll();
        } finally {
            this.cacheLock.unlock();
        }
    }
}