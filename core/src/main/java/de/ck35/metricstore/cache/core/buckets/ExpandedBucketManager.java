package de.ck35.metricstore.cache.core.buckets;

import java.util.Deque;
import java.util.LinkedList;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import com.google.common.base.Function;
import com.google.common.base.Supplier;

import de.ck35.metricstore.MetricBucket;

public class ExpandedBucketManager implements BucketExpandListener {

    private final Lock queueLock;
    private final Deque<MinuteBucket> expandedBuckets;
    private final Supplier<Integer> maxCachedEntriesSetting;
    
    public ExpandedBucketManager(Supplier<Integer> maxCachedEntriesSetting) {
        this.maxCachedEntriesSetting = maxCachedEntriesSetting;
        this.queueLock = new ReentrantLock();
        this.expandedBuckets = new LinkedList<>();
    }
    
    @Override
    public void expanded(MinuteBucket minuteBucket) {
        final MinuteBucket[] compressList;
        this.queueLock.lock();
        try {
            int compressCount = expandedBuckets.size() + 1 - getMaxCachedEntries();
            if(compressCount > 0) {
                compressList = new MinuteBucket[compressCount];
                for(int count = 0 ; count < compressCount ; count++) {
                    compressList[count] = expandedBuckets.removeFirst();
                }
            } else {
                compressList = new MinuteBucket[0];
            }
            expandedBuckets.addLast(minuteBucket);
        } finally {
            this.queueLock.unlock();
        }
        for(MinuteBucket bucket : compressList) {
            bucket.compress();
        }
    }
    
    public int getMaxCachedEntries() {
        int entries = maxCachedEntriesSetting.get().intValue();
        if(entries < 0) {
            throw new IllegalArgumentException("Max cached entries can not be less than 0! Was: " + entries);
        }
        return entries;
    }
    
    public static class ExpandedBucketManagerFactory implements Function<MetricBucket, ExpandedBucketManager> {

        private final Supplier<Integer> maxExpandedBucketsSetting;
        
        public ExpandedBucketManagerFactory(Supplier<Integer> maxExpandedBucketsSetting) {
            this.maxExpandedBucketsSetting = maxExpandedBucketsSetting;
        }
        @Override
        public ExpandedBucketManager apply(MetricBucket input) {
            return new ExpandedBucketManager(maxExpandedBucketsSetting);
        }
    }
}