package de.ck35.metricstore.cache.core;

import java.util.Map.Entry;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;

import org.joda.time.DateTimeZone;
import org.joda.time.Interval;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jmx.export.annotation.ManagedAttribute;
import org.springframework.jmx.export.annotation.ManagedResource;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;

import de.ck35.metricstore.MetricBucket;
import de.ck35.metricstore.MetricRepository;
import de.ck35.metricstore.StoredMetric;
import de.ck35.metricstore.StoredMetricCallable;
import de.ck35.metricstore.cache.MetricCache;
import de.ck35.metricstore.cache.MetricCacheRequest;
import de.ck35.metricstore.cache.core.buckets.BucketManager;
import de.ck35.metricstore.cache.core.buckets.ReadFilterPredicate;
import de.ck35.metricstore.cache.core.filter.AbstractThreadsafeMetricCacheRequest;
import de.ck35.metricstore.cache.core.filter.ImmutableReadFilter;

@ManagedResource
public class BucketMetricCache implements MetricCache {

    private static final Logger LOG = LoggerFactory.getLogger(BucketMetricCache.class);
    
    private final MetricRepository metricRepository;
    private final BucketManager bucketManager;
    private final Predicate<StoredMetric> cacheablePredicate;
    private final CountDownLatch initLatch;
    
    private final AtomicLong totalReadRequests;
    private final AtomicLong totalWrites;
    private final AtomicLong totalReadCalls;
    private final AtomicLong totalListCalls;
    
    public BucketMetricCache(MetricRepository metricRepository, 
                                       BucketManager bucketManager,
                                       Predicate<StoredMetric> cacheablePredicate) {
        this.metricRepository = metricRepository;
        this.bucketManager = bucketManager;
        this.cacheablePredicate = cacheablePredicate;
        this.initLatch = new CountDownLatch(1);
        this.totalReadRequests = new AtomicLong();
        this.totalWrites = new AtomicLong();
        this.totalReadCalls = new AtomicLong();
        this.totalListCalls = new AtomicLong();
    }
    
    public void init(Interval interval) throws InterruptedException {
        LOG.info("Starting cache init with interval: '{}'.", interval);
        for(MetricBucket bucket : bucketManager.listBuckets()) {
            if(Thread.interrupted()) {
                throw new InterruptedException();
            }
            LOG.info("Starting cache init with interval: '{}' and Bucket: '{}'.", interval, bucket.getName());
            metricRepository.read(bucket.getName(), interval, new StoredMetricCallable() {
                @Override
                public void call(StoredMetric node) {
                    bucketManager.write(node);
                }
            });
        }
        initLatch.countDown();
        LOG.info("Cache init with interval: '{}' sucessfully done.", interval);
    }
    
    public void awaitInit() {
        try {            
            initLatch.await();
        } catch(InterruptedException e) {
            throw new RuntimeException("Interrupted while waiting for init completed.");
        }
    }
    
    @Override
    public MetricCacheRequest request() {
        this.totalReadRequests.incrementAndGet();
        return new AbstractThreadsafeMetricCacheRequest() {
            @Override
            public void read(String bucketName, Interval interval) {
                BucketMetricCache.this.read(bucketName, interval, filters);
            }
        };
    }
    @Override
    public Iterable<MetricBucket> listBuckets() {
        awaitInit();
        this.totalListCalls.incrementAndGet();
        return bucketManager.listBuckets();
    }
    @Override
    public StoredMetric write(String bucketName,
                              String bucketType,
                              ObjectNode node) {
        awaitInit();
        this.totalWrites.incrementAndGet();
        StoredMetric storedMetric = metricRepository.write(bucketName, bucketType, node);
        if(cacheablePredicate.apply(storedMetric)) {            
            bucketManager.write(storedMetric);
        }
        return storedMetric;
    }
    
    public void read(String bucketName, Interval interval, final Iterable<ImmutableReadFilter> filters) {
        awaitInit();
        Interval utcInterval = new Interval(interval.getStart().withZone(DateTimeZone.UTC).withSecondOfMinute(0).withMillisOfSecond(0), 
                                            interval.getEnd().withZone(DateTimeZone.UTC).withSecondOfMinute(0).withMillisOfSecond(0));
        Entry<Interval, Iterable<StoredMetric>> cached = bucketManager.read(bucketName, utcInterval);
        if(!utcInterval.getStart().equals(cached.getKey().getStart())) {            
            metricRepository.read(bucketName, new Interval(utcInterval.getStart(), cached.getKey().getStart()), new StoredMetricCallable() {
                @Override
                public void call(StoredMetric node) {
                    filteredCall(node, filters);
                }
            });
        }
        for(StoredMetric metric : cached.getValue()) {
            this.totalReadCalls.incrementAndGet();
            filteredCall(metric, filters);
        }
    }
    
    public static void filteredCall(StoredMetric metric, Iterable<ImmutableReadFilter> filters) {
        for(ImmutableReadFilter filter : Iterables.filter(filters, new ReadFilterPredicate(metric.getObjectNode()))) {
            filter.getCallable().call(metric);
        }
    }
    
    @ManagedAttribute
    public long getTotalListCalls() {
        return totalListCalls.get();
    }
    @ManagedAttribute
    public long getTotalReadCalls() {
        return totalReadCalls.get();
    }
    @ManagedAttribute
    public long getTotalReadRequests() {
        return totalReadRequests.get();
    }
    @ManagedAttribute
    public long getTotalWrites() {
        return totalWrites.get();
    }
    @ManagedAttribute
    public boolean isInitialized() {
        return initLatch.getCount() == 0;
    }
}