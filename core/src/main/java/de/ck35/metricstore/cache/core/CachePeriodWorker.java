package de.ck35.metricstore.cache.core;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.joda.time.Period;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jmx.export.annotation.ManagedAttribute;
import org.springframework.jmx.export.annotation.ManagedResource;

import com.google.common.base.Optional;
import com.google.common.base.Supplier;

import de.ck35.metricstore.cache.core.buckets.BucketManager;

@ManagedResource
public class CachePeriodWorker implements Runnable {

    private static final Logger LOG = LoggerFactory.getLogger(CachePeriodWorker.class);
    
    private final Supplier<Interval> cacheIntervalSupplier;
    private final Supplier<DateTime> clock;
    private final Period cacheCleanupPeriod;
    
    private final BucketMetricCache cache;
    private final BucketManager bucketManager;

    private final AtomicReference<Interval> lastCacheCleanupInterval;
    private final AtomicBoolean running;
    
    public CachePeriodWorker(Supplier<Interval> cacheIntervalSupplier,
                             Supplier<DateTime> clock,
                             Period cacheCleanupPeriod,
                             BucketMetricCache cache,
                             BucketManager bucketManager) {
        this.cacheIntervalSupplier = cacheIntervalSupplier;
        this.clock = clock;
        this.cacheCleanupPeriod = cacheCleanupPeriod;
        this.cache = cache;
        this.bucketManager = bucketManager;
        
        this.lastCacheCleanupInterval = new AtomicReference<>();
        this.running = new AtomicBoolean(false);
    }

    @Override
    public void run() {
        running.set(true);
        try {            
            cache.init(cacheIntervalSupplier.get());
            while(!Thread.interrupted()) {
                DateTime nextCleanup = clock.get().plus(cacheCleanupPeriod);
                cleanup(cacheIntervalSupplier.get());
                long sleepMillis = nextCleanup.getMillis() - clock.get().getMillis();
                if(sleepMillis > 0) {
                    Thread.sleep(sleepMillis);
                }
            }
        } catch(InterruptedException e) {
            LOG.info("Cache period worker has been interrupted.");
        } finally {
            running.set(false);
            LOG.info("Cache period worker will exit now.");
        }
    }
    
    public void cleanup(Interval interval) {
        setLastCacheCleanupInterval(interval);
        bucketManager.clear(interval.getStart());
    }
    
    public Optional<Interval> getLastCacheCleanupInterval() {
        return Optional.fromNullable(lastCacheCleanupInterval.get());
    }
    public void setLastCacheCleanupInterval(Interval interval) {
        lastCacheCleanupInterval.set(interval);
    }
    
    @ManagedAttribute
    public String getCacheCleanupPeriodISO() {
        return cacheCleanupPeriod.toString();
    }
    @ManagedAttribute
    public String getLastCacheCleanupIntervalISO() {
        Optional<Interval> interval = getLastCacheCleanupInterval();
        if(interval.isPresent()) {
            return interval.get().toString();
        } else {
            return null;
        }
    }
    @ManagedAttribute
    public boolean isRunning() {
        return running.get();
    }
}