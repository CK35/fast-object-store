package de.ck35.metricstore.cache.core;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.util.concurrent.atomic.AtomicReference;

import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.joda.time.Period;
import org.junit.After;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.runners.MockitoJUnitRunner;
import org.mockito.stubbing.Answer;

import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;

import de.ck35.metricstore.cache.core.BucketMetricCache;
import de.ck35.metricstore.cache.core.CachePeriodWorker;
import de.ck35.metricstore.cache.core.UTCCacheIntervalSupplier;
import de.ck35.metricstore.cache.core.buckets.BucketManager;

@RunWith(MockitoJUnitRunner.class)
public class CachePeriodWorkerTest {

    @Mock BucketMetricCache cache;
    @Mock BucketManager bucketManager;
    
    private Interval interval;
    private Supplier<Interval> cacheIntervalSupplier;
    private Supplier<DateTime> clock;
    private Period cacheCleanupPeriod;

    public CachePeriodWorkerTest() {
        this.interval = new Interval(new DateTime(2015, 1, 1, 0, 0), new DateTime(2015, 1, 1, 1, 0));
        this.cacheIntervalSupplier = Suppliers.ofInstance(interval);
        this.clock = new UTCCacheIntervalSupplier.Clock();
        this.cacheCleanupPeriod = Period.minutes(1);
    }
    
    public CachePeriodWorker cachePeriodWorker() {
        return new CachePeriodWorker(cacheIntervalSupplier, clock, cacheCleanupPeriod, cache, bucketManager);
    }
    
    @After
    public void after() {
        Thread.interrupted();
    }
    
    @Test
    public void testRunAsync() throws InterruptedException {
        CachePeriodWorker worker = cachePeriodWorker();
        assertFalse(worker.isRunning());
        
        Thread thread = new Thread(worker);
        thread.setDaemon(true);
        thread.start();
        
        verify(cache, timeout(1000)).init(interval);
        verify(bucketManager, timeout(1000)).clear(interval.getStart());
        assertTrue(worker.isRunning());
        
        thread.interrupt();
        thread.join(10_000);
        assertFalse(worker.isRunning());
    }
    
    @Test
    public void testRunSync() throws InterruptedException {
        final AtomicReference<Long> firstCall = new AtomicReference<>();
        final AtomicReference<Long> secondCall = new AtomicReference<>();
        clock = Suppliers.ofInstance(new DateTime(2015,1,1,0,0));
        cacheCleanupPeriod = Period.millis(500);
        doAnswer(new Answer<Void>() {
            @Override
            public Void answer(InvocationOnMock invocation) {
                if(!firstCall.compareAndSet(null, System.currentTimeMillis())) {
                    if(!secondCall.compareAndSet(null, System.currentTimeMillis())) {
                        Thread.currentThread().interrupt();
                    }
                }
                return null;
            }
        }).when(bucketManager).clear(interval.getStart());
        CachePeriodWorker worker = cachePeriodWorker();
        assertNull(worker.getLastCacheCleanupIntervalISO());
        worker.run();
        verify(cache).init(interval);
        verify(bucketManager, times(3)).clear(interval.getStart());
        assertNotNull(firstCall.get());
        assertNotNull(secondCall.get());
        assertTrue("Cleanup has been called twice in under: " + cacheCleanupPeriod.toString(), secondCall.get()-firstCall.get() >= cacheCleanupPeriod.getMillis());
        assertEquals(interval.toString(), worker.getLastCacheCleanupIntervalISO());
        assertEquals(cacheCleanupPeriod.toString(), worker.getCacheCleanupPeriodISO());
    }

}