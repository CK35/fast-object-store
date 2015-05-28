package de.ck35.metricstore.fs;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

import java.util.Deque;
import java.util.LinkedList;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;

import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import com.google.common.base.Suppliers;

import de.ck35.metricstore.api.StoredMetric;
import de.ck35.metricstore.util.MetricsIOException;

public class StoredMetricReadCacheTest {

    private int maxCacheSize;
    private StoredMetric storedMetric;
    
    public StoredMetricReadCacheTest() {
        this.storedMetric = mock(StoredMetric.class);
        this.maxCacheSize = 1;
    }
    
    public StoredMetricReadCache storedMetricReadCache() {
        return new StoredMetricReadCache(Suppliers.ofInstance(maxCacheSize));
    }
    public StoredMetricReadCache storedMetricReadCache(Lock lock, Deque<StoredMetric> cache) {
        return new StoredMetricReadCache(Suppliers.ofInstance(maxCacheSize), lock, cache);
    }
    
    @Test
    public void testApply() {
        StoredMetricReadCache cache = storedMetricReadCache();
        assertTrue(cache.apply(storedMetric));
        assertTrue(cache.hasNext());
        assertEquals(storedMetric, cache.next());
    }
    @Test
    public void testApplyWhenClosed() {
        StoredMetricReadCache cache = storedMetricReadCache();
        cache.close();
        assertFalse(cache.apply(storedMetric));
        assertFalse(cache.hasNext());
    }
    @Test
    public void testApplyWhenClosedThroughUpdate() {
        StoredMetricReadCache cache = storedMetricReadCache();
        cache.update(null, null);
        assertFalse(cache.apply(storedMetric));
        assertFalse(cache.hasNext());
    }
    @Test
    public void testComputeNextAfterClose() {
        StoredMetricReadCache cache = storedMetricReadCache();
        assertTrue(cache.apply(storedMetric));
        cache.close();
        assertTrue(cache.hasNext());
        assertEquals(storedMetric, cache.next());
        assertFalse(cache.hasNext());
    }
    @Test
    public void testApplyWithMaxCacheSizeReached() throws InterruptedException {
        final Deque<StoredMetric> queue = new LinkedList<>();
        queue.add(storedMetric);
        Lock lock = mock(Lock.class);
        Condition condition = mock(Condition.class);
        when(lock.newCondition()).thenReturn(condition);
        StoredMetricReadCache cache = storedMetricReadCache(lock, queue);
        doAnswer(new Answer<Void>() {
            @Override
            public Void answer(InvocationOnMock invocation) throws Throwable {
                queue.removeLast();
                return null;
            }
        }).when(condition).await();
        assertTrue(cache.apply(storedMetric));
        assertEquals(1, queue.size());
        verify(condition).signalAll();
    }
    @Test
    public void testApplyWithMaxCacheSizeReachedAndInterruption() throws InterruptedException {
        Deque<StoredMetric> queue = new LinkedList<>();
        queue.add(storedMetric);
        Lock lock = mock(Lock.class);
        Condition condition = mock(Condition.class);
        when(lock.newCondition()).thenReturn(condition);
        StoredMetricReadCache cache = storedMetricReadCache(lock, queue);
        doThrow(InterruptedException.class).when(condition).await();
        assertFalse(cache.apply(storedMetric));
        assertEquals(1, queue.size());
    }
    @Test
    public void testComputeNextWaitsForAdd() throws InterruptedException {
        final Deque<StoredMetric> queue = new LinkedList<>();
        Lock lock = mock(Lock.class);
        Condition condition = mock(Condition.class);
        when(lock.newCondition()).thenReturn(condition);
        StoredMetricReadCache cache = storedMetricReadCache(lock, queue);
        doAnswer(new Answer<Void>() {
            @Override
            public Void answer(InvocationOnMock invocation) throws Throwable {
                queue.add(storedMetric);
                return null;
            }
        }).when(condition).await();
        assertTrue(cache.hasNext());
        assertEquals(storedMetric, cache.next());
        assertTrue(queue.isEmpty());
        verify(condition).signalAll();
    }
    @Test
    public void testComputeNextWaitsForClose() throws InterruptedException {
        Deque<StoredMetric> queue = new LinkedList<>();
        Lock lock = mock(Lock.class);
        Condition condition = mock(Condition.class);
        when(lock.newCondition()).thenReturn(condition);
        final StoredMetricReadCache cache = storedMetricReadCache(lock, queue);
        doAnswer(new Answer<Void>() {
            @Override
            public Void answer(InvocationOnMock invocation) throws Throwable {
                cache.close();
                return null;
            }
        }).when(condition).await();
        assertFalse(cache.hasNext());
        verify(condition).await();
    }
    @Test(expected=MetricsIOException.class)
    public void testComputeNextFailsOnInterruption() throws InterruptedException {
        Deque<StoredMetric> queue = new LinkedList<>();
        Lock lock = mock(Lock.class);
        Condition condition = mock(Condition.class);
        when(lock.newCondition()).thenReturn(condition);
        StoredMetricReadCache cache = storedMetricReadCache(lock, queue);
        doThrow(InterruptedException.class).when(condition).await();
        cache.hasNext();
    }
}