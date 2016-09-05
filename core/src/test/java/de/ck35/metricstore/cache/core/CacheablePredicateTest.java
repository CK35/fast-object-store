package de.ck35.metricstore.cache.core;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;

import de.ck35.metricstore.StoredMetric;
import de.ck35.metricstore.cache.core.CacheablePredicate;

@RunWith(MockitoJUnitRunner.class)
public class CacheablePredicateTest {

    @Mock StoredMetric metric;
    
    private Interval interval;
    private Supplier<Interval> cacheIntervalSupplier;

    public CacheablePredicateTest() {
        this.interval = new Interval(new DateTime(2015, 1, 1, 1, 0),
                                     new DateTime(2015, 1, 2, 1, 0));
        this.cacheIntervalSupplier = Suppliers.ofInstance(interval);
    }
    
    public CacheablePredicate cacheablePredicate() {
        return new CacheablePredicate(cacheIntervalSupplier);
    }
    
    @Test
    public void testApplyNull() {
        assertFalse(cacheablePredicate().apply(null));
    }
    
    @Test
    public void testApplyBefore() {
        when(metric.getTimestamp()).thenReturn(new DateTime(2015, 1, 1, 0, 59));
        assertFalse(cacheablePredicate().apply(metric));
    }
    
    @Test
    public void testApplyEqualsStart() {
        when(metric.getTimestamp()).thenReturn(new DateTime(2015, 1, 1, 1, 0));
        assertTrue(cacheablePredicate().apply(metric));
    }
    
    @Test
    public void testApplyInside() {
        when(metric.getTimestamp()).thenReturn(new DateTime(2015, 1, 1, 1, 1));
        assertTrue(cacheablePredicate().apply(metric));
    }
    
    @Test
    public void testApplyEqualsEnd() {
        when(metric.getTimestamp()).thenReturn(new DateTime(2015, 1, 2, 1, 0));
        assertTrue(cacheablePredicate().apply(metric));
    }
    
    @Test
    public void testApplyAfter() {
        when(metric.getTimestamp()).thenReturn(new DateTime(2015, 1, 2, 1, 1));
        assertTrue(cacheablePredicate().apply(metric));
    }

}