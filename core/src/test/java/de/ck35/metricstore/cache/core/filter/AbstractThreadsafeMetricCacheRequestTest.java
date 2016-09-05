package de.ck35.metricstore.cache.core.filter;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;

import org.joda.time.Interval;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.ImmutableList;

import de.ck35.metricstore.StoredMetricCallable;
import de.ck35.metricstore.cache.core.filter.AbstractThreadsafeMetricCacheRequest;
import de.ck35.metricstore.cache.core.filter.ImmutableReadFilter;

public class AbstractThreadsafeMetricCacheRequestTest {

    private AbstractThreadsafeMetricCacheRequest cacheRequest;
    
    @Before
    public void before() {
        this.cacheRequest = new AbstractThreadsafeMetricCacheRequest() {
            @Override
            public void read(String bucketName, Interval interval) {
                
            }
        };
    }
    
    @Test
    public void testBuilder() {
        StoredMetricCallable callable = mock(StoredMetricCallable.class);
        ImmutableReadFilter readFilter = (ImmutableReadFilter) cacheRequest.builder(callable)
                                                                           .andFieldIsPresent("a")
                                                                           .andFieldIsNotPresent("b")
                                                                           .andValueMatches("a", "b")
                                                                           .build();
        assertEquals(ImmutableList.of(readFilter), cacheRequest.getFilters());
        assertEquals(callable, readFilter.getCallable());
        assertEquals(1, ImmutableList.of(readFilter.getRequiredNonPresentFields()).size());
        assertEquals(1, ImmutableList.of(readFilter.getRequiredPresentFields()).size());
        assertEquals(1, ImmutableList.of(readFilter.getValueFields()).size());
        assertEquals(readFilter.getRequiredPresentFields().iterator().next(),
                     readFilter.getValueFields().iterator().next().getKey());
    }

}