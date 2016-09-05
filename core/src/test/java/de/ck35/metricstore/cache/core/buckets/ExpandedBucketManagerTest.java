package de.ck35.metricstore.cache.core.buckets;

import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import com.google.common.base.Supplier;

import de.ck35.metricstore.MetricBucket;
import de.ck35.metricstore.cache.core.buckets.ExpandedBucketManager;
import de.ck35.metricstore.cache.core.buckets.MinuteBucket;
import de.ck35.metricstore.cache.core.buckets.ExpandedBucketManager.ExpandedBucketManagerFactory;

@RunWith(MockitoJUnitRunner.class)
public class ExpandedBucketManagerTest {

    @Mock Supplier<Integer> maxCachedEntriesSetting;
    @Mock MinuteBucket bucket1;
    @Mock MinuteBucket bucket2;
    @Mock MinuteBucket bucket3;
    @Mock MinuteBucket bucket4;
    @Mock MinuteBucket bucket5;

    @Before
    public void before() {
        when(maxCachedEntriesSetting.get()).thenReturn(3);
    }
    
    public ExpandedBucketManager expandedBucketManager() {
        return new ExpandedBucketManager(maxCachedEntriesSetting);
    }
    
    @Test
    public void testExpanded() {
        ExpandedBucketManager manager = expandedBucketManager();
        manager.expanded(bucket1);
        verifyZeroInteractions(bucket1);
        manager.expanded(bucket2);
        verifyZeroInteractions(bucket1, bucket2);
        manager.expanded(bucket3);
        verifyZeroInteractions(bucket1, bucket2, bucket3);
        manager.expanded(bucket4);
        verifyZeroInteractions(bucket2, bucket3, bucket4);
        verify(bucket1).compress();
        reset(bucket1);
        manager.expanded(bucket5);
        verifyZeroInteractions(bucket1, bucket3, bucket4, bucket5);
        verify(bucket2).compress();
    }
    
    @Test
    public void testExpandedBucketManagerFactory() {
        MetricBucket bucket = mock(MetricBucket.class);
        ExpandedBucketManagerFactory factory = new ExpandedBucketManagerFactory(maxCachedEntriesSetting);
        assertTrue(factory.apply(bucket) instanceof ExpandedBucketManager);
    }
}