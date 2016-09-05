package de.ck35.metricstore.cache.core;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.mockito.Mockito.*;

import java.util.HashMap;
import java.util.Map;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Interval;
import org.joda.time.Period;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

import de.ck35.metricstore.MetricBucket;
import de.ck35.metricstore.StoredMetric;
import de.ck35.metricstore.StoredMetricCallable;
import de.ck35.metricstore.cache.MetricCache;
import de.ck35.metricstore.cache.MetricCacheRequest;
import de.ck35.metricstore.cache.core.buckets.BucketManager;
import de.ck35.metricstore.configuration.BucketMetricCacheRepositoryConfiguration;
import de.ck35.metricstore.configuration.NonPersistentMetricRepositoryConfiguration;
import de.ck35.metricstore.configuration.ObjectMapperConfiguration;
import de.ck35.metricstore.util.TimestampFunction;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes={NonPersistentMetricRepositoryConfiguration.class,
        ObjectMapperConfiguration.class,
        BucketMetricCacheRepositoryConfiguration.class})
public class BucketMetricCacheTest {

    @Autowired MetricCache cache;
    @Autowired BucketManager bucketManager;
    @Autowired ObjectMapper mapper;
    
    @Test
    public void testReadFromNotExistingBucket() {
        StoredMetricCallable callable = mock(StoredMetricCallable.class);
        MetricCacheRequest request = cache.request();
        request.build(callable);
        request.read("xyz", new Interval(DateTime.now(), Period.minutes(1)));
        verifyZeroInteractions(callable);
    }
    
    @Test
    public void testWriteAndRead() {
        
        DateTime timestamp = DateTime.now();
        Interval interval = new Interval(timestamp.minusMinutes(1), timestamp.plusMinutes(1));
        
        String bucketType = "BucketType";
        String bucket1 = "Bucket_1";
        String bucket2 = "Bucket_2";
        
        Map<String, Object> node1 = ImmutableMap.<String, Object>of("b", false);
        Map<String, Object> node2 = ImmutableMap.<String, Object>of("a", "a1", "b", true);
        Map<String, Object> node3 = ImmutableMap.<String, Object>of("a", "a2", "b", true);
        
        Map<String, Object> node4 = ImmutableMap.<String, Object>of("a", "a1", "b", false);

        assertFalse(cache.listBuckets().iterator().hasNext());
        cache.write(bucket1, bucketType, node(timestamp, node1));
        cache.write(bucket1, bucketType, node(timestamp, node2));
        cache.write(bucket1, bucketType, node(timestamp, node3));
        cache.write(bucket2, bucketType, node(timestamp, node4));
        
        assertEquals(ImmutableSet.of(bucket1, bucket2), ImmutableSet.copyOf(Iterables.transform(cache.listBuckets(), new Function<MetricBucket, String>() {
            @Override
            public String apply(MetricBucket input) {
                return input.getName();
            }
        })));
        
        assertEquals(2, bucketManager.getTotalCreatedCachedMetricBuckets());
        assertEquals(ImmutableMap.of(bucket1, 3L, 
                                     bucket2, 1L), bucketManager.getTotalObjectNodeCountPerBucket());
        String isoInterval = new Interval(timestamp.withZone(DateTimeZone.UTC)
                                                   .withSecondOfMinute(0)
                                                   .withMillisOfSecond(0), Period.minutes(1)).toString();
        assertEquals(new HashMap<>(ImmutableMap.of(bucket1, isoInterval,
                                                   bucket2, isoInterval)), bucketManager.getDataIntervalPerBucket());
        
        MetricCacheRequest request = cache.request();
        
        StoredMetricCallable unfiltered = mock(StoredMetricCallable.class);
        StoredMetricCallable node2and3Filtered = mock(StoredMetricCallable.class);
        StoredMetricCallable falseFiltered = mock(StoredMetricCallable.class);
        
        request.build(unfiltered);
        request.builder(node2and3Filtered).andValueMatches("a", "a[1,2]").build();
        request.builder(falseFiltered).andValueMatches("b", "false").build();
        
        request.read(bucket1, interval);
        
        ArgumentCaptor<StoredMetric> unfilteredCaptor = ArgumentCaptor.forClass(StoredMetric.class);
        ArgumentCaptor<StoredMetric> node2and3FilteredCaptor = ArgumentCaptor.forClass(StoredMetric.class);
        ArgumentCaptor<StoredMetric> falseFilteredCaptor = ArgumentCaptor.forClass(StoredMetric.class);
        
        verify(unfiltered, times(3)).call(unfilteredCaptor.capture());
        verify(node2and3Filtered, times(2)).call(node2and3FilteredCaptor.capture());
        verify(falseFiltered).call(falseFilteredCaptor.capture());
        
        assertEquals(ImmutableList.of(node1, node2, node3), Lists.transform(unfilteredCaptor.getAllValues(), STORED_METRICS_MAP));
        assertEquals(ImmutableList.of(node2, node3), Lists.transform(node2and3FilteredCaptor.getAllValues(), STORED_METRICS_MAP));
        assertEquals(ImmutableList.of(node1), Lists.transform(falseFilteredCaptor.getAllValues(), STORED_METRICS_MAP));
    }
    
    private final Function<StoredMetric, Map<String, Object>> STORED_METRICS_MAP = new Function<StoredMetric, Map<String,Object>>() {
        @Override
        public Map<String, Object> apply(StoredMetric input) {
            return map(input.getObjectNode());
        }
    };
    
    public ObjectNode node(DateTime timestamp, Map<String, Object> properties) {
        ObjectNode node = mapper.convertValue(properties, ObjectNode.class);
        node.put(TimestampFunction.DEFAULT_TIMESTAMP_FILED_NAME, timestamp.toString());
        return node;
    }
    public Map<String, Object> map(ObjectNode node) {
        @SuppressWarnings("unchecked")
        Map<String, Object> map = mapper.convertValue(node, Map.class);
        map.remove(TimestampFunction.DEFAULT_TIMESTAMP_FILED_NAME);
        return map;
    }
}