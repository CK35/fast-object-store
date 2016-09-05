package de.ck35.metricstore.cache.core.buckets;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;
import java.util.Map.Entry;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Interval;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Charsets;
import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;

import de.ck35.metricstore.MetricBucket;
import de.ck35.metricstore.StoredMetric;
import de.ck35.metricstore.cache.core.buckets.BucketManager;
import de.ck35.metricstore.cache.core.buckets.ExpandedBucketManager;
import de.ck35.metricstore.cache.core.buckets.ImmutableStoredMetric;
import de.ck35.metricstore.configuration.ObjectMapperConfiguration;
import de.ck35.metricstore.util.io.ObjectNodeReader;
import de.ck35.metricstore.util.io.ObjectNodeWriter;

@RunWith(MockitoJUnitRunner.class)
public class BucketManagerTest {

    @Mock MetricBucket metricBucket;
    @Mock ExpandedBucketManager expandedBucketManager;
    
    @Mock Function<MetricBucket, ExpandedBucketManager> expandedBucketManagerFactory;
    
    private ObjectMapper mapper;
    private Function<InputStream, ObjectNodeReader> objectNodeReaderFactory;
    private Function<OutputStream, ObjectNodeWriter> objectNodeWriterFactory;

    public BucketManagerTest() {
        this.mapper = ObjectMapperConfiguration.objectMapper();
        this.objectNodeReaderFactory = new ObjectNodeReader.StreamFactory(mapper, Charsets.UTF_8);
        this.objectNodeWriterFactory = new ObjectNodeWriter.StreamFactory(mapper.getFactory(), Charsets.UTF_8);
    }
    
    @Before
    public void before() {
        when(metricBucket.getName()).thenReturn("Bucket_1");
        when(expandedBucketManagerFactory.apply(metricBucket)).thenReturn(expandedBucketManager);
    }
    
    public BucketManager bucketManager() {
        return new BucketManager(expandedBucketManagerFactory, objectNodeReaderFactory, objectNodeWriterFactory);
    }
    
    @Test
    public void testWriteRead() {
        ObjectNode node1 = mapper.getNodeFactory().objectNode();
        node1.put("a", "a1");
        ObjectNode node2 = mapper.getNodeFactory().objectNode();
        node2.put("b", "b2");
        
        DateTime timestamp1 = new DateTime(2015, 1, 1, 1, 0, DateTimeZone.UTC);
        DateTime timestamp2 = new DateTime(2015, 1, 1, 1, 1, DateTimeZone.UTC);
        
        BucketManager bucketManager = bucketManager();
        bucketManager.write(new ImmutableStoredMetric(metricBucket, timestamp1, node1));
        bucketManager.write(new ImmutableStoredMetric(metricBucket, timestamp2, node2));
        Entry<Interval, Iterable<StoredMetric>> readResult = bucketManager.read(metricBucket.getName(), new Interval(new DateTime(2014, 1, 1, 0, 0, DateTimeZone.UTC), 
                                                                                                                     new DateTime(2015, 2, 1, 0, 0, DateTimeZone.UTC)));
        
        assertEquals(new Interval(timestamp1, timestamp2.plusMinutes(1)), readResult.getKey());
        List<StoredMetric> result = ImmutableList.copyOf(readResult.getValue());
        assertEquals(metricBucket, result.get(0).getMetricBucket());
        assertEquals(metricBucket, result.get(1).getMetricBucket());
        assertEquals(timestamp1, result.get(0).getTimestamp());
        assertEquals(timestamp2, result.get(1).getTimestamp());
        assertEquals(node1, result.get(0).getObjectNode());
        assertEquals(node2, result.get(1).getObjectNode());
    }

}
