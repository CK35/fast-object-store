package de.ck35.metricstore.nonpersistent;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Interval;
import org.joda.time.Period;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;

import de.ck35.metricstore.api.MetricBucket;
import de.ck35.metricstore.api.MetricRepository;
import de.ck35.metricstore.api.StoredMetric;
import de.ck35.metricstore.api.StoredMetricCallable;
import de.ck35.metricstore.nonpersistent.configuration.NonPersistentMetricRepositoryConfiguration;
import de.ck35.metricstore.util.TimestampFunction;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes=NonPersistentMetricRepositoryConfiguration.class)
public class NonPersistentMetricRepositoryTest {

    @Autowired MetricRepository metricRepository;
    
    private DateTime timestamp;
    
    public NonPersistentMetricRepositoryTest() {
        this.timestamp = new DateTime(2015, 1, 1, 0, 0, DateTimeZone.UTC);
    }
    
    @Test
    public void testListBuckets() {
        String bucketName = "my-test-bucket";
        String bucketType = "my-test-bucket-type";
        metricRepository.wirte(bucketName, bucketType, node(timestamp));
        for(MetricBucket bucket : metricRepository.listBuckets()) {
            if(bucketName.equals(bucket.getName()) && bucketType.equals(bucket.getType())) {
                return;
            }
        }
        fail("Bucket not found!");
    }

    @Test
    public void testWirte() {
        String bucketName = "my-test-bucket";
        String bucketType = "my-test-bucket-type";
        ObjectNode node = node(timestamp);
        StoredMetric storedMetric = metricRepository.wirte(bucketName, bucketType, node);
        assertNotNull(storedMetric);
        assertNotNull(storedMetric.getMetricBucket());
        assertEquals(bucketName, storedMetric.getMetricBucket().getName());
        assertEquals(bucketType, storedMetric.getMetricBucket().getType());
        assertEquals(node, storedMetric.getObjectNode());
        assertEquals(timestamp, storedMetric.getTimestamp());
    }

    @Test
    public void testRead() {
        StoredMetricCallable callable = mock(StoredMetricCallable.class);
        metricRepository.read("bucketB", new Interval(timestamp, Period.minutes(1)), callable);
        verifyZeroInteractions(callable);
    }
    
    public static ObjectNode node(DateTime timestamp) {
        JsonNodeFactory nodeFactory = new ObjectMapper().getNodeFactory();
        ObjectNode objectNode = nodeFactory.objectNode();
        objectNode.put(TimestampFunction.DEFAULT_TIMESTAMP_FILED_NAME, timestamp.toString());
        return objectNode;
    }

}