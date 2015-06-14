package de.ck35.metricstore.fs.configuration;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Interval;
import org.joda.time.Period;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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
import de.ck35.metricstore.fs.BucketData;
import de.ck35.metricstore.fs.WritableFilesystemBucket;
import de.ck35.metricstore.util.TimestampFunction;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes=RootConfiguration.class)
public class ConfigurationTest {

    private static final Logger LOG = LoggerFactory.getLogger(ConfigurationTest.class);

    private static final String BUCKET_NAME = "MyBucket";
    private static final String BUCKET_TYPE = "MyBucketType";
    private static final DateTime TIMESTAMP = new DateTime(2015, 1, 1, 0, 0, DateTimeZone.UTC);
    
    @BeforeClass
    public static void before() throws IOException {
        Path workdir = Files.createTempDirectory("ConfigurationTest");
        LOG.debug("Running test inside tmp work dir: '{}'.", workdir);

        BucketData.create(workdir, BUCKET_NAME, BUCKET_TYPE);
        System.setProperty("metricstore.fs.basepath", workdir.toString());
    }
    
    @AfterClass
    public static void after() throws IOException {
        Path workdir = Paths.get(System.getProperty("metricstore.fs.basepath"));
        WritableFilesystemBucket.clearDirectory(workdir);
        Files.delete(workdir);
    }
    
    @Autowired ObjectMapper mapper;
    @Autowired MetricRepository metricRepository;
    
    @Test
    public void testReadNonExistingBucket() {
        StoredMetricCallable callable = mock(StoredMetricCallable.class);
        metricRepository.read("NotExistingBucket", new Interval(TIMESTAMP, Period.minutes(1)), callable);
        verifyNoMoreInteractions(callable);
    }
    
    @Test
    public void testListBuckets() {
        assertBucketsContains(BUCKET_NAME, BUCKET_TYPE, metricRepository.listBuckets());
    }
    
    @Test
    public void testWriteIntoExistingBucket() {
        JsonNodeFactory nodeFactory = mapper.getNodeFactory();
        ObjectNode node = nodeFactory.objectNode();
        node.put(TimestampFunction.DEFAULT_TIMESTAMP_FILED_NAME, TIMESTAMP.toString());
        node.put("a", "a1");
        
        StoredMetric storedMetric = metricRepository.wirte(BUCKET_NAME, BUCKET_TYPE, node);
        assertEquals(TIMESTAMP, storedMetric.getTimestamp());
        assertEquals(node, storedMetric.getObjectNode());
        assertNotNull(storedMetric.getMetricBucket());
        assertEquals(BUCKET_NAME, storedMetric.getMetricBucket().getName());
        assertEquals(BUCKET_TYPE, storedMetric.getMetricBucket().getType());
        
        StoredMetricCallable callable = mock(StoredMetricCallable.class);
        metricRepository.read(BUCKET_NAME, new Interval(TIMESTAMP, Period.minutes(1)), callable);
        ArgumentCaptor<StoredMetric> captor = ArgumentCaptor.forClass(StoredMetric.class);
        verify(callable).call(captor.capture());
        assertStoredMetricEquals(storedMetric, captor.getValue());
    }
    
    @Test
    public void testWriteIntoNewBucket() {
        JsonNodeFactory nodeFactory = mapper.getNodeFactory();
        ObjectNode node = nodeFactory.objectNode();
        node.put(TimestampFunction.DEFAULT_TIMESTAMP_FILED_NAME, TIMESTAMP.toString());
        node.put("a", "a1");
        
        String newBucketName = "NewBucket";
        String newBucketType = "NewBucketType";
        StoredMetric storedMetric = metricRepository.wirte(newBucketName, newBucketType, node);
        assertEquals(TIMESTAMP, storedMetric.getTimestamp());
        assertEquals(node, storedMetric.getObjectNode());
        assertNotNull(storedMetric.getMetricBucket());
        assertEquals(newBucketName, storedMetric.getMetricBucket().getName());
        assertEquals(newBucketType, storedMetric.getMetricBucket().getType());
        
        assertBucketsContains(newBucketName, newBucketType, metricRepository.listBuckets());
        
        StoredMetricCallable callable = mock(StoredMetricCallable.class);
        metricRepository.read(newBucketName, new Interval(TIMESTAMP, Period.minutes(1)), callable);
        ArgumentCaptor<StoredMetric> captor = ArgumentCaptor.forClass(StoredMetric.class);
        verify(callable).call(captor.capture());
        assertStoredMetricEquals(storedMetric, captor.getValue());
    }
    
    public static void assertStoredMetricEquals(StoredMetric expected, StoredMetric actual) {
        assertNotNull(expected);
        assertNotNull(expected.getMetricBucket());
        assertNotNull(actual);
        assertNotNull(actual.getMetricBucket());
        assertEquals(expected.getTimestamp(), actual.getTimestamp());
        assertEquals(expected.getObjectNode(), actual.getObjectNode());
        assertEquals(expected.getMetricBucket().getName(), actual.getMetricBucket().getName());
        assertEquals(expected.getMetricBucket().getType(), actual.getMetricBucket().getType());
    }
    
    public static void assertBucketsContains(String bucketName, String bucketType, Iterable<MetricBucket> buckets) {
        for(MetricBucket bucket : buckets) {
            if(bucketName.equals(bucket.getName()) && bucketType.equals(bucket.getType())) {
                return;
            }
        }
        fail("Bucket: " + bucketName + " with type: " + bucketType + " not found in list.");
    }
    
}