package de.ck35.metricstore.benchmark;

import static org.junit.Assert.*;

import java.util.List;
import java.util.Map.Entry;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Interval;
import org.joda.time.Period;
import org.junit.Test;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;

public class DataGeneratorTest {

    @Test
    public void testGet() {
        JsonNodeFactory nodeFactory = new ObjectMapper().getNodeFactory();
        int bucketCount = 2;
        Interval dataInterval = new Interval(new DateTime(2015, 1, 1, 0, 0, DateTimeZone.UTC), Period.minutes(2));
        int nodesPerMinute = 5;
        int fieldsPerNode = 2;
        int fieldValueLength = 10;
        int numberOfRandomFieldValues = 2;
        DataGenerator dataGenerator = new DataGenerator(nodeFactory, bucketCount, dataInterval, nodesPerMinute, fieldsPerNode, fieldValueLength, numberOfRandomFieldValues);
        
        List<Entry<BucketInfo, List<Entry<DateTime, ObjectNode>>>> result = dataGenerator.get();
        assertEquals(2, result.size());
        assertFalse(result.get(0).getKey().getBucketName() == result.get(1).getKey().getBucketName());
        assertEquals(10, result.get(0).getValue().size());
        assertEquals(10, result.get(1).getValue().size());
    }

}