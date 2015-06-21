package de.ck35.metricstore.benchmark;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map.Entry;
import java.util.Random;

import org.apache.commons.lang.RandomStringUtils;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.Maps;

public class DataGenerator implements Supplier<List<Entry<BucketInfo, List<Entry<DateTime, ObjectNode>>>>> {

    private static final Logger LOG = LoggerFactory.getLogger(DataGenerator.class);
    
    private final JsonNodeFactory nodeFactory;
    private final int bucketCount;
    private final Interval dataInterval;
    private final int nodesPerMinute;
    private final int fieldsPerNode;
    private final int fieldValueLength;
    private final int numberOfRandomFieldValues;
    
    private final Supplier<List<Entry<BucketInfo, List<Entry<DateTime, ObjectNode>>>>> memorizedSupplier;
    
    public DataGenerator(JsonNodeFactory nodeFactory,
                         int bucketCount,
                         Interval dataInterval,
                         int nodesPerMinute,
                         int fieldsPerNode,
                         int fieldValueLength,
                         int numberOfRandomFieldValues) {
        this.nodeFactory = nodeFactory;
        this.bucketCount = bucketCount;
        this.dataInterval = dataInterval;
        this.nodesPerMinute = nodesPerMinute;
        this.fieldsPerNode = fieldsPerNode;
        this.fieldValueLength = fieldValueLength;
        this.numberOfRandomFieldValues = numberOfRandomFieldValues;
        this.memorizedSupplier = Suppliers.memoize(new Supplier<List<Entry<BucketInfo, List<Entry<DateTime, ObjectNode>>>>>() {
			@Override
			public List<Entry<BucketInfo, List<Entry<DateTime, ObjectNode>>>> get() {
				return load();
			}
		});
    }
    
    protected List<Entry<BucketInfo, List<Entry<DateTime, ObjectNode>>>> load() {
    	int minutes = (int) dataInterval.toDuration().getStandardMinutes();
        int valuesPerBucket = minutes * nodesPerMinute;
        int totalValues = valuesPerBucket * bucketCount;
        
        LOG.info("Creating '{}' object nodes for interval: '{}' with: '{}' Buckets, '{}' nodes per minute and '{}' fields per node.", totalValues,
                                                                                                                                      dataInterval, 
                                                                                                                                      bucketCount, 
                                                                                                                                      nodesPerMinute, 
                                                                                                                                      fieldsPerNode);
        Random random = new Random();
        List<String> fieldNames = fieldNames(fieldsPerNode);
        List<String> randomFieldValues = randomFieldValues(numberOfRandomFieldValues, fieldValueLength);
        List<Entry<BucketInfo, List<Entry<DateTime, ObjectNode>>>> result = new ArrayList<>(bucketCount);
        for(int bucketNumber=1 ; bucketNumber<bucketCount+1 ; bucketNumber++) {
            BucketInfo bucketInfo = bucketInfo(bucketNumber);
            LOG.info("Creating data for Bucket: '{}'.", bucketInfo);
            List<Entry<DateTime, ObjectNode>> nodes = new ArrayList<>(valuesPerBucket);
            for(DateTime current = dataInterval.getStart() ; current.isBefore(dataInterval.getEnd()) ; current = current.plusMinutes(1)) {   
                for(int nodeCount = 0 ; nodeCount < nodesPerMinute ; nodeCount++) {                    
                    nodes.add(Maps.immutableEntry(current, objectNode(current, fieldNames, randomFieldValues, random)));
                }
            }
            result.add(Maps.immutableEntry(bucketInfo, nodes));
        }
        return result;
    }
    
    @Override
    public List<Entry<BucketInfo, List<Entry<DateTime, ObjectNode>>>> get() {
        return memorizedSupplier.get();
    }
    
    public ObjectNode objectNode(DateTime timestamp, Iterable<String> fieldNames, List<String> randomFieldValues, Random random) {
        ObjectNode objectNode = nodeFactory.objectNode();
        objectNode.put("timestamp", timestamp.toString());
        for(String fieldName : fieldNames) {            
            objectNode.put(fieldName, randomFieldValues.get(random.nextInt(randomFieldValues.size())));
        }
        return objectNode;
    }
    
    public static BucketInfo bucketInfo(int bucketNumber) {
        return new BucketInfo("Test-Bucket-" + bucketNumber, "Test-Bucket-Type-" + bucketNumber);
    }
    
    public static List<String> fieldNames(int fieldCount) {
        List<String> result = new ArrayList<>(fieldCount);
        for(int fieldIndex = 0 ; fieldIndex < fieldCount ; fieldIndex++) {
            result.add("field-" + fieldIndex);
        }
        return Collections.unmodifiableList(result);
    }
    
    public static List<String> randomFieldValues(int valueCount, int valueLength) {
        List<String> result = new ArrayList<>(valueCount);
        for(int valueIndex = 0 ; valueIndex < valueCount ; valueIndex++) {
            result.add(RandomStringUtils.random(valueLength, "abcdefghijklmnopqrstuvwxyz"));
        }
        return Collections.unmodifiableList(result);
    }
    
}