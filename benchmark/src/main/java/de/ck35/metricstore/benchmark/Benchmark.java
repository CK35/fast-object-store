package de.ck35.metricstore.benchmark;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map.Entry;
import java.util.Random;

import org.joda.time.DateTime;
import org.joda.time.Interval;

import com.fasterxml.jackson.core.TreeNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.Maps;

import de.ck35.metricstore.api.MetricRepository;

public class Benchmark extends Thread {

	private final MetricRepository repository;
	private final ObjectMapper mapper;
	
	public Benchmark(MetricRepository repository, ObjectMapper mapper) {
		super("Metric-Store-Benchmark-Thread");
		this.repository = repository;
		this.mapper = mapper;
	}

	public static List<BucketInfo> createTestBuckets(int bucketCount) {
		List<BucketInfo> result = new ArrayList<>(bucketCount);
		for(int i=0 ; i<bucketCount ; i++) {
			result.add(new BucketInfo("Test-Bucket-" + i, "Test-Bucket-Type-" + i));
		}
		return Collections.unmodifiableList(result);
	}
	
	public static TreeNode createTestNode(DateTime timestamp, JsonNodeFactory nodeFactory, int fieldCount) {
		ObjectNode objectNode = nodeFactory.objectNode();
		objectNode.put("timestamp", timestamp.toString());
		for(int i=0 ; i<fieldCount ; i++) {			
			objectNode.put("field" + i, "value" + i);
		}
		return objectNode;
	}
	
	public List<Entry<BucketInfo, TreeNode>> createTestData(List<BucketInfo> buckets, Interval dataInterval, int nodesPerMinute, int fieldsPerNode) {
		Random bucketIndex = new Random();
		List<Entry<BucketInfo, TreeNode>> result = new ArrayList<>();
		for(DateTime current = dataInterval.getStart() ; current.isBefore(dataInterval.getEnd()) ; current = current.plusMinutes(1)) {
			for(int i=0 ; i<nodesPerMinute ; i++) {
				result.add(Maps.immutableEntry(buckets.get(bucketIndex.nextInt(buckets.size())), createTestNode(current, mapper.getNodeFactory(), fieldsPerNode)));
			}
		}
		return result;
	}

	@Override
	public void run() {
		
	}
	
}