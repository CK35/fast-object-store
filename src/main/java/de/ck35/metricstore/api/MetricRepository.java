package de.ck35.metricstore.api;

import org.joda.time.Interval;

import com.fasterxml.jackson.databind.node.ObjectNode;

public interface MetricRepository {

	Iterable<MetricBucket> listBuckets();
	
	void read(String bucketName, Interval interval, StoredMetricCallable callable);
	
	StoredMetric wirte(String bucketName, String bucketType, ObjectNode node);
	
}