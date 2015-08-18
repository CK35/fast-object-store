package de.ck35.metricstore.api;

import org.joda.time.Interval;

import com.fasterxml.jackson.databind.node.ObjectNode;

/**
 * Responsible for reading and writing metric data into the underlying metric buckets.
 * New buckets are created on-the-fly when new metric data is written.
 * 
 * @author Christian Kaspari
 * @since 1.0.0
 */
public interface MetricRepository {

	/**
	 * @return A list of all known metric buckets which belong to this repository.
	 */
	Iterable<MetricBucket> listBuckets();
	
	/**
	 * Read stored metric data from a bucket with a given time interval. The provided
	 * callable will receive all loaded metric data.
	 * 
	 * @param bucketName The name of the bucket where the data will be loaded from.
	 * @param interval The time interval to load.
	 * @param callable The callable which will receive the loaded data.
	 */
	void read(String bucketName, Interval interval, StoredMetricCallable callable);
	
	/**
	 * Write a metric data object into the bucket with the given bucket name. The provided
	 * data object needs a valid timestamp value. The name of the timestamp field is
	 * implementation dependent.
	 * 
	 * @param bucketName The bucket name where data should be appended.
	 * @param bucketType The bucket type.
	 * @param node The metric data object to write.
	 * @return A reference to the stored metric data object node.
	 */
	StoredMetric write(String bucketName, String bucketType, ObjectNode node);
	
}