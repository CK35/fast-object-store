package de.ck35.metricstore.fs;

import java.io.Closeable;
import java.io.IOException;
import java.util.Objects;

import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Function;

import de.ck35.metricstore.api.MetricBucket;
import de.ck35.metricstore.api.StoredMetric;
import de.ck35.metricstore.util.io.ObjectNodeReader;

public class StoredObjectNodeReader implements Closeable {

	private static final Logger LOG = LoggerFactory.getLogger(StoredObjectNodeReader.class);
	
	private final MetricBucket bucket;
	private final ObjectNodeReader reader;
	private final Function<ObjectNode, DateTime> timestampFunction;

	private int ignoredObjectsCount;
	
	public StoredObjectNodeReader(MetricBucket bucket,
	                              ObjectNodeReader reader,
	                              Function<ObjectNode, DateTime> timestampFunction) {
		this.bucket = bucket;
		this.reader = reader;
 		this.timestampFunction = timestampFunction;
	}

	public StoredMetric read() {
		ObjectNode objectNode = reader.read();
		while(objectNode != null) {
			try {
				DateTime timestamp = Objects.requireNonNull(timestampFunction.apply(objectNode), "Timestamp must not be null!");
				return storedObjectNode(bucket, timestamp, objectNode);				
			} catch(IllegalArgumentException e) {
				ignoredObjectsCount++;
				LOG.warn("Missing timestamp inside object node: '{}' in file: '{}'.", objectNode, reader.getPath());				
			}
			objectNode = reader.read();
		}
		return null;
	}
	
	public int getIgnoredObjectsCount() {
		return ignoredObjectsCount + reader.getIgnoredObjectsCount();
	}
	
	@Override
	public void close() throws IOException {
		this.reader.close();
	}
	
	public static StoredMetric storedObjectNode(MetricBucket bucket, DateTime timestamp, ObjectNode objectNode) {
		return new ImmutableStoredObjectNode(bucket, timestamp, objectNode);
	}
	private static class ImmutableStoredObjectNode implements StoredMetric {
		
		private final MetricBucket bucket;
		private final DateTime timestamp;
		private final ObjectNode objectNode;
		
		public ImmutableStoredObjectNode(MetricBucket bucket, DateTime timestamp, ObjectNode objectNode) {
			this.bucket = bucket;
			this.timestamp = timestamp;
			this.objectNode = objectNode;
		}

		@Override
		public MetricBucket getMetricBucket() {
			return bucket;
		}
		@Override
		public DateTime getTimestamp() {
			return timestamp;
		}
		@Override
		public ObjectNode getObjectNode() {
			return objectNode;
		}
	}
}