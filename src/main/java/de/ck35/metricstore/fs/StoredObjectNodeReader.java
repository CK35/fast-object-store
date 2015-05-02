package de.ck35.metricstore.fs;

import java.io.Closeable;
import java.io.IOException;

import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Function;

import de.ck35.metricstore.api.MetricBucket;
import de.ck35.metricstore.api.StoredMetric;

public class StoredObjectNodeReader implements Closeable {

	private static final Logger LOG = LoggerFactory.getLogger(StoredObjectNodeReader.class);
	
	private final MetricBucket bucket;
	private final ObjectNodeReader reader;
	private final Function<ObjectNode, DateTime> timestampFunction;

	private int ignoredObjects;
	
	public StoredObjectNodeReader(MetricBucket bucket,
	                              ObjectNodeReader reader,
	                              Function<ObjectNode, DateTime> timestampFunction) {
		this.bucket = bucket;
		this.reader = reader;
 		this.timestampFunction = timestampFunction;
	}

	public StoredMetric read() throws IOException {
		ObjectNode objectNode = reader.read();
		if(objectNode == null) {
			return null;
		}
		DateTime timestamp = timestampFunction.apply(objectNode);
		if(timestamp == null) {
			ignoredObjects++;
			LOG.warn("Missing timestamp inside object node: '{}' in file: '{}'.", objectNode, reader.getPath());
		}
		return storedObjectNode(bucket, timestamp, objectNode);
	}
	
	public int getIgnoredObjects() {
		return ignoredObjects + reader.getIgnoredObjects();
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