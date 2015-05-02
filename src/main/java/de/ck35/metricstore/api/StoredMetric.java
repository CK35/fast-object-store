package de.ck35.metricstore.api;

import org.joda.time.DateTime;

import com.fasterxml.jackson.databind.node.ObjectNode;

public interface StoredMetric {

	MetricBucket getMetricBucket();
	
	DateTime getTimestamp();
	
	ObjectNode getObjectNode();
	
}