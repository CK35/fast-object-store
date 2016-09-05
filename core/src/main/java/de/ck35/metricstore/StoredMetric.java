package de.ck35.metricstore;

import org.joda.time.DateTime;

import com.fasterxml.jackson.databind.node.ObjectNode;

/**
 * Representation of a persisted metric data object node.
 * 
 * @author Christian Kaspari
 * @since 1.0.0
 */
public interface StoredMetric {

	/**
	 * @return The bucket where this metric data object node can be found in.
	 */
	MetricBucket getMetricBucket();
	
	/**
	 * @return The timestamp of this metric data object node.
	 */
	DateTime getTimestamp();
	
	/**
	 * @return The metric data object node.
	 */
	ObjectNode getObjectNode();
	
}