package de.ck35.metricstore;

/**
 * Describes a place where one type of metric is stored. This allows us to group different
 * types of metrics e.g. metrics which belong to a running application server or metrics 
 * which belong to a database. 
 * 
 * @author Christian Kaspari
 * @since 1.0.0
 */
public interface MetricBucket {
	
	/**
	 * @return The name of the bucket.
	 */
	String getName();
	
	/**
	 * @return Type of metric inside the bucket.
	 */
	String getType();
	
}