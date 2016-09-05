package de.ck35.metricstore;


/**
 * Implement this interface when you load metric data from the {@link MetricRepository}. 
 * <p>
 * <b>This interface is implemented by API clients. Every change here is a major API version change.</b>
 * 
 * @author Christian Kaspari
 * @since 1.0.0
 */
public interface StoredMetricCallable {

	/**
	 * This method will be called after a stored metric has been loaded.
	 * 
	 * @param node The loaded metric node.
	 */
	void call(StoredMetric node);
	
}