package de.ck35.metricstore.cache;

import com.fasterxml.jackson.databind.node.ObjectNode;

import de.ck35.metricstore.MetricBucket;
import de.ck35.metricstore.StoredMetric;

/**
 * The metric cache allows fast access to the stored metric data. It also allows the definition
 * of filter queries.
 *
 * @author Christian Kaspari
 * @since 1.0.0
 */
public interface MetricCache {

    /**
     * @return Start a request to the cached data.
     */
    MetricCacheRequest request();
    
    /**
     * @return A list of all known metric buckets which are contained inside this cache.
     */
    Iterable<MetricBucket> listBuckets();
    
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